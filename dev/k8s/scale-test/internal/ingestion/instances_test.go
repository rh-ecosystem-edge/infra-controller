// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package ingestion

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/NVIDIA/infra-controller/dev/k8s/scale-test/internal/cluster"
	"github.com/NVIDIA/infra-controller/dev/k8s/scale-test/internal/clustertest"
	"github.com/NVIDIA/infra-controller/dev/k8s/scale-test/internal/config"
	"github.com/NVIDIA/infra-controller/dev/k8s/scale-test/internal/inventory"
	"github.com/NVIDIA/infra-controller/dev/k8s/scale-test/internal/journal"
	"github.com/NVIDIA/infra-controller/dev/k8s/scale-test/internal/jsonx"
	"github.com/NVIDIA/infra-controller/dev/k8s/scale-test/internal/lock"
	"github.com/NVIDIA/infra-controller/dev/k8s/scale-test/internal/probe"
	"github.com/NVIDIA/infra-controller/dev/k8s/scale-test/internal/probeapi"
	"github.com/NVIDIA/infra-controller/dev/k8s/scale-test/internal/readiness"
	"github.com/NVIDIA/infra-controller/dev/k8s/scale-test/internal/result"
	"github.com/NVIDIA/infra-controller/dev/k8s/scale-test/internal/run"
	"github.com/NVIDIA/infra-controller/dev/k8s/scale-test/internal/writer"
	"golang.org/x/time/rate"
	"gopkg.in/yaml.v3"
)

func instanceUUID(i int) string { return fmt.Sprintf("00000000-0000-4000-8000-%012d", i) }

// One fake site supplies immutable probe snapshots and accepts REST writes.
// A source frame lags by one interval, so read-back must wait for new evidence.
type instanceSite struct {
	mu            sync.Mutex
	state         map[probeapi.Family]map[string]probeapi.Unit
	cache         map[probeapi.Family][]probeapi.Unit
	frame         int64
	process       float64
	inv           inventory.SiteInventory
	requests      []string
	labelAttempts int
	created       int
	mode          string
	action        func()
}

func instanceFixture(n int) *instanceSite {
	f := &instanceSite{state: map[probeapi.Family]map[string]probeapi.Unit{}, frame: -1, process: float64(time.Now().Add(-time.Hour).UnixMilli()) / 1000}
	for _, spec := range instanceFamilies {
		f.state[spec.family] = map[string]probeapi.Unit{}
	}
	records := inventory.Response{InventoryID: "fixture", CarbideAPIURL: "https://api.example:1079"}
	for i := 0; i < n; i++ {
		id, rid, mac := fmt.Sprintf("h%d", i), instanceUUID(i+1), fmt.Sprintf("00:00:00:00:%02x:%02x", i/256, i%256)
		dpu := fmt.Sprintf("d%d", i)
		ip := fmt.Sprintf("10.0.%d.%d", i/256, i%256)
		f.state[probeapi.FamilyHost][id] = probeapi.Unit{ID: id, Type: probeapi.UnitTypeHost, Source: probeapi.SourceNicoAPI, Stage: probeapi.StageReady, State: "Ready", BmcIP: &ip, Host: &probeapi.HostFields{Allocatable: new(true), RackID: new("rack-1"), DpuIDs: []string{dpu}}}
		f.state[probeapi.FamilyRestMachines][rid] = probeapi.Unit{ID: rid, Type: probeapi.UnitTypeHost, Source: probeapi.SourceRest, Stage: probeapi.StageReady, State: "Ready", RestMachine: &probeapi.RestMachineFields{ControllerID: id, InstanceType: new(instanceUUID(900)), IsUsableByTenant: true, Labels: map[string]string{"preserve": "value"}}}
		f.state[probeapi.FamilyDpu][dpu] = probeapi.Unit{ID: dpu, Type: probeapi.UnitTypeDpu, Source: probeapi.SourceNicoAPI, Stage: probeapi.StageReady, State: "Ready", Dpu: &probeapi.DpuFields{HostID: &id}}
		f.state[probeapi.FamilyExpected][mac] = probeapi.Unit{ID: mac, Type: probeapi.UnitTypeExpectedRecord, Source: probeapi.SourceNicoAPI, Expected: &probeapi.ExpectedFields{RecordType: probeapi.RecordTypeHost, Linked: true, DeviceID: &id, BmcMac: &mac}}
		records.Devices = append(records.Devices, inventory.Device{Kind: inventory.KindMachine, BmcMac: mac, RackID: new("rack-1")})
		records.DPUs = append(records.DPUs, inventory.DPU{BmcMac: "dpu-" + mac, ParentBmcMac: mac})
	}
	f.inv = inventory.SiteInventory{Pods: []inventory.MatPod{{Name: "mat-0", UID: "uid-1", Ready: true, Records: records}}, N: inventory.TypeCounts{Host: uint64(n), DPU: uint64(n)}}
	return f
}
func (f *instanceSite) meta(family probeapi.Family) probeapi.SweepInfo {
	start := time.Now().Truncate(30 * time.Second).Add(-30 * time.Second)
	if f.mode == "stale" {
		start = start.Add(-3 * time.Minute)
	}
	return probeapi.SweepInfo{SnapshotID: fmt.Sprintf("%.0f-devices-%d", f.process*1000, start.Unix()), Family: family, LastAttemptSucceeded: new(f.mode != "partial"), SweepStart: jsonx.Time{Time: start}, ReadAt: jsonx.Time{Time: start.Add(time.Second)}, SweepIntervalSeconds: 30, SweepAgeSeconds: time.Since(start.Add(time.Second)).Seconds()}
}
func (f *instanceSite) snapshot(family probeapi.Family) []probeapi.Unit {
	frame := time.Now().Unix() / 30
	if frame != f.frame {
		f.frame = frame
		f.cache = map[probeapi.Family][]probeapi.Unit{}
		for family, index := range f.state {
			for _, id := range slices.Sorted(maps.Keys(index)) {
				b, _ := json.Marshal(index[id])
				var u probeapi.Unit
				if json.Unmarshal(b, &u) != nil {
					panic("fixture unit")
				}
				f.cache[family] = append(f.cache[family], u)
			}
		}
		if f.mode == "rest-binding-lag" && f.created > 0 {
			for _, u := range f.cache[probeapi.FamilyRestInstances] {
				u.Instance.HostID = nil
			}
			f.mode = "binding-recovers"
		}
	}
	return f.cache[family]
}
func familyFor(filter probeapi.Filter) probeapi.Family {
	for _, spec := range instanceFamilies {
		if spec.filter.Type == filter.Type && (spec.filter.Source == nil) == (filter.Source == nil) {
			return spec.family
		}
	}
	panic("unexpected filter")
}
func (f *instanceSite) Units(_ context.Context, filter probeapi.Filter, _ *string, page probe.Page) (probeapi.UnitsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.mode == "read-error" {
		return probeapi.UnitsResponse{}, errors.New("probe unavailable")
	}
	family := familyFor(filter)
	rows := f.snapshot(family)
	begin := 0
	if page.Cursor != nil {
		begin, _ = strconv.Atoi(*page.Cursor)
	}
	end := min(begin+1000, len(rows))
	reply := probeapi.UnitsResponse{SweepInfo: f.meta(family), Units: rows[begin:end]}
	if end < len(rows) {
		reply.NextCursor = new(strconv.Itoa(end))
	}
	if f.mode == "duplicate" && family == probeapi.FamilyHost && len(rows) > 0 {
		reply.Units = append(slices.Clone(reply.Units), rows[0])
	}
	return reply, nil
}
func (f *instanceSite) Count(_ context.Context, filter probeapi.Filter) (probeapi.CountResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	family := familyFor(filter)
	rows := f.snapshot(family)
	n := uint64(0)
	for _, u := range rows {
		if filter.Stage != nil && u.Stage != *filter.Stage {
			continue
		}
		if filter.Allocatable != nil && (u.Host == nil || u.Host.Allocatable == nil || *u.Host.Allocatable != *filter.Allocatable) {
			continue
		}
		n++
	}
	if f.mode == "missing-unit" && family == probeapi.FamilyHost {
		n++
	}
	return probeapi.CountResponse{Count: n, SweepInfo: f.meta(family)}, nil
}
func (f *instanceSite) UnitsQuery(context.Context, probeapi.UnitsQuery) (probeapi.UnitsResponse, error) {
	return probeapi.UnitsResponse{}, errors.New("unexpected targeted query")
}
func (f *instanceSite) Config(context.Context, probe.ConfigFilter) (probeapi.ConfigResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	m := f.meta(probeapi.FamilyConfigRest)
	r := probeapi.ReadInfo{ReadStart: m.SweepStart, ReadAt: m.ReadAt, ReadIntervalSeconds: 30}
	return probeapi.ConfigResponse{Metrics: &probeapi.MetricsInfo{ContractVersion: 1, ProcessStartSeconds: f.process}, RestSite: &probeapi.RestSiteBinding{ReadInfo: r, Result: probeapi.SiteMatchMatch}, Rest: &probeapi.ConfigRest{ReadInfo: r, TenantAllocation: &probeapi.TenantAllocation{Tenant: instanceUUID(901), InstanceType: instanceUUID(900), Assigned: 100, Total: 100, Unused: 100, UnusedUsable: 100}}}, nil
}
func (f *instanceSite) RoundTrip(req *http.Request) (*http.Response, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, req.Method+" "+req.URL.Path)
	if req.Header.Get("Authorization") != "Bearer test-token" {
		panic("token not passed")
	}
	var body map[string]json.RawMessage
	if req.Body != nil {
		_ = json.NewDecoder(req.Body).Decode(&body)
	}
	reply := []map[string]string{}
	if req.Method == http.MethodPatch {
		f.labelAttempts++
		id := req.URL.Path[strings.LastIndex(req.URL.Path, "/")+1:]
		u := f.state[probeapi.FamilyRestMachines][id]
		labels := map[string]string{}
		_ = json.Unmarshal(body["labels"], &labels)
		before := u.RestMachine.Labels
		u.RestMachine.Labels = labels
		_, adding := labels[instanceLabel]
		unclear := strings.HasPrefix(f.mode, "label-") && (!strings.HasPrefix(f.mode, "label-remove") || !adding)
		if unclear && (f.mode == "label-absent" || f.mode == "label-budget" || strings.Contains(f.mode, "retry") && f.labelAttempts == 1) {
			u.RestMachine.Labels = before
		}
		if unclear && (f.mode == "label-other" || f.mode == "label-retry-other" && f.labelAttempts == 2) {
			u.RestMachine.Labels[instanceLabel] = "other-owner"
		}
		f.state[probeapi.FamilyRestMachines][id] = u
		if unclear {
			return &http.Response{StatusCode: 500, Body: io.NopCloser(strings.NewReader("private error"))}, nil
		}
	} else if req.Method == http.MethodPost {
		var count int
		var prefix string
		selector := map[string]string{}
		_ = json.Unmarshal(body["count"], &count)
		_ = json.Unmarshal(body["namePrefix"], &prefix)
		_ = json.Unmarshal(body["machineLabelSelector"], &selector)
		for i := 0; i < count; i++ {
			machine := ""
			for _, id := range slices.Sorted(maps.Keys(f.state[probeapi.FamilyRestMachines])) {
				u := f.state[probeapi.FamilyRestMachines][id]
				if u.RestMachine.InstanceID == nil && u.RestMachine.Labels[instanceLabel] == selector[instanceLabel] {
					machine = id
					break
				}
			}
			if machine == "" {
				panic("no selected machine")
			}
			if f.mode == "wrong-host" {
				machine = instanceUUID(len(f.state[probeapi.FamilyHost]))
			}
			f.created++
			id, name := instanceUUID(1000+f.created), fmt.Sprintf("%s-abc123-%d", prefix, i+1)
			if f.mode == "suffix-mismatch" && i > 0 {
				name = fmt.Sprintf("%s-fed456-%d", prefix, i+1)
			}
			if f.mode == "duplicate-slot" {
				name = prefix + "-abc123-1"
			}
			r := f.state[probeapi.FamilyRestMachines][machine]
			host := r.RestMachine.ControllerID
			r.State = "Assigned"
			r.RestMachine.InstanceID = &id
			r.RestMachine.TenantID = new(instanceUUID(901))
			r.RestMachine.IsUsableByTenant = false
			f.state[probeapi.FamilyRestMachines][machine] = r
			u := f.state[probeapi.FamilyHost][host]
			u.Host.Allocatable = new(false)
			f.state[probeapi.FamilyHost][host] = u
			f.state[probeapi.FamilyRestInstances][id] = probeapi.Unit{ID: id, ControllerInstanceID: &id, Type: probeapi.UnitTypeInstance, Source: probeapi.SourceRest, Stage: probeapi.StageReady, State: "Ready", Instance: &probeapi.InstanceFields{Name: &name, Tenant: new(instanceUUID(901)), HostID: &machine, Status: "Ready"}}
			f.state[probeapi.FamilyInstance][id] = probeapi.Unit{ID: id, Type: probeapi.UnitTypeInstance, Source: probeapi.SourceNicoAPI, Stage: probeapi.StageReady, State: "ready", Instance: &probeapi.InstanceFields{Name: &name, Tenant: new("tenant-org"), HostID: &host, Status: "ready"}}
			reply = append(reply, map[string]string{"id": id})
		}
	} else if req.Method == http.MethodDelete {
		id := req.URL.Path[strings.LastIndex(req.URL.Path, "/")+1:]
		instance := f.state[probeapi.FamilyRestInstances][id]
		machine := *instance.Instance.HostID
		r := f.state[probeapi.FamilyRestMachines][machine]
		r.State = "Ready"
		r.RestMachine.InstanceID = nil
		r.RestMachine.TenantID = nil
		r.RestMachine.IsUsableByTenant = true
		f.state[probeapi.FamilyRestMachines][machine] = r
		h := f.state[probeapi.FamilyHost][r.RestMachine.ControllerID]
		h.Host.Allocatable = new(true)
		f.state[probeapi.FamilyHost][h.ID] = h
		delete(f.state[probeapi.FamilyRestInstances], id)
		delete(f.state[probeapi.FamilyInstance], id)
	} else {
		panic("runner read REST directly")
	}
	if f.action != nil {
		f.action()
	}
	if req.Method == http.MethodPost && f.mode == "late" {
		f.mu.Unlock()
		time.Sleep(90 * time.Second)
		f.mu.Lock()
	}
	if req.Method == http.MethodPost && f.mode == "lost" {
		return nil, errors.New(secret)
	}
	if req.Method == http.MethodPost && f.mode == "malformed" {
		reply = nil
	}
	b, _ := json.Marshal(reply)
	return &http.Response{StatusCode: 201, Body: io.NopCloser(bytes.NewReader(b)), Header: http.Header{}}, nil
}

type instanceMetrics struct {
	fakeStore
	afterSummary func()
}

func (*instanceMetrics) PoolHealthy(context.Context, time.Time, map[string]string) ([]string, error) {
	return []string{"api-1"}, nil
}
func (*instanceMetrics) ProbeDemand(context.Context, time.Time, time.Duration) error { return nil }
func (s *instanceMetrics) Summarize(_ context.Context, _ time.Time, f probeapi.Filter, p probeapi.SummaryParams) (probeapi.SummaryResponse, error) {
	if p.Base == nil {
		panic("missing frozen base")
	}
	if s.afterSummary != nil {
		s.afterSummary()
	}
	cross := []probeapi.Crossing{}
	for _, q := range probeapi.FixedQuantiles {
		cross = append(cross, probeapi.Crossing{Q: q, Seconds: new(1.0)})
	}
	return probeapi.SummaryResponse{Family: familyFor(f), N: p.N, Crossings: cross}, nil
}
func instanceCommand(t *testing.T, f *instanceSite, command, id, dir string, n int) (*InstanceTest, *rig, *run.Control) {
	t.Helper()
	r := newRig(t)
	control := run.NewControl()
	r.w = writer.New(r.j, r.gate, control.Stopper(), time.Time{})
	script := filepath.Join(dir, "fixture.ipxe")
	if err := os.WriteFile(script, []byte("#!ipxe\nexit\n"), 0600); err != nil {
		t.Fatal(err)
	}
	s := InstanceSettings{}
	s.Instance.APIPoolReplicas = 1
	s.Instance.N = uint64(n)
	s.Instance.BurstSize = 100
	s.Instance.TenantID = instanceUUID(901)
	s.Instance.InstanceTypeID = instanceUUID(900)
	s.Instance.IPXEFile = script
	s.Instance.VPCID = instanceUUID(902)
	s.Instance.SubnetID = instanceUUID(903)
	s.Rest.TokenFile = filepath.Join(dir, "rest-token")
	if err := os.WriteFile(s.Rest.TokenFile, []byte("test-token"), 0600); err != nil {
		t.Fatal(err)
	}
	s.Rest.Org = "tenant-org"
	s.Rest.ProviderOrg = "provider-org"
	s.Rest.URL = "https://rest.example"
	s.Observation.IntervalSeconds = 30
	s.Observation.LabelReadbackSeconds = 90
	s.Mat.Selector = "mat"
	metrics := &instanceMetrics{}
	pods := &fakePods{pods: map[string][]cluster.PodInfo{"mat": {{Name: "mat-0", UID: "uid-1", Ready: true}}, "core": {{Name: "api-0", UID: "uid-2", Ready: true}}}}
	cfg := &config.Config{RunID: id, Command: command, Namespace: "mat", Journal: config.Journal{Dir: dir}, ControlPlane: []config.PodSelector{{Namespace: "core", LabelSelector: "api"}}, Probe: config.Probe{PollSeconds: 10}, Timeouts: config.Timeouts{ReadinessSeconds: 300}, ResultMaxBytes: 262144}
	deps := readiness.Deps{Pods: pods, Store: func(context.Context) (readiness.StoreProbe, error) { return readiness.StoreProbe{Client: metrics}, nil }}
	test := &InstanceTest{cfg: cfg, s: s, deps: deps, p: f, rest: &restWrites{client: &http.Client{Transport: f}, settings: s, pace: rate.NewLimiter(rate.Inf, 1), token: "test-token"}, check: func(context.Context, readiness.Deps, time.Time) readiness.Report {
		return readiness.Report{Checks: []readiness.Check{{Name: "fixture", OK: true}}, Inventory: f.inv}
	}}
	return test, r, control
}
func runInstance(test *InstanceTest, r *rig, c *run.Control, dry bool) (result.Outcome, error) {
	now := time.Now()
	return test.Run(run.RunCtx{Ctx: c.Context(), Clock: run.RealClock{}, Deadline: now.Add(time.Hour), EvidenceEnd: now.Add(2 * time.Hour), ProbeReadEnd: now.Add(2 * time.Hour), DryRun: dry}, r.w)
}

func TestInstanceCommands(t *testing.T) {
	for _, tc := range []struct {
		name     string
		n, burst int
		mode     string
		dry      bool
		want     result.Verdict
		pending  bool
	}{
		{"create-delete", 5, 100, "", false, result.Passed, false},
		{"landed-label", 5, 100, "label-landed", false, result.Passed, false},
		{"absent-label", 2, 100, "label-absent", false, result.Unresolved, true},
		{"label-budget", 2, 100, "label-budget", false, result.Unresolved, true},
		{"resend-label", 2, 100, "label-retry-landed", false, result.Passed, false},
		{"resend-other-label", 2, 100, "label-retry-other", false, result.Unresolved, true},
		{"other-label", 5, 100, "label-other", false, result.Unresolved, true}, {"opaque-create-delete", 5, 100, "", false, result.Passed, false}, {"serial-bursts", 6, 2, "", false, result.Passed, false}, {"batch-split", 20, 100, "", false, result.Passed, false},
		{"rest-binding-lag", 5, 100, "rest-binding-lag", false, result.Passed, false},
		{"invalid-token", 5, 100, "invalid-token", false, result.Unresolved, true},
		{"dry", 5, 100, "", true, result.Go, false},
		{"late-reply", 5, 100, "late", false, result.Unresolved, true},
		{"stale-read", 5, 100, "stale", false, result.NotReady, false},
		{"missing-unit", 5, 100, "missing-unit", false, result.NotReady, false},
		{"read-fails-after-write", 5, 100, "fail-after-write", false, result.Unresolved, true},
		{"pod-restart", 5, 100, "restart", false, result.Unresolved, true},
		{"cohort-failure", 5, 100, "failed", false, result.Failed, true}, {"lost-reply", 5, 100, "lost", false, result.Unresolved, true}, {"malformed-acceptance", 5, 100, "malformed", false, result.Unresolved, true},
		{"wrong-placement", 5, 100, "wrong-host", false, result.Failed, true},
		{"suffix-mismatch", 5, 100, "suffix-mismatch", false, result.Failed, true},
		{"duplicate-slot", 5, 100, "duplicate-slot", false, result.Failed, true}, {"partial-read", 5, 100, "partial", false, result.NotReady, false}, {"duplicate-unit", 5, 100, "duplicate", false, result.NotReady, false},
		{"persist-before-write", 5, 100, "persist", false, result.NotReady, true}, {"lost-lock", 5, 100, "lock", false, result.Unresolved, true}, {"cancel", 5, 100, "cancel", false, result.Unresolved, true}, {"soft-stop", 5, 100, "stop", false, result.Unresolved, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := instanceFixture(tc.n + 1)
				f.mode = tc.mode
				if tc.name == "opaque-create-delete" {
					for id, u := range f.state[probeapi.FamilyRestMachines] {
						delete(f.state[probeapi.FamilyRestMachines], id)
						u.ID = u.RestMachine.ControllerID
						f.state[probeapi.FamilyRestMachines][u.ID] = u
					}
				}
				dir := t.TempDir()
				test, r, c := instanceCommand(t, f, "instance-create", "create-1", dir, tc.n)
				test.s.Instance.BurstSize = uint64(tc.burst)
				switch tc.mode {
				case "invalid-token":
					if err := os.WriteFile(test.s.Rest.TokenFile, []byte("sensitive\nvalue"), 0600); err != nil {
						t.Fatal(err)
					}
				case "persist":
					r.j.Inject(func(_ *os.File, _ []byte) error { return errors.New("disk failed") })
				case "fail-after-write":
					f.action = func() { f.mode = "partial" }
				case "restart":
					f.action = func() { test.deps.Pods.(*fakePods).set("core", "replacement") }
				case "failed":
					f.action = func() {
						if len(f.state[probeapi.FamilyInstance]) > 0 {
							for id, u := range f.state[probeapi.FamilyInstance] {
								u.Stage = probeapi.StageError
								u.State = "failed"
								f.state[probeapi.FamilyInstance][id] = u
							}
						}
					}
				case "lock":
					f.action = func() { r.gate.lost.Store(true) }
				case "cancel":
					f.action = c.Cancel
				case "stop":
					f.action = c.RequestStop
				}
				if tc.mode == "label-budget" {
					test.s.Observation.LabelReadbackSeconds = 1800
				}
				started := time.Now()
				out, err := runInstance(test, r, c, tc.dry)
				if tc.mode == "label-absent" && ((time.Since(started) < 210*time.Second || time.Since(started) > 211*time.Second) || !strings.Contains(strings.Join(out.Reasons, " "), "window ended unsettled")) {
					t.Fatalf("label wait was not bounded or observed: %v %v", time.Since(started), out.Reasons)
				}
				if tc.mode == "label-other" && !strings.Contains(strings.Join(out.Reasons, " "), "another owner") {
					t.Fatalf("ownership breach not classified: %v", out.Reasons)
				}
				if out.Verdict != tc.want || r.j.Unsettled() != tc.pending {
					t.Fatalf("verdict=%s pending=%t error=%v", out.Verdict, r.j.Unsettled(), err)
				}
				if out.Verdict == result.Passed || out.Verdict == result.Go {
					if err != nil {
						t.Fatal(err)
					}
				}
				if tc.mode == "invalid-token" && (len(f.requests) != 0 || r.j.Counts().Attempts != 0) {
					t.Fatal("invalid token dispatched a write")
				}
				if tc.mode == "persist" && len(f.requests) != 0 || tc.dry && len(f.requests) != 0 {
					t.Fatal("mutation without persisted intent or in dry run")
				}
				if tc.mode == "lost" || tc.mode == "malformed" || tc.mode == "late" {
					posts := strings.Count("\n"+strings.Join(f.requests, "\n"), "\nPOST ")
					if posts != 1 {
						t.Fatalf("unclear create replayed %d times", posts)
					}
				}
				if strings.HasPrefix(tc.mode, "label-") {
					calls := "\n" + strings.Join(f.requests, "\n")
					patches, posts := strings.Count(calls, "\nPATCH "), strings.Count(calls, "\nPOST ")
					expected := tc.n
					if tc.pending {
						expected = 1
					}
					if tc.mode == "label-absent" || strings.Contains(tc.mode, "retry") {
						expected = 2
						if !tc.pending {
							expected = tc.n + 1
						}
						raw, e := os.ReadFile(r.j.Path())
						if e != nil {
							t.Fatal(e)
						}
						var ops []uint64
						for line := range bytes.SplitSeq(raw, []byte("\n")) {
							var v journal.Intent
							_ = json.Unmarshal(line, &v)
							if v.Entry == "intent" && v.Kind == "labels" && (len(ops) == 0 || v.Op == ops[0]) {
								ops = append(ops, v.Op)
								if v.Attempt != uint32(len(ops)) {
									t.Fatal("retry attempt not journaled")
								}
							}
						}
						if len(ops) != 2 || ops[0] != ops[1] {
							t.Fatalf("label retry changed op: %v", ops)
						}
					}
					if patches != expected || tc.pending && posts != 0 || !tc.pending && posts != 1 {
						t.Fatalf("label read-back replayed or dispatched too far: %v", f.requests)
					}
				}
				if tc.mode == "failed" && (len(out.Stages) != 1 || out.Stages[0].Units.Failed != uint64(tc.n)) {
					t.Fatal("cohort failure lost its denominator")
				}
				if tc.name == "create-delete" || tc.name == "opaque-create-delete" || tc.name == "serial-bursts" {
					if tc.name == "create-delete" {
						owned, readErr := readOwned(dir, "create-1.owned.json")
						if readErr != nil {
							t.Fatal(readErr)
						}
						bad := owned
						bad.Tenant = instanceUUID(999)
						if _, e := writeOnce(dir, "wrong.owned.json", bad); e != nil {
							t.Fatal(e)
						}
						denied, rr, cc := instanceCommand(t, f, "instance-delete", "wrong-delete", dir, tc.n)
						denied.s.Instance.OwnedManifest = "wrong.owned.json"
						before := len(f.requests)
						rejected, e := runInstance(denied, rr, cc, false)
						if rejected.Verdict != result.NotReady || len(rejected.Reasons) == 0 || e != nil || len(f.requests) != before {
							t.Fatal("ownership mismatch dispatched a write")
						}
					}
					var detail instanceDetail
					if json.Unmarshal(out.Detail, &detail) != nil || detail.Manifest == "" {
						t.Fatal("owned manifest missing")
					}
					dryDel, dryR, dryC := instanceCommand(t, f, "instance-delete", "delete-dry", dir, tc.n)
					dryDel.s.Instance.OwnedManifest = detail.Manifest
					before := len(f.requests)
					dryOut, dryErr := runInstance(dryDel, dryR, dryC, true)
					if dryOut.Verdict != result.Go || dryErr != nil || len(f.requests) != before || dryR.j.Unsettled() {
						t.Fatalf("delete dry run=%s err=%v", dryOut.Verdict, dryErr)
					}
					del, dr, dc := instanceCommand(t, f, "instance-delete", "delete-1", dir, tc.n)
					del.s.Instance.OwnedManifest = detail.Manifest
					if tc.name == "create-delete" {
						f.mode, f.labelAttempts = "label-remove-retry", 0
					}
					result, e := runInstance(del, dr, dc, false)
					if result.Verdict != "passed" || e != nil || dr.j.Unsettled() {
						t.Fatalf("delete=%s pending=%t err=%v", result.Verdict, dr.j.Unsettled(), e)
					}
					if tc.name == "create-delete" && f.labelAttempts != tc.n+1 {
						t.Fatalf("removal retry PATCH count=%d", f.labelAttempts)
					}
					if len(f.state[probeapi.FamilyRestInstances]) != 0 {
						t.Fatal("owned instances remain")
					}
					for _, u := range f.state[probeapi.FamilyRestMachines] {
						if u.RestMachine.Labels[instanceLabel] != "" || u.RestMachine.Labels["preserve"] != "value" {
							t.Fatal("labels not restored")
						}
					}
				}
			})
		})
	}
}

func TestCohortAndBatchSizes(t *testing.T) {
	for _, tc := range []struct {
		n, burst int
		valid    bool
	}{{1, 18, false}, {19, 18, true}, {101, 100, true}, {5, 2, false}, {19800, 2, true}} {
		sizes, err := batches(uint64(tc.n), uint64(tc.burst))
		if (err == nil) != tc.valid {
			t.Fatalf("partition %d/%d: %v", tc.n, tc.burst, err)
		}
		total := 0
		for _, n := range sizes {
			if n < 2 || n > tc.burst {
				t.Fatal("illegal batch")
			}
			total += n
		}
		if err == nil && total != tc.n {
			t.Fatal("lost slot")
		}
	}
	for _, n := range []int{10008, 19800} {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			f := instanceFixture(n)
			s := InstanceSettings{}
			s.Instance.N = uint64(n)
			s.Instance.BurstSize = 2
			s.Instance.TenantID = instanceUUID(901)
			s.Instance.InstanceTypeID = instanceUUID(900)
			snapshot := &instanceSnapshot{rows: f.state, meta: map[probeapi.Family]probeapi.SweepInfo{}}
			plan, err := planInstances(snapshot, f.inv, map[string]podState{"mat/mat-0": {UID: "uid-1"}}, s, "large", "mat")
			if err != nil || len(plan.Cohort) != n || plan.Cohort[n-1].Burst != n/2-1 {
				t.Fatalf("cohort=%d err=%v", len(plan.Cohort), err)
			}
		})
	}
}

func TestInstanceSettings(t *testing.T) {
	for _, tc := range []struct {
		name, change string
		ok           bool
	}{
		{"valid", "", true}, {"topology", "instance:\n  topologyOptimized: true", true},
		{"bad-topology", "instance:\n  topologyOptimized: invalid", false}, {"plain-http", "rest:\n  url: http://rest.example", false},
		{"singleton", "instance:\n  n: 1", false}, {"missing-pool", "instance:\n  apiPoolReplicas: 0", false},
		{"too-fast", "observation:\n  intervalSeconds: 29", false},
		{"bad-label-window", "observation:\n  labelReadbackSeconds: 0", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var node yaml.Node
			body := fmt.Sprintf("checkStore: true\nrest:\n  url: https://rest.example\n  org: tenant\n  providerOrg: provider\n  tokenFile: token\nobservation:\n  intervalSeconds: 30\ninstance:\n  n: 5\n  tenantId: %s\n  instanceTypeId: %s\n  vpcId: %s\n  subnetId: %s\n  ipxeFile: script\n  apiPoolReplicas: 1\n  apiPoolSelector: {job: api}\n", instanceUUID(1), instanceUUID(2), instanceUUID(3), instanceUUID(4))
			var base, patch map[string]any
			if err := yaml.Unmarshal([]byte(body), &base); err != nil {
				t.Fatal(err)
			}
			if tc.change != "" {
				if err := yaml.Unmarshal([]byte(tc.change), &patch); err != nil {
					t.Fatal(err)
				}
				for key, value := range patch {
					maps.Copy(base[key].(map[string]any), value.(map[string]any))
				}
			}
			raw, err := yaml.Marshal(base)
			if err != nil {
				t.Fatal(err)
			}
			if err = yaml.Unmarshal(raw, &node); err != nil {
				t.Fatal(err)
			}
			s, err := ParseInstanceSettings(node, "instance-create")
			if (err == nil) != tc.ok {
				t.Fatalf("settings: %v", err)
			}
			if err == nil && (&instanceRun{t: &InstanceTest{s: s}}).createBody(instanceRequest{})["topologyOptimized"] != (tc.name == "topology") {
				t.Fatal("batch topology option omitted or changed")
			}
		})
	}
}

func TestInstanceSourceTimes(t *testing.T) {
	for _, tc := range []string{"valid", "future", "slow", "failed", "cadence"} {
		t.Run(tc, func(t *testing.T) {
			f := instanceFixture(2)
			m := f.meta(probeapi.FamilyHost)
			if tc == "future" {
				m.ReadAt.Time = time.Now().Add(time.Second)
			}
			if tc == "slow" {
				m.ReadAt.Time = m.SweepStart.Add(24 * time.Second)
			}
			if tc == "failed" {
				m.LastAttemptSucceeded = new(false)
			}
			if tc == "cadence" {
				m.SweepIntervalSeconds = 240
			}
			err := sourceMeta(m, probeapi.FamilyHost, f.process, 30)
			if tc == "cadence" && (err == nil || !strings.Contains(err.Error(), "probe host cadence is 240 seconds but runner.observation.intervalSeconds is 30")) {
				t.Fatalf("cadence diagnostic: %v", err)
			}
			if (err == nil) != (tc == "valid") {
				t.Fatalf("source times: %v", err)
			}
		})
	}
}

func TestInstanceSlots(t *testing.T) {
	for _, tc := range []struct{ name, slot string }{
		{"st-run-b0-c0-abc123-1", "st-run-b0-c0-1"},
		{"st-run-b0-c0-1", ""}, {"st-run-b0-c0-xyz123-1", ""}, {"st-run-b0-c0-abc1234-1", ""},
	} {
		slot, _ := instanceSlot(tc.name)
		if slot != tc.slot {
			t.Fatalf("slot=%q want=%q", slot, tc.slot)
		}
	}
}

func TestInstanceSettlementReview(t *testing.T) {
	for _, tc := range []struct {
		name     string
		deleting bool
		n, burst int
		want     result.Verdict
		failed   uint64
	}{
		{"native-orphan-outside", false, 5, 100, result.Failed, 0},
		{"native-duplicate-slot", false, 5, 100, result.Failed, 0},
		{"native-missing-binding", false, 5, 100, result.Unresolved, 0},
		{"rest-missing-binding", false, 5, 100, result.Unresolved, 0},
		{"final-rest-missing-binding", false, 5, 100, result.Unresolved, 0},
		{"dpu-error", false, 5, 100, result.Failed, 1},
		{"final-dpu-error", false, 5, 100, result.Failed, 1},
		{"prior-dpu-error", false, 6, 2, result.Failed, 1},
		{"dpu-missing", false, 5, 100, result.Unresolved, 0},
		{"prior-create-regression", false, 6, 2, result.Unresolved, 0},
		{"delete-label-regression", true, 5, 100, result.Unresolved, 0},
		{"delete-empty-label", true, 5, 100, result.Unresolved, 0},
		{"prior-delete-label", true, 6, 2, result.Unresolved, 0},
		{"prior-delete-regression", true, 6, 2, result.Unresolved, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, dir := instanceFixture(tc.n+1), t.TempDir()
				test, r, c := instanceCommand(t, f, "instance-create", "review-create", dir, tc.n)
				test.s.Instance.BurstSize = uint64(tc.burst)
				if tc.deleting {
					out, err := runInstance(test, r, c, false)
					if out.Verdict != result.Passed || err != nil {
						t.Fatalf("create fixture: %s %v", out.Verdict, err)
					}
					test, r, c = instanceCommand(t, f, "instance-delete", "review-delete", dir, tc.n)
					test.s.Instance.OwnedManifest = "review-create.owned.json"
				}
				f.action = func() {
					if f.created == 0 {
						return
					}
					switch tc.name {
					case "rest-missing-binding":
						f.state[probeapi.FamilyRestInstances][instanceUUID(1001)].Instance.HostID = nil
					case "native-orphan-outside", "native-duplicate-slot", "native-missing-binding":
						u := f.state[probeapi.FamilyInstance][instanceUUID(1001)]
						v := *u.Instance
						if tc.name == "native-orphan-outside" {
							v.HostID = new("h5")
						}
						if tc.name == "native-missing-binding" {
							v.HostID = nil
						}
						u.Instance, u.ID = &v, instanceUUID(5000)
						f.state[probeapi.FamilyInstance][u.ID] = u
					case "dpu-error", "prior-dpu-error":
						if tc.name == "prior-dpu-error" && f.created <= 2 {
							return
						}
						u := f.state[probeapi.FamilyDpu]["d0"]
						u.Stage, u.State = probeapi.StageError, "Error"
						f.state[probeapi.FamilyDpu]["d0"] = u
					case "dpu-missing":
						delete(f.state[probeapi.FamilyDpu], "d0")
					case "prior-create-regression":
						if f.created > 2 {
							u := f.state[probeapi.FamilyInstance][instanceUUID(1001)]
							u.Stage = probeapi.StageRegistered
							f.state[probeapi.FamilyInstance][u.ID] = u
						}
					default:
						deletes := 0
						for _, request := range f.requests {
							if strings.HasPrefix(request, "DELETE") {
								deletes++
							}
						}
						cleanup := strings.HasPrefix(tc.name, "delete-") && len(f.state[probeapi.FamilyInstance]) == 0 && strings.HasPrefix(f.requests[len(f.requests)-1], "PATCH")
						if tc.name == "delete-empty-label" && cleanup || tc.name == "prior-delete-label" && deletes > 2 {
							f.state[probeapi.FamilyRestMachines][instanceUUID(1)].RestMachine.Labels[instanceLabel] = ""
						} else if cleanup || tc.name == "prior-delete-regression" && deletes > 2 {
							u := f.state[probeapi.FamilyHost]["h0"]
							u.Stage, u.Host.Allocatable = probeapi.StageRegistered, new(false)
							f.state[probeapi.FamilyHost]["h0"] = u
						}
					}
				}
				if tc.name == "final-dpu-error" || tc.name == "final-rest-missing-binding" {
					sp, _ := test.deps.Store(context.Background())
					sp.Client.(*instanceMetrics).afterSummary = func() {
						f.mu.Lock()
						defer f.mu.Unlock()
						if tc.name == "final-rest-missing-binding" {
							f.state[probeapi.FamilyRestInstances][instanceUUID(1001)].Instance.HostID = nil
							return
						}
						u := f.state[probeapi.FamilyDpu]["d0"]
						u.Stage, u.State = probeapi.StageError, "Error"
						f.state[probeapi.FamilyDpu]["d0"] = u
					}
				}
				out, err := runInstance(test, r, c, false)
				if out.Verdict != tc.want || err != nil && !errors.Is(err, context.DeadlineExceeded) || len(out.Reasons) == 0 || !r.j.Unsettled() {
					t.Fatalf("verdict=%s pending=%t err=%v", out.Verdict, r.j.Unsettled(), err)
				}
				failed := uint64(0)
				for _, stage := range out.Stages {
					failed += stage.Units.Failed
				}
				if failed != tc.failed {
					t.Fatalf("failed=%d want=%d", failed, tc.failed)
				}
				if tc.failed > 0 {
					raw, e := os.ReadFile(filepath.Join(dir, "review-create.cohort.json"))
					if e != nil || !bytes.Contains(raw, []byte("DPU d0 entered Error")) {
						t.Fatalf("failure reason missing: %v", e)
					}
				}
			})
		})
	}
}

func TestInstancePublication(t *testing.T) {
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded, errors.New("journal"), errors.Join(instanceIssue("semantic"), errors.New("evidence"))} {
		op := &writer.OpError{Err: cause}
		if instanceExecutionError(op) != op {
			t.Fatalf("hard operation error suppressed: %v", cause)
		}
	}

	for _, tc := range []struct {
		name    string
		want    result.Verdict
		pending bool
	}{
		{"final-dpu-error", result.Failed, true}, {"burst-dpu-error", result.Failed, true},
		{"stale", result.NotReady, false}, {"read-error", result.NotReady, false},
		{"lost", result.Unresolved, true}, {"journal", result.Aborted, true},
		{"label-absent", result.Unresolved, true}, {"label-other", result.Unresolved, true},
		{"evidence", result.Aborted, true}, {"joined-evidence", result.Aborted, true},
		{"cancel", result.Aborted, true}, {"deadline", result.Aborted, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, dir := instanceFixture(6), t.TempDir()
				test, _, _ := instanceCommand(t, f, "instance-create", "publish", dir, 5)
				cfg := test.cfg
				cfg.ExecutionID, cfg.DryRun, cfg.Site.LockName, cfg.ConfigMaps.Result = "publish-pod", new(false), "lock", "publish-result"
				cfg.Timeouts, cfg.Journal.Claim = config.Timeouts{WholeRunSeconds: 7200, ReadinessSeconds: 300, FinalizeSeconds: 1800}, "c"
				outside, cancel := context.WithCancel(context.Background())
				defer cancel()
				cm := &oneCM{}
				d := run.Deps{Config: cfg, Lock: &lock.SiteLock{Cluster: clustertest.New("mat"), Name: "lock", Timing: lock.DefaultTiming}, CM: cm, Outside: outside}
				f.mode = tc.name
				if tc.name == "burst-dpu-error" {
					f.action = func() {
						if f.created > 0 {
							u := f.state[probeapi.FamilyDpu]["d0"]
							u.Stage, u.State = probeapi.StageError, "Error"
							f.state[probeapi.FamilyDpu]["d0"] = u
						}
					}
				}
				if tc.name == "journal" {
					d.JournalFault = func(*os.File, []byte) error { return errors.New("disk failed") }
				}
				if tc.name == "cancel" {
					f.action = func() {
						if f.created > 0 {
							cancel()
						}
					}
				}
				if tc.name == "deadline" {
					cfg.Timeouts.WholeRunSeconds = 2100
				}
				sp, _ := test.deps.Store(context.Background())
				sp.Client.(*instanceMetrics).afterSummary = func() {
					if tc.name == "final-dpu-error" || tc.name == "joined-evidence" {
						f.mu.Lock()
						u := f.state[probeapi.FamilyDpu]["d0"]
						u.Stage, u.State = probeapi.StageError, "Error"
						f.state[probeapi.FamilyDpu]["d0"] = u
						f.mu.Unlock()
					}
					if tc.name == "evidence" || tc.name == "joined-evidence" {
						if err := os.Mkdir(filepath.Join(dir, "publish.cohort.json"), 0700); err != nil {
							t.Error(err)
						}
					}
				}
				res := run.Execute(d, test)
				raw, err := os.ReadFile(res.Result.Path)
				var saved result.RunResult
				if err != nil || json.Unmarshal(raw, &saved) != nil {
					t.Fatalf("result: %v", err)
				}
				var summary result.RunResult
				if err := json.Unmarshal([]byte(cm.o.Data[result.ResultKey]), &summary); err != nil {
					t.Fatal(err)
				}
				if res.Verdict != tc.want || saved.Verdict != tc.want || summary.Verdict != tc.want {
					t.Fatalf("published=%s file=%s summary=%s reasons=%v", res.Verdict, saved.Verdict, summary.Verdict, res.Reasons)
				}
				if res.Lock.FinalState == nil || (*res.Lock.FinalState == string(lock.NeedsCleanup)) != tc.pending {
					t.Fatalf("lock=%+v", res.Lock)
				}
				if len(res.Reasons) == 0 {
					t.Fatal("failure reasons lost")
				}
				if tc.name == "final-dpu-error" || tc.name == "burst-dpu-error" || tc.name == "joined-evidence" {
					if len(res.Stages) != 1 || res.Stages[0].Status != result.StageFailed || res.Stages[0].Units.Failed != 1 {
						t.Fatalf("stages=%+v", res.Stages)
					}
				}
			})
		})
	}
}

func TestRESTTrust(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch || r.Header.Get("Authorization") != "Bearer token" {
			t.Error("wrong write or credential")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	for _, tc := range []struct {
		name, errorText string
		accepted        bool
	}{
		{"system", "", false}, {"private", "", true}, {"token-trim", "", true}, {"wrong-ca", "", false}, {"wrong-host", "", false},
		{"token-newline", "REST token file", false}, {"token-control", "REST token file", false}, {"token-unicode", "REST token file", false},
		{"missing", "cannot be read", false}, {"invalid", "no PEM certificate", false},
		{"oversize", "within 1 MiB", false}, {"directory", "must be regular", false}, {"fifo", "must be regular", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			s := InstanceSettings{}
			s.Rest.URL, s.Rest.TokenFile = server.URL, filepath.Join(dir, "token")
			token := map[string]string{"token-trim": " \ttoken\n", "token-newline": "sensitive\nvalue", "token-control": "sensitive\x00value", "token-unicode": "sensitiveévalue"}[tc.name]
			if token == "" {
				token = "token"
			}
			if err := os.WriteFile(s.Rest.TokenFile, []byte(token), 0600); err != nil {
				t.Fatal(err)
			}
			if tc.name != "system" {
				s.Rest.CAFile = filepath.Join(dir, "ca.pem")
				cert := server.Certificate()
				bundle := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw})
				if tc.name == "wrong-ca" {
					template := *cert
					template.Subject.CommonName, template.RawSubject = "other-root", nil
					der, err := x509.CreateCertificate(rand.Reader, &template, &template, cert.PublicKey, server.TLS.Certificates[0].PrivateKey)
					if err != nil {
						t.Fatal(err)
					}
					bundle = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
				}
				switch tc.name {
				case "invalid":
					bundle = []byte("sensitive-invalid-pem")
				case "oversize":
					bundle = append(bundle, bytes.Repeat([]byte("x"), (1<<20)+1-len(bundle))...)
				case "directory":
					s.Rest.CAFile = dir
				case "fifo":
					if err := syscall.Mkfifo(s.Rest.CAFile, 0600); err != nil {
						t.Fatal(err)
					}
				}
				if tc.name != "directory" && tc.name != "fifo" && tc.name != "missing" {
					if err := os.WriteFile(s.Rest.CAFile, bundle, 0600); err != nil {
						t.Fatal(err)
					}
				}
				if tc.name == "wrong-host" {
					s.Rest.URL = strings.Replace(server.URL, "127.0.0.1", "localhost", 1)
				}
			}
			r, err := newREST(s)
			if tc.errorText != "" {
				if err == nil || !strings.Contains(err.Error(), tc.errorText) || strings.Contains(err.Error(), "sensitive") {
					t.Fatalf("CA error=%v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer r.client.CloseIdleConnections()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			out := r.attempt(ctx, http.MethodPatch, "machine/"+instanceUUID(1), nil, 0)
			if (!out.NoReply && out.Rejected == nil) != tc.accepted {
				t.Fatalf("TLS acceptance=%+v", out)
			}
		})
	}
}

func TestRESTMachineIdentity(t *testing.T) {
	for _, id := range []string{"fm100htabc123", "machine-name_1.2~3", "", ".", "..", "a/b", "a?b", "a#b", "a%2fb", "a\\b", "a\nb"} {
		t.Run(id, func(t *testing.T) {
			valid := id == "fm100htabc123" || id == "machine-name_1.2~3"
			unit := probeapi.Unit{ID: id, RestMachine: &probeapi.RestMachineFields{ControllerID: id}}
			s := instanceSnapshot{rows: map[probeapi.Family]map[string]probeapi.Unit{probeapi.FamilyRestMachines: {id: unit}}}
			_, err := s.restHosts()
			if (err == nil) != valid {
				t.Fatalf("machine identity: %v", err)
			}
			if valid {
				s.rest = nil
				s.rows[probeapi.FamilyRestMachines]["duplicate"] = unit
				if _, err = s.restHosts(); err == nil {
					t.Fatal("ambiguous controller identity accepted")
				}
			}
		})
	}
}

func TestCalibrationAllowsBackgroundChurn(t *testing.T) {
	for _, churn := range []int{3, 10, -1} {
		t.Run(strconv.Itoa(churn), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := instanceFixture(7 + churn)
				if churn >= 0 {
					f.state[probeapi.FamilyHost]["h0"].Host.Allocatable = new(false)
					f.state[probeapi.FamilyRestMachines][instanceUUID(2)].RestMachine.IsUsableByTenant = false
				}
				dir := t.TempDir()
				test, r, c := instanceCommand(t, f, "instance-create", "churn", dir, 5)
				var lag *calibrationLagProbe
				if churn < 0 {
					held := f.meta(probeapi.FamilyRestInstances)
					held.SweepStart.Time = time.Now().Add(-time.Second)
					held.ReadAt.Time = time.Now().Add(-time.Second / 2)
					lag = &calibrationLagProbe{instanceSite: f, started: time.Now(), held: held}
					test.p = lag
				}
				go func() {
					time.Sleep(20 * time.Second)
					f.mu.Lock()
					if churn < 0 {
						f.state[probeapi.FamilyHost]["h0"].Host.Allocatable = new(false)
						f.mu.Unlock()
						time.Sleep(30 * time.Second)
						f.mu.Lock()
						f.state[probeapi.FamilyHost]["h0"].Host.Allocatable = new(true)
						f.mu.Unlock()
						return
					}
					defer f.mu.Unlock()
					f.state[probeapi.FamilyHost]["h0"].Host.Allocatable = new(true)
					f.state[probeapi.FamilyRestMachines][instanceUUID(2)].RestMachine.IsUsableByTenant = true
					for i := 7; i < 7+churn; i++ {
						f.state[probeapi.FamilyHost][fmt.Sprintf("h%d", i)].Host.Allocatable = new(false)
					}
				}()
				out, err := runInstance(test, r, c, true)
				if err != nil || out.Verdict != result.Go {
					t.Fatalf("calibration rejected background allocation churn: %s %v %v", out.Verdict, err, out.Reasons)
				}
				var plan instancePlan
				raw, err := os.ReadFile(filepath.Join(dir, "churn.instance-plan.json"))
				if err != nil || json.Unmarshal(raw, &plan) != nil {
					t.Fatal("planned cohort missing")
				}
				for _, h := range plan.Cohort {
					index, _ := strconv.Atoi(strings.TrimPrefix(h.ID, "h"))
					if churn >= 0 && (index < 2 || index >= 7) || churn < 0 && index == 0 {
						t.Fatalf("unstable candidate selected: %s", h.ID)
					}
				}
				if lag != nil && !lag.observed {
					t.Fatal("recovery test never observed ineligibility")
				}
				expected := 2 + churn
				events := expected
				if churn < 0 {
					events = 2
				}
				if len(plan.Cohort) != 5 || len(plan.Excluded) != expected {
					t.Fatal("calibration exclusions lost")
				}
				var detail instanceDetail
				if json.Unmarshal(out.Detail, &detail) != nil || len(detail.BackgroundChanges) != min(8, events) {
					t.Fatal("bounded background list lost")
				}
				raw, err = os.ReadFile(filepath.Join(dir, "churn.calibration-background.json"))
				var changes []transition
				if err != nil || json.Unmarshal(raw, &changes) != nil || len(changes) != events {
					t.Fatal("complete background evidence lost")
				}
				for _, event := range changes {
					if event.BeforeSource == nil || !event.Source.SweepStart.After(event.BeforeSource.ReadAt.Time) || event.Before == event.After {
						t.Fatal("background source times or before-image missing")
					}
				}
				if len(f.requests) != 0 || r.j.Unsettled() {
					t.Fatal("calibration or dry plan mutated the site")
				}
			})
		})
	}
}

func TestCalibrationRejectsSourceRestart(t *testing.T) {
	f := instanceFixture(6)
	test, _, _ := instanceCommand(t, f, "instance-create", "restart-calibration", t.TempDir(), 5)
	until := time.Now().Add(time.Minute)
	first, err := readInstanceSnapshot(t.Context(), f, until, test.s)
	if err != nil {
		t.Fatal(err)
	}
	f.process++
	_, err = readInstanceSnapshot(t.Context(), f, until, test.s, first)
	if err == nil || !strings.Contains(err.Error(), "native probe process changed") || len(f.requests) != 0 {
		t.Fatalf("source restart accepted: %v", err)
	}
}

// Hold one valid REST family while native observations change and recover.
type calibrationLagProbe struct {
	*instanceSite
	started  time.Time
	held     probeapi.SweepInfo
	observed bool
}

func (p *calibrationLagProbe) Units(ctx context.Context, f probeapi.Filter, pattern *string, page probe.Page) (probeapi.UnitsResponse, error) {
	r, err := p.instanceSite.Units(ctx, f, pattern, page)
	if familyFor(f) == probeapi.FamilyRestInstances && time.Since(p.started) < 60*time.Second {
		r.SweepInfo = p.held
	}
	if familyFor(f) == probeapi.FamilyHost {
		for _, u := range r.Units {
			if u.ID == "h0" && !readyHost(u) {
				p.observed = true
			}
		}
	}
	return r, err
}

func TestCalibrationEvidenceQuota(t *testing.T) {
	f := instanceFixture(2)
	test, _, _ := instanceCommand(t, f, "instance-create", "quota", t.TempDir(), 2)
	first, err := readInstanceSnapshot(t.Context(), f, time.Now().Add(time.Minute), test.s)
	if err != nil {
		t.Fatal(err)
	}
	f.state[probeapi.FamilyHost]["h0"].Host.Allocatable = new(false)
	f.frame = -1
	next, err := readInstanceSnapshot(t.Context(), f, time.Now().Add(time.Minute), test.s)
	if err != nil {
		t.Fatal(err)
	}
	for _, full := range []bool{false, true} {
		in := instanceRun{}
		used := 2
		if full {
			used = 128 << 20
		}
		changes := []transition{}
		err = in.calibrationChanges(first, next, &changes, &used)
		if (err != nil) != full || full && len(changes) != 0 || !full && len(changes) != 1 {
			t.Fatalf("bounded calibration evidence: %v", err)
		}
	}
}

// A stalled full read must never become remembered absence at its deadline.
type labelReadTimeout struct {
	*instanceSite
	first   *atomic.Int64
	stalled atomic.Bool
}

func (p *labelReadTimeout) Units(ctx context.Context, f probeapi.Filter, n *string, page probe.Page) (probeapi.UnitsResponse, error) {
	if at := p.first.Load(); at != 0 && time.Since(time.Unix(0, at)) >= 90*time.Second && familyFor(f) == probeapi.FamilyRestMachines && page.Limit != nil && *page.Limit > 1 && p.stalled.CompareAndSwap(false, true) {
		<-ctx.Done()
		return probeapi.UnitsResponse{}, ctx.Err()
	}
	return p.instanceSite.Units(ctx, f, n, page)
}

type labelRetryTransport func(*http.Request) (*http.Response, error)

func (f labelRetryTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type labelRetryGate func() error

func (g labelRetryGate) Check() error { return g() }
func TestLabelRetryBoundaries(t *testing.T) {
	for _, mode := range []string{"read-timeout", "second-401", "dispatch-budget"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := instanceFixture(3)
				f.mode = "label-absent"
				test, r, c := instanceCommand(t, f, "instance-create", "retry-boundary", t.TempDir(), 2)
				var first atomic.Int64
				var delayed atomic.Bool
				calls := 0
				end := time.Now().Add(time.Hour)
				if mode == "read-timeout" {
					test.s.Observation.LabelReadbackSeconds = 120
					test.p = &labelReadTimeout{instanceSite: f, first: &first}
				}
				if mode == "dispatch-budget" {
					end = time.Now().Add(275 * time.Second)
					r.w = writer.New(r.j, labelRetryGate(func() error {
						if at := first.Load(); at != 0 && time.Since(time.Unix(0, at)) >= 90*time.Second && r.j.Counts().Attempts == 1 && delayed.CompareAndSwap(false, true) {
							time.Sleep(10 * time.Second)
						}
						return r.gate.Check()
					}), c.Stopper(), end)
				}
				test.rest.client.Transport = labelRetryTransport(func(req *http.Request) (*http.Response, error) {
					calls++
					if calls == 1 {
						first.Store(time.Now().UnixNano())
						return f.RoundTrip(req)
					}
					return &http.Response{StatusCode: 401, Body: io.NopCloser(strings.NewReader("refused"))}, nil
				})
				_, _ = test.Run(run.RunCtx{Ctx: c.Context(), Clock: run.RealClock{}, Deadline: end, EvidenceEnd: end.Add(time.Hour), ProbeReadEnd: end.Add(time.Hour)}, r.w)
				expected := 1
				if mode == "second-401" {
					expected = 2
				}
				if calls != expected || !r.j.Unsettled() {
					t.Fatalf("calls=%d expected=%d pending=%t", calls, expected, r.j.Unsettled())
				}
			})
		})
	}
}

func TestRESTLimiterRefusal(t *testing.T) {
	const limited = `{"source":"","message":"Request rate limit exceeded, please re-evaluate your request patterns","data":null}`
	for _, tc := range []struct {
		name, body string
		want       writer.Kind
	}{
		{"limiter", limited, writer.Failed},
		{"handler", `{"message":"Site workflow timed out","data":null}`, writer.Unclear},
		{"missing-data", strings.Replace(limited, `,"data":null`, "", 1), writer.Unclear},
		{"wrong-data", strings.Replace(limited, `null`, `{}`, 1), writer.Unclear},
		{"oversize", limited + strings.Repeat(" ", 4096), writer.Unclear},
		{"duplicate-message", strings.TrimSuffix(limited, "}") + `,"message":"other"}`, writer.Unclear},
		{"duplicate-data", strings.TrimSuffix(limited, "}") + `,"data":null}`, writer.Unclear},
		{"trailing", limited + "{}", writer.Unclear}, {"malformed", "{", writer.Unclear},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			r := restWrites{client: &http.Client{Transport: labelRetryTransport(func(*http.Request) (*http.Response, error) {
				calls++
				return &http.Response{StatusCode: 429, Body: io.NopCloser(strings.NewReader(tc.body))}, nil
			})}}
			r.settings.Rest.URL = "https://rest.example"
			rig := newRig(t)
			out, err := writer.Send(t.Context(), rig.w, writer.Call{WriteCall: journal.WriteCall{Kind: "create", Key: "limiter", RequestDigest: "digest"}, Retry: writer.RetryPolicy{MaxAttempts: 1, AttemptTimeout: time.Second}}, func(ctx context.Context) writer.Attempt {
				return r.attempt(ctx, http.MethodPost, "instance/batch", nil, 0)
			})
			if err != nil || out.Kind != tc.want || calls != 1 || rig.j.Unsettled() != (tc.want == writer.Unclear) {
				t.Fatalf("out=%+v calls=%d pending=%t err=%v", out, calls, rig.j.Unsettled(), err)
			}
		})
	}
}
