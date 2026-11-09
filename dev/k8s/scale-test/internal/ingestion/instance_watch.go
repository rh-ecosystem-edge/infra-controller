// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package ingestion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/NVIDIA/infra-controller/dev/k8s/scale-test/internal/probeapi"
	"github.com/google/uuid"
)

type instanceRequest struct {
	Burst  int    `json:"burst"`
	Prefix string `json:"prefix"`
	Count  int    `json:"count"`
}

func readScript(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", instanceIssue("iPXE fixture file unavailable")
	}
	defer func() { _ = f.Close() }()
	b, err := io.ReadAll(io.LimitReader(f, 65537))
	if err != nil || len(b) > 65536 || !strings.HasPrefix(string(b), "#!ipxe") {
		return "", instanceIssue("pinned iPXE fixture is invalid or exceeds 64 KiB")
	}
	return string(b), nil
}
func (in *instanceRun) selectorValue(burst int) string {
	return fmt.Sprintf("%s-b%d", in.plan.RunID, burst)
}
func (in *instanceRun) selector(scope []int) bool {
	want := map[string]bool{}
	for _, i := range scope {
		want[in.plan.Cohort[i].ID] = true
	}
	got := map[string]bool{}
	for id, u := range in.latest.rest {
		if u.RestMachine.Labels[instanceLabel] == in.selectorValue(in.active) {
			got[id] = true
		}
	}
	return maps.Equal(want, got)
}

func (in *instanceRun) labels(scope []int, remove bool) (time.Time, error) {
	var last time.Time
	for _, i := range scope {
		host := in.plan.Cohort[i]
		r := in.latest.rest[host.ID]
		if !readyHost(in.latest.rows[probeapi.FamilyHost][host.ID]) || !restFree(r, in.plan.Type) {
			return last, instanceIssue("selected host lost eligibility before label update")
		}
		labels := maps.Clone(r.RestMachine.Labels)
		if labels == nil {
			labels = map[string]string{}
		}
		value, exists := labels[instanceLabel]
		if remove {
			if !exists || value != in.selectorValue(host.Burst) {
				return last, instanceIssue("owned selector label was changed")
			}
			delete(labels, instanceLabel)
		} else {
			if exists {
				return last, instanceIssue("selector label conflicts with prior ownership")
			}
			labels[instanceLabel] = in.selectorValue(host.Burst)
		}
		out, err := in.send("labels", http.MethodPatch, "machine/"+host.RestID, map[string]any{"labels": labels}, 0, host.ID)
		last = out.RepliedAt
		if err != nil {
			err = errors.Join(err, in.refresh())
			return last, err
		}
	}
	return last, nil
}

// A lost label reply is settled only by a complete post-write probe frame.
// A full fresh window without the desired change permits one same-op retry.
const errLabelAbsent = instanceIssue("label read-back window ended unsettled")

func (in *instanceRun) settleLabel(host CohortHost, op uint64, remove bool, after time.Time) error {
	until := minTime(in.end, time.Now().Add(time.Duration(in.t.s.Observation.LabelReadbackSeconds)*time.Second))
	ctx, cancel := context.WithDeadline(in.ctx, until)
	defer cancel()
	unsettled := instanceIssue("label read-back window ended without fresh absence proof")
	for {
		if !time.Now().Before(until) && in.ctx.Err() == nil && time.Now().Before(in.end) {
			return unsettled
		}
		if err := in.refresh(until); err != nil {
			return err
		}
		row := in.latest.rest[host.ID]
		if row.RestMachine == nil || row.ID != host.RestID {
			return instanceIssue("label read-back host identity changed")
		}
		value, present := row.RestMachine.Labels[instanceLabel]
		source := in.latest.meta[probeapi.FamilyRestMachines]
		proof := map[string]any{"op": op, "host": host.ID, "remove": remove, "present": present, "value": value, "expected": in.selectorValue(host.Burst), "source": source, "deadline": until.UTC().Format(time.RFC3339Nano)}
		if err := in.w.Record(in.ctx, "labelReadback", proof); err != nil {
			return err
		}
		if source.SweepStart.After(after) {
			unsettled = errLabelAbsent
			if present && value != in.selectorValue(host.Burst) {
				return instanceIssue("label read-back found another owner")
			}
			if remove && !present || !remove && present {
				return in.w.SettleLabel(in.ctx, op, proof)
			}
		}
		if err := waitInstance(ctx, in.t.cfg.Probe.PollSeconds); err != nil {
			if in.ctx.Err() == nil && time.Now().Before(in.end) {
				return unsettled
			}
			return err
		}
	}
}

// agreement checks tenants and owned placement without treating an in-flight
// native/REST lag as completion. Exact end agreement is checked by settled.
func (in *instanceRun) agreement(s *instanceSnapshot) error {
	native := s.rows[probeapi.FamilyInstance]
	rest := s.rows[probeapi.FamilyRestInstances]
	machines := map[string]string{}
	for id, u := range s.rest {
		machines[u.ID] = id
	}
	seenNative, seenHosts := map[string]bool{}, map[string]bool{}
	slots, suffixes := map[string]string{}, map[string]string{}
	for id, u := range rest {
		v := u.Instance
		if uuid.Validate(id) != nil || v == nil || v.Name == nil || v.Tenant == nil || *v.Tenant != in.t.s.Instance.TenantID {
			return instanceIssue("REST instance tenant evidence is invalid")
		}
		burst, owned := in.ownedName(*v.Name)
		if receiptBurst, ok := in.created[id]; ok && (!owned || receiptBurst != burst) {
			return errCohort
		}
		if !owned {
			continue
		}
		slot, suffix := instanceSlot(*v.Name)
		prefix := slot[:strings.LastIndex(slot, "-")]
		if prior := slots[slot]; prior != "" && prior != id {
			return errCohort
		}
		if prior := suffixes[prefix]; prior != "" && prior != suffix {
			return errCohort
		}
		slots[slot], suffixes[prefix] = id, suffix
		if burst > in.active && !in.deleting {
			return errCohort
		}
		if v.HostID == nil {
			continue
		}
		hostID := machines[*v.HostID]
		index, selected := in.positions[hostID]
		if !selected || in.plan.Cohort[index].Burst != burst || seenHosts[hostID] {
			return errCohort
		}
		seenHosts[hostID] = true
		h := &in.plan.Cohort[index]
		if h.InstanceID != "" && h.InstanceID != id {
			return errCohort
		}
		h.InstanceID = id
		h.Name = *v.Name
		if u.ControllerInstanceID != nil && *u.ControllerInstanceID != "" {
			nid := *u.ControllerInstanceID
			if seenNative[nid] {
				return errCohort
			}
			seenNative[nid] = true
			h.NativeInstanceID = nid
			if n, ok := native[nid]; ok && (n.Instance == nil || n.Instance.Name == nil || *n.Instance.Name != h.Name || n.Instance.HostID == nil || *n.Instance.HostID != hostID) {
				return errCohort
			}
		}
	}
	nativeSlots, nativeHosts := map[string]bool{}, map[string]bool{}
	for id, u := range native {
		if u.Instance == nil || u.Instance.Tenant == nil || *u.Instance.Tenant != in.t.s.Rest.Org {
			return instanceIssue("native instance tenant evidence is invalid")
		}
		if u.Instance.Name != nil {
			if burst, owned := in.ownedName(*u.Instance.Name); owned {
				if burst > in.active && !in.deleting {
					return errCohort
				}
				if u.Instance.HostID == nil {
					continue
				} // Missing binding is unsettled evidence.
				host := *u.Instance.HostID
				i, selected := in.positions[host]
				slot, _ := instanceSlot(*u.Instance.Name)
				if !selected || in.plan.Cohort[i].Burst != burst || nativeSlots[slot] || nativeHosts[host] {
					return errCohort
				}
				h := in.plan.Cohort[i]
				if h.NativeInstanceID != "" && h.NativeInstanceID != id {
					return errCohort
				}
				nativeSlots[slot], nativeHosts[host] = true, true
			}
		}
	}
	if in.previous == nil {
		if len(native) != len(rest) {
			return instanceIssue("native and REST instance baselines disagree")
		}
		for _, u := range rest {
			if u.ControllerInstanceID == nil || native[*u.ControllerInstanceID].ID == "" || u.Instance.HostID == nil {
				return instanceIssue("initial instance identity agreement is incomplete")
			}
			n := native[*u.ControllerInstanceID]
			if n.Instance.HostID == nil || *n.Instance.HostID != machines[*u.Instance.HostID] || n.Instance.Name == nil || *n.Instance.Name != *u.Instance.Name {
				return instanceIssue("initial instance bindings disagree")
			}
		}
	}
	return nil
}

func (in *instanceRun) owned() error {
	p := &in.plan
	s := in.t.s
	if p.RunID == "" || p.Tenant != s.Instance.TenantID || p.Type != s.Instance.InstanceTypeID || p.Org != s.Rest.Org || p.Origin != s.Rest.URL || uint64(len(p.Cohort)) != s.Instance.N || len(p.Cohort) == 0 {
		return instanceIssue("owned manifest belongs to another fixture or count")
	}
	hosts, ids, natives := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, h := range p.Cohort {
		u := in.latest.rows[probeapi.FamilyHost][h.ID]
		r := in.latest.rest[h.ID]
		if hosts[h.ID] || ids[h.InstanceID] || natives[h.NativeInstanceID] || uuid.Validate(h.InstanceID) != nil || !safeMachineID(h.RestID) || h.NativeInstanceID == "" || h.Burst < 0 || h.Burst >= len(p.Cohort) || h.Name == "" || !strings.HasPrefix(h.Name, "st-"+p.RunID+"-b") {
			return instanceIssue("owned manifest has ambiguous identities")
		}
		if in.latest.unstable[h.ID] || u.Host == nil || u.Host.RackID == nil || *u.Host.RackID != h.Rack || u.BmcIP == nil || *u.BmcIP != h.BMC || in.mat[in.t.cfg.Namespace+"/"+h.Pod].UID != h.PodUID || r.ID != h.RestID || r.RestMachine.Labels[instanceLabel] != in.selectorValue(h.Burst) {
			return instanceIssue("owned host identity or labels changed")
		}
		ri := in.latest.rows[probeapi.FamilyRestInstances][h.InstanceID]
		ni := in.latest.rows[probeapi.FamilyInstance][h.NativeInstanceID]
		if ri.Instance == nil || ri.Instance.Name == nil || *ri.Instance.Name != h.Name || ri.Instance.HostID == nil || *ri.Instance.HostID != h.RestID || ri.ControllerInstanceID == nil || *ri.ControllerInstanceID != h.NativeInstanceID || ni.Instance == nil || ni.Instance.HostID == nil || *ni.Instance.HostID != h.ID {
			return instanceIssue("owned instance bindings changed")
		}
		hosts[h.ID], ids[h.InstanceID], natives[h.NativeInstanceID] = true, true, true
	}
	slices.SortFunc(p.Cohort, func(a, b CohortHost) int { return a.Burst - b.Burst })
	for _, request := range p.Requests {
		for slot := 1; slot <= request.Count; slot++ {
			in.names[fmt.Sprintf("%s-%d", request.Prefix, slot)] = request.Burst
		}
	}
	return nil
}

func (in *instanceRun) labelsCleared(burst int) bool {
	for _, h := range in.plan.Cohort {
		if h.Burst > burst {
			continue
		}
		r := in.latest.rest[h.ID]
		if r.RestMachine == nil {
			return false
		}
		if _, exists := r.RestMachine.Labels[instanceLabel]; exists {
			return false
		}
	}
	return true
}

func (in *instanceRun) settled(burst int) bool {
	expected := 0
	for _, h := range in.plan.Cohort {
		if h.Burst > burst {
			continue
		}
		expected++
		r := in.latest.rest[h.ID]
		if in.deleting {
			if in.latest.rows[probeapi.FamilyInstance][h.NativeInstanceID].ID != "" || in.latest.rows[probeapi.FamilyRestInstances][h.InstanceID].ID != "" || !readyHost(in.latest.rows[probeapi.FamilyHost][h.ID]) || !restFree(r, in.plan.Type) {
				return false
			}
		} else {
			if _, ok := in.created[h.InstanceID]; !ok || h.NativeInstanceID == "" {
				return false
			}
			ri := in.latest.rows[probeapi.FamilyRestInstances][h.InstanceID]
			ni := in.latest.rows[probeapi.FamilyInstance][h.NativeInstanceID]
			if ri.State != "Ready" || ri.Instance == nil || ri.Instance.HostID == nil || *ri.Instance.HostID != h.RestID || ni.Stage != probeapi.StageReady || ni.Instance == nil || ni.Instance.HostID == nil || *ni.Instance.HostID != h.ID || ni.Instance.Name == nil || *ni.Instance.Name != h.Name || ri.ControllerInstanceID == nil || *ri.ControllerInstanceID != h.NativeInstanceID || r.RestMachine == nil || r.RestMachine.InstanceID == nil || *r.RestMachine.InstanceID != h.InstanceID {
				return false
			}
		}
	}
	for _, family := range []probeapi.Family{probeapi.FamilyInstance, probeapi.FamilyRestInstances} {
		count := 0
		for _, u := range in.latest.rows[family] {
			if u.Instance != nil && u.Instance.Name != nil {
				if b, owned := in.ownedName(*u.Instance.Name); owned && b <= burst {
					count++
				}
			}
		}
		if in.deleting && count != 0 || !in.deleting && count != expected {
			return false
		}
	}
	return true
}

func (in *instanceRun) transitions(next *instanceSnapshot) error {
	var fatal error
	defer func() {
		for i, h := range in.plan.Cohort {
			if next.rows[probeapi.FamilyHost][h.ID].Stage == probeapi.StageError {
				in.plan.Cohort[i].FirstFailure = "host entered Error"
			}
		}
		for _, u := range next.rows[probeapi.FamilyInstance] {
			if u.Stage == probeapi.StageError && u.Instance != nil && u.Instance.HostID != nil && u.Instance.Name != nil {
				if _, ours := in.ownedName(*u.Instance.Name); ours {
					if i, ok := in.positions[*u.Instance.HostID]; ok {
						in.plan.Cohort[i].FirstFailure = "instance entered Error"
					}
				}
			}
		}
	}()
	stop := false
	for _, id := range slices.Sorted(maps.Keys(in.previous)) {
		before := in.previous[id]
		after := hostSignature(next, id)
		if before == after {
			continue
		}
		class := in.classes[id]
		event := transition{Observation: in.observation, Host: id, Class: class, Burst: in.active, Before: before, After: after, Source: next.meta[probeapi.FamilyHost], Attribution: "undetermined"}
		if err := in.evidence.append(event); err != nil {
			return err
		}
		in.previous[id] = after
		if class == "in-cohort" {
			h := in.plan.Cohort[in.positions[id]]
			record := next.rows[probeapi.FamilyExpected][h.ExpectedRecord].Expected
			if record == nil || !record.Linked || record.DeviceID == nil || *record.DeviceID != h.ID || record.BmcMac == nil || !strings.EqualFold(*record.BmcMac, h.MAC) {
				fatal = errors.Join(fatal, instanceIssue("selected expected-record identity changed"))
			}

			u := next.rows[probeapi.FamilyHost][id]
			if u.Host == nil || u.Host.RackID == nil || *u.Host.RackID != h.Rack || u.BmcIP == nil || *u.BmcIP != h.BMC || !slices.Equal(slices.Sorted(slices.Values(u.Host.DpuIDs)), h.DPUs) {
				fatal = errors.Join(fatal, instanceIssue("cohort host or dependency identity changed"))
			}
			if next.rest[id].ID != h.RestID {
				fatal = errors.Join(fatal, instanceIssue("cohort REST identity changed"))
			}
			for _, dpu := range h.DPUs {
				d := next.rows[probeapi.FamilyDpu][dpu]
				if d.Stage == probeapi.StageError {
					in.plan.Cohort[in.positions[id]].FirstFailure = "DPU " + dpu + " entered Error"
					fatal = errors.Join(fatal, errCohort)
				}
				if d.Stage != probeapi.StageReady || d.Dpu == nil || d.Dpu.HostID == nil || *d.Dpu.HostID != id {
					fatal = errors.Join(fatal, instanceIssue("cohort DPU dependency is not Ready"))
				}
			}
			if len(in.d.CohortTimeline) < 8 {
				in.d.CohortTimeline = append(in.d.CohortTimeline, event)
			}
			if next.rows[probeapi.FamilyHost][id].Stage == probeapi.StageError {
				in.plan.Cohort[in.positions[id]].FirstFailure = "host entered Error"
				fatal = errors.Join(fatal, errCohort)
			}
		} else {
			if len(in.d.BackgroundChanges) < 8 {
				in.d.BackgroundChanges = append(in.d.BackgroundChanges, event)
			}
			stop = stop || in.deleting && readyHost(in.latest.rows[probeapi.FamilyHost][id]) != readyHost(next.rows[probeapi.FamilyHost][id])
		}
	}
	for id := range next.rows[probeapi.FamilyHost] {
		if _, ok := in.previous[id]; !ok {
			event := transition{Observation: in.observation, Host: id, Class: "untouched-other", After: hostSignature(next, id), Source: next.meta[probeapi.FamilyHost], Attribution: "undetermined"}
			if err := in.evidence.append(event); err != nil {
				return err
			}
			in.previous[id] = event.After
			if len(in.d.BackgroundChanges) < 8 {
				in.d.BackgroundChanges = append(in.d.BackgroundChanges, event)
			}
			stop = true
		}
	}
	for id, u := range next.rows[probeapi.FamilyInstance] {
		owned := u.Instance != nil && u.Instance.Name != nil
		if owned {
			_, owned = in.ownedName(*u.Instance.Name)
		}
		if owned {
			if u.Stage == probeapi.StageError {
				if u.Instance.HostID != nil {
					if i, ok := in.positions[*u.Instance.HostID]; ok {
						in.plan.Cohort[i].FirstFailure = "instance entered Error"
					}
				}
				fatal = errors.Join(fatal, errCohort)
			}
			continue
		}
		old, ok := in.latest.rows[probeapi.FamilyInstance][id]
		a, _ := json.Marshal(old.Instance)
		b, _ := json.Marshal(u.Instance)
		if !ok || old.Stage != u.Stage || string(a) != string(b) {
			event := transition{Observation: in.observation, Host: id, Class: "untouched-other", Before: string(a), After: string(b), Source: next.meta[probeapi.FamilyInstance], Attribution: "undetermined"}
			if err := in.evidence.append(event); err != nil {
				return err
			}
			if len(in.d.BackgroundChanges) < 8 {
				in.d.BackgroundChanges = append(in.d.BackgroundChanges, event)
			}
			stop = true
		}
	}
	for id, u := range in.latest.rows[probeapi.FamilyInstance] {
		if next.rows[probeapi.FamilyInstance][id].ID == "" {
			owned := u.Instance != nil && u.Instance.Name != nil
			if owned {
				_, owned = in.ownedName(*u.Instance.Name)
			}
			if !owned {
				stop = true
			}
		}
	}
	beforeOthers, afterOthers := otherStates(in.latest), otherStates(next)
	union := maps.Clone(beforeOthers)
	maps.Copy(union, afterOthers)
	for _, id := range slices.Sorted(maps.Keys(union)) {
		if beforeOthers[id] == afterOthers[id] {
			continue
		}
		class := "untouched-other"
		if _, excluded := in.plan.Excluded[id]; excluded {
			class = "excluded-at-plan"
		}
		event := transition{Observation: in.observation, Host: id, Class: class, Before: beforeOthers[id], After: afterOthers[id], Source: next.meta[probeapi.FamilyDpu], Attribution: "undetermined"}
		if err := in.evidence.append(event); err != nil {
			return err
		}
		if len(in.d.BackgroundChanges) < 8 {
			in.d.BackgroundChanges = append(in.d.BackgroundChanges, event)
		}
		if _, known := beforeOthers[id]; !known {
			stop = true
		}
		if _, present := afterOthers[id]; !present {
			stop = true
		}
	}
	if fatal != nil {
		return fatal
	}
	if stop {
		return instanceIssue("background change invalidated a required counter or identity")
	}
	return nil
}

func otherStates(s *instanceSnapshot) map[string]string {
	out := map[string]string{}
	for id, u := range s.rows[probeapi.FamilyDpu] {
		if u.Dpu == nil || u.Dpu.HostID == nil {
			out["dpu:"+id] = string(u.Stage) + ":" + u.State
		}
	}
	return out
}

// REST inserts one six-hex batch suffix before each one-based slot.
func instanceSlot(name string) (string, string) {
	end := strings.LastIndex(name, "-")
	if end < 0 {
		return "", ""
	}
	begin := strings.LastIndex(name[:end], "-")
	if begin < 0 || end-begin != 7 {
		return "", ""
	}
	suffix := name[begin+1 : end]
	for _, c := range suffix {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return "", ""
		}
	}
	return name[:begin] + name[end:], suffix
}
func (in *instanceRun) ownedName(name string) (int, bool) {
	slot, _ := instanceSlot(name)
	burst, ok := in.names[slot]
	return burst, ok
}
