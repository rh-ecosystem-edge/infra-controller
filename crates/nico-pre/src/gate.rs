/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! The site-definition gate every checklist runs first. It proves the site
//! definition is usable before any stage check reads it: the decisions are
//! known register entries with valid options, the stated ones are present,
//! the implication and conflict rules hold, and the arithmetic invariants
//! of the network and host sections are met. A failing gate stops the
//! checklist with exit code 3 and the offending rows.

use std::collections::{BTreeSet, HashSet};
use std::str::FromStr;

use indexmap::IndexMap;
use ipnet::IpNet;
use mac_address::MacAddress;

use crate::check::Row;
use crate::register::Register;
use crate::sitedef::SiteDef;

/// Runs every gate check and returns one row per invariant, or one row per
/// offending value when an invariant fails.
pub(crate) fn run(sd: &SiteDef, reg: &Register) -> Vec<Row> {
    let mut rows = Vec::new();
    decisions(sd, reg, &mut rows);
    network(sd, &mut rows);
    hosts(sd, &mut rows);
    rows
}

/// The decisions in effect after the implication rules: stated ones first,
/// then what the rules imply from them. The second map says which decision
/// implied each implied entry.
pub(crate) struct EffectiveDecisions {
    /// Every value is a list: one entry for a single-select, any number for an "Any of".
    pub(crate) values: IndexMap<String, Vec<String>>,
    pub(crate) implied_by: IndexMap<String, String>,
}

/// True when a stated value is usable: every listed option exists, and a
/// single-select carries exactly one.
fn valid_value(d: &crate::register::Decision, vs: &[String]) -> bool {
    (d.multiple || vs.len() == 1) && vs.iter().all(|v| d.option(v).is_some())
}

/// Applies the register's implication rules to the stated decisions until
/// nothing changes. Stated entries with an unknown id or option are left
/// out; `decisions` reports those.
pub(crate) fn effective_decisions(sd: &SiteDef, reg: &Register) -> EffectiveDecisions {
    effective_decisions_from(&sd.decisions, reg)
}

/// [`effective_decisions`] on a bare decisions map, for the template writer.
pub(crate) fn effective_decisions_from(
    stated: &IndexMap<String, crate::sitedef::DecisionValue>,
    reg: &Register,
) -> EffectiveDecisions {
    let mut values: IndexMap<String, Vec<String>> = stated
        .iter()
        .filter(|(id, value)| reg.get(id).is_some_and(|d| valid_value(d, &value.values())))
        .map(|(id, value)| (id.clone(), value.values()))
        .collect();
    let mut implied_by = IndexMap::new();
    loop {
        let mut changed = false;
        for rule in &reg.rules {
            let held = values.get(&rule.when.id).map(Vec::as_slice);
            if !rule.when.holds(held) {
                continue;
            }
            for implied in &rule.set {
                if !values.contains_key(&implied.id) {
                    values.insert(implied.id.clone(), vec![implied.option.clone()]);
                    implied_by.insert(implied.id.clone(), rule.when.id.clone());
                    changed = true;
                }
            }
        }
        if !changed {
            break;
        }
    }
    EffectiveDecisions { values, implied_by }
}

fn decisions(sd: &SiteDef, reg: &Register, rows: &mut Vec<Row>) {
    let before = rows.len();

    for (id, value) in &sd.decisions {
        let vs = value.values();
        match reg.get(id) {
            None => rows.push(
                Row::fail(
                    format!("sitedef.decisions.{id}"),
                    "a register entry",
                    format!("`{id}` is not in the register"),
                )
                .with_fix("remove it, or run `nico-pre decisions` to list the entries"),
            ),
            Some(d) if !d.multiple && vs.len() != 1 => rows.push(
                Row::fail(
                    format!("sitedef.decisions.{id}"),
                    format!("exactly one of: {}", d.option_names()),
                    format!("{} values: {}", vs.len(), quoted(&vs)),
                )
                .with_fix("state one option; only `Any of` entries take a list"),
            ),
            Some(d) => {
                for v in vs.iter().filter(|v| d.option(v).is_none()) {
                    rows.push(
                        Row::fail(
                            format!("sitedef.decisions.{id}"),
                            format!("one of: {}", d.option_names()),
                            format!("`{v}`"),
                        )
                        .with_fix("state the option by its short name"),
                    );
                }
            }
        }
    }

    let effective = effective_decisions(sd, reg);

    // A stated option that contradicts what another stated option implies.
    let mut reported = BTreeSet::new();
    for rule in &reg.rules {
        let held = effective.values.get(&rule.when.id).map(Vec::as_slice);
        if !rule.when.holds(held) {
            continue;
        }
        for implied in &rule.set {
            let Some(current) = effective.values.get(&implied.id) else {
                continue;
            };
            let agrees = current
                .iter()
                .any(|c| c.eq_ignore_ascii_case(&implied.option));
            if agrees || !reported.insert(implied.id.clone()) {
                continue;
            }
            rows.push(
                Row::fail(
                    format!("sitedef.decisions.{}", implied.id),
                    format!(
                        "`{}`, implied by {} = {}",
                        implied.option,
                        rule.when.id,
                        quoted(held.unwrap_or_default())
                    ),
                    quoted(current),
                )
                .with_fix("change one of the two decisions so they agree"),
            );
        }
    }

    // Present-but-invalid entries were reported above; only truly absent ones here.
    for d in reg.stated() {
        if !effective.values.contains_key(&d.id) && !sd.decisions.contains_key(&d.id) {
            rows.push(
                Row::fail(
                    format!("sitedef.decisions.{}", d.id),
                    "stated in the site definition; no collateral records it",
                    "absent",
                )
                .with_fix(format!(
                    "add `{}: <{}>` under `decisions`",
                    d.id,
                    d.option_names()
                )),
            );
        }
    }

    for conflict in &reg.conflicts {
        let held: Vec<_> = conflict
            .between
            .iter()
            .map(|t| (t, effective.values.get(&t.id).map(Vec::as_slice)))
            .collect();
        if held.iter().all(|(t, v)| t.holds(*v)) {
            let observed = held
                .iter()
                .map(|(t, v)| {
                    let shown = quoted(v.unwrap_or_default());
                    if t.lacks {
                        format!(
                            "{} = {} (without `{}`)",
                            t.id,
                            shown,
                            t.options.join("`, `")
                        )
                    } else {
                        format!("{} = {shown}", t.id)
                    }
                })
                .collect::<Vec<_>>()
                .join(" with ");
            rows.push(
                Row::fail(
                    "sitedef.decisions.conflict",
                    "no two held options exclude each other",
                    observed,
                )
                .with_fix(conflict.reason.clone()),
            );
        }
    }

    if rows.len() == before {
        rows.push(Row::pass(
            "sitedef.decisions",
            "known entries, valid options, stated ones present, rules hold",
            format!(
                "{} stated, {} implied, {} in effect",
                sd.decisions.len(),
                effective.implied_by.len(),
                effective.values.len()
            ),
        ));
    }
}

/// `` `a` `` for one value, `` `a`, `b` `` for several, `(none)` for an empty list.
fn quoted(values: &[String]) -> String {
    if values.is_empty() {
        return "(none)".to_string();
    }
    values
        .iter()
        .map(|v| format!("`{v}`"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Every prefix the site definition carries, with the key it came from.
fn prefixes(sd: &SiteDef) -> Vec<(String, IpNet)> {
    let n = &sd.network;
    let mut out = vec![
        (
            "network.forgeControlPlanePrefix".to_string(),
            n.forge_control_plane_prefix,
        ),
        (
            "network.forgeServiceVipPrefix".to_string(),
            n.forge_service_vip_prefix,
        ),
        (
            "network.forgeDpuLoopbackPrefix".to_string(),
            n.forge_dpu_loopback_prefix,
        ),
        (
            "network.kubernetes.podCidr".to_string(),
            n.kubernetes.pod_cidr,
        ),
        (
            "network.kubernetes.serviceCidr".to_string(),
            n.kubernetes.service_cidr,
        ),
    ];
    for (i, p) in n.forge_overlay_prefixes.iter().enumerate() {
        out.push((format!("network.forgeOverlayPrefixes[{i}]"), *p));
    }
    for (key, seg) in segments(sd) {
        out.push((format!("{key}.prefix"), seg.prefix));
    }
    if let Some(fnn) = &n.fnn {
        out.push((
            "network.fnn.vpcVrfLoopbackPrefix".to_string(),
            fnn.vpc_vrf_loopback_prefix,
        ));
    }
    out
}

struct SegmentRef {
    prefix: IpNet,
    gateway: std::net::IpAddr,
}

fn segments(sd: &SiteDef) -> Vec<(String, SegmentRef)> {
    let s = &sd.network.segments;
    let mut out = vec![
        (
            "network.segments.admin".to_string(),
            SegmentRef {
                prefix: s.admin.prefix,
                gateway: s.admin.gateway,
            },
        ),
        (
            "network.segments.controlPlaneOob".to_string(),
            SegmentRef {
                prefix: s.control_plane_oob.prefix,
                gateway: s.control_plane_oob.gateway,
            },
        ),
    ];
    for seg in &s.managed_host_bmc {
        out.push((
            format!("network.segments.managedHostBmc[{}]", seg.switch_name),
            SegmentRef {
                prefix: seg.prefix,
                gateway: seg.gateway,
            },
        ));
    }
    for seg in &s.host_inband {
        out.push((
            format!("network.segments.hostInband[{}]", seg.name),
            SegmentRef {
                prefix: seg.prefix,
                gateway: seg.gateway,
            },
        ));
    }
    out
}

fn network(sd: &SiteDef, rows: &mut Vec<Row>) {
    let n = &sd.network;
    let prefixes = prefixes(sd);

    let misaligned: Vec<_> = prefixes.iter().filter(|(_, p)| p.trunc() != *p).collect();
    if misaligned.is_empty() {
        rows.push(Row::pass(
            "sitedef.network.prefix-aligned",
            "every prefix is aligned to its mask",
            format!("{} prefixes", prefixes.len()),
        ));
    }
    for (key, p) in misaligned {
        rows.push(
            Row::fail(
                "sitedef.network.prefix-aligned",
                "a prefix aligned to its mask",
                format!("{key} = {p}"),
            )
            .with_fix(format!("use {}", p.trunc())),
        );
    }

    let mut overlaps = Vec::new();
    for (i, (ka, a)) in prefixes.iter().enumerate() {
        for (kb, b) in &prefixes[i + 1..] {
            if a.contains(b) || b.contains(a) {
                overlaps.push(format!("{ka} = {a} and {kb} = {b}"));
            }
        }
    }
    if overlaps.is_empty() {
        rows.push(Row::pass(
            "sitedef.network.no-overlap",
            "no two prefixes overlap",
            "disjoint",
        ));
    }
    for observed in overlaps {
        rows.push(
            Row::fail(
                "sitedef.network.no-overlap",
                "no two prefixes overlap",
                observed,
            )
            .with_fix("move one of the two prefixes"),
        );
    }

    let outside: Vec<_> = segments(sd)
        .into_iter()
        .filter(|(_, seg)| !seg.prefix.contains(&seg.gateway))
        .collect();
    if outside.is_empty() {
        rows.push(Row::pass(
            "sitedef.network.gateway-inside",
            "every segment gateway lies inside its prefix",
            format!("{} segments", segments(sd).len()),
        ));
    }
    for (key, seg) in outside {
        rows.push(
            Row::fail(
                "sitedef.network.gateway-inside",
                format!("a gateway inside {}", seg.prefix),
                format!("{key}.gateway = {}", seg.gateway),
            )
            .with_fix("fix the gateway or the prefix"),
        );
    }

    let mut pools: Vec<(String, u64, u64)> = vec![
        (
            "network.pools.vlan".to_string(),
            n.pools.vlan.start,
            n.pools.vlan.end,
        ),
        (
            "network.pools.vni".to_string(),
            n.pools.vni.start,
            n.pools.vni.end,
        ),
        (
            "network.bgpAsnStart..bgpAsnEnd".to_string(),
            u64::from(n.bgp_asn_start),
            u64::from(n.bgp_asn_end),
        ),
    ];
    if let Some(r) = n.pools.external_vpc_vni {
        pools.push(("network.pools.externalVpcVni".to_string(), r.start, r.end));
    }
    if let Some(fnn) = &n.fnn {
        for (i, r) in fnn.pools_vpc_vni.iter().enumerate() {
            pools.push((format!("network.fnn.poolsVpcVni[{i}]"), r.start, r.end));
        }
    }
    let inverted: Vec<_> = pools.iter().filter(|(_, s, e)| s > e).collect();
    if inverted.is_empty() {
        rows.push(Row::pass(
            "sitedef.network.pool-order",
            "every pool has start at or below end",
            format!("{} pools", pools.len()),
        ));
    }
    for (key, s, e) in inverted {
        rows.push(
            Row::fail(
                "sitedef.network.pool-order",
                "start at or below end",
                format!("{key} = {s}..{e}"),
            )
            .with_fix("swap or correct the bounds"),
        );
    }

    asn_layout(sd, rows);
}

/// The ASN range holds the site controller ASNs and leaves a per-DPU pool that the
/// datacenter ASN does not fall into. Derived by convention (routes = start, node i =
/// start + i) unless `asnLayout` overrides it.
fn asn_layout(sd: &SiteDef, rows: &mut Vec<Row>) {
    const ROW: &str = "sitedef.network.asn-layout";
    let n = &sd.network;
    let range = n.bgp_asn_start..=n.bgp_asn_end;
    let mut problems: Vec<(String, String)> = Vec::new();

    if range.contains(&n.datacenter_asn) {
        problems.push((
            format!("network.datacenterAsn = {}", n.datacenter_asn),
            "move the per-DPU ASN range or the datacenter ASN".to_string(),
        ));
    }
    let ids: Vec<u32> = sd
        .hosts
        .site_controllers
        .iter()
        .map(|c| c.node_id)
        .collect();
    if let Some(m) = n
        .asn_layout
        .as_ref()
        .and_then(|l| l.site_controllers.as_ref())
    {
        for id in m.keys().filter(|id| !ids.contains(id)) {
            problems.push((
                format!("network.asnLayout.siteControllers names nodeId {id}, which no site controller has"),
                "key the overrides by the site controllers' nodeId".to_string(),
            ));
        }
    }
    let routes = n.site_controller_routes_asn();
    let mut reserved: Vec<u32> = vec![routes];
    reserved.extend(ids.iter().map(|&id| n.site_controller_asn(id)));
    if n.asn_layout.is_none() {
        for (&id, &asn) in ids.iter().zip(&reserved[1..]) {
            if !range.contains(&asn) {
                problems.push((
                    format!(
                        "nodeId {id} would take {asn}, outside {}..{}",
                        n.bgp_asn_start, n.bgp_asn_end
                    ),
                    "widen bgpAsnEnd, or state asnLayout".to_string(),
                ));
            }
        }
    }
    let dpu_pool = range.filter(|a| !reserved.contains(a)).count();
    if dpu_pool == 0 {
        problems.push((
            format!(
                "no ASN left for DPUs in {}..{}",
                n.bgp_asn_start, n.bgp_asn_end
            ),
            "widen bgpAsnEnd".to_string(),
        ));
    }

    if problems.is_empty() {
        let nodes: Vec<String> = ids
            .iter()
            .map(|&id| n.site_controller_asn(id).to_string())
            .collect();
        rows.push(Row::pass(
            ROW,
            "the ASN range holds the site controller ASNs and leaves a per-DPU pool clear of the datacenter ASN",
            format!(
                "routes {routes}, nodes {}, {dpu_pool} for DPUs in {}..{}",
                nodes.join(" "),
                n.bgp_asn_start,
                n.bgp_asn_end
            ),
        ));
    }
    for (observed, fix) in problems {
        rows.push(Row::fail(ROW, "a consistent ASN layout", observed).with_fix(fix));
    }
}

fn hosts(sd: &SiteDef, rows: &mut Vec<Row>) {
    let controllers = &sd.hosts.site_controllers;

    let mut ids: Vec<u32> = controllers.iter().map(|c| c.node_id).collect();
    ids.sort_unstable();
    let expected: Vec<u32> = (1..=u32::try_from(controllers.len()).unwrap_or(u32::MAX)).collect();
    if ids == expected {
        rows.push(Row::pass(
            "sitedef.hosts.node-ids",
            "node ids run from one without gaps",
            format!("1..{}", controllers.len()),
        ));
    } else {
        rows.push(
            Row::fail(
                "sitedef.hosts.node-ids",
                format!("1..{} without gaps or repeats", controllers.len()),
                format!("{ids:?}"),
            )
            .with_fix("renumber the site controllers"),
        );
    }

    let mut duplicates = Vec::new();
    let mut names = HashSet::new();
    let mut serials = HashSet::new();
    for c in controllers {
        if !names.insert(c.host_name.as_str()) {
            duplicates.push(format!("hostName `{}`", c.host_name));
        }
        if !serials.insert(c.host_serial.as_str()) {
            duplicates.push(format!("hostSerial `{}`", c.host_serial));
        }
    }
    if duplicates.is_empty() {
        rows.push(Row::pass(
            "sitedef.hosts.unique",
            "unique hostnames and serials",
            format!("{} site controllers", controllers.len()),
        ));
    }
    for observed in duplicates {
        rows.push(
            Row::fail(
                "sitedef.hosts.unique",
                "unique hostnames and serials",
                format!("{observed} repeats"),
            )
            .with_fix("give every site controller its own hostname and serial"),
        );
    }

    let bad_macs: Vec<_> = controllers
        .iter()
        .filter_map(|c| c.p0_mac.as_deref().map(|m| (c.host_name.as_str(), m)))
        .filter(|(_, m)| MacAddress::from_str(m).is_err())
        .collect();
    if bad_macs.is_empty() {
        rows.push(Row::pass(
            "sitedef.hosts.p0-mac",
            "every stated p0 MAC parses",
            format!(
                "{} stated",
                controllers.iter().filter(|c| c.p0_mac.is_some()).count()
            ),
        ));
    }
    for (host, mac) in bad_macs {
        rows.push(
            Row::fail(
                "sitedef.hosts.p0-mac",
                "a MAC address like aa:bb:cc:dd:ee:ff",
                format!("{host}: `{mac}`"),
            )
            .with_fix("correct the MAC or leave p0Mac out until post-power-cycle.sh fills it in"),
        );
    }

    // Optional vantage points: when stated, every hostInband probe names a segment.
    if let Some(probes) = &sd.hosts.probe_hosts {
        let segments: HashSet<&str> = sd
            .network
            .segments
            .host_inband
            .iter()
            .map(|s| s.name.as_str())
            .collect();
        let unknown: Vec<&str> = probes
            .host_inband
            .iter()
            .map(|p| p.name.as_str())
            .filter(|n| !segments.contains(n))
            .collect();
        if unknown.is_empty() {
            rows.push(Row::pass(
                "sitedef.hosts.probe-hosts",
                "every hostInband probe names a hostInband segment",
                format!(
                    "{} hostInband probes{}",
                    probes.host_inband.len(),
                    if probes.admin.is_some() {
                        ", one admin probe"
                    } else {
                        ""
                    }
                ),
            ));
        } else {
            rows.push(
                Row::fail(
                    "sitedef.hosts.probe-hosts",
                    "every hostInband probe names a hostInband segment",
                    format!("no segment named {}", unknown.join(", ")),
                )
                .with_fix("use the segment's `name` from network.segments.hostInband"),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::check::Status;
    use crate::sitedef::parse;

    const SAMPLE: &str = include_str!("../examples/site-def.yaml");

    #[test]
    fn sample_passes_the_gate() {
        let sd = parse(SAMPLE).expect("sample parses");
        let rows = run(&sd, Register::builtin());
        let failures: Vec<_> = rows.iter().filter(|r| r.status != Status::Pass).collect();
        assert!(failures.is_empty(), "{failures:#?}");
        assert_eq!(rows.len(), 10, "one pass row per invariant: {rows:#?}");
    }

    #[test]
    fn a_probe_on_an_unknown_segment_fails_the_gate() {
        let text = SAMPLE.replacen(
            "{ name: rack1, address: 10.10.72.250 }",
            "{ name: rack9, address: 10.10.72.250 }",
            1,
        );
        assert_ne!(text, SAMPLE, "splice target missing");
        let sd = parse(&text).expect("parses");
        let rows = run(&sd, Register::builtin());
        let row = rows
            .iter()
            .find(|r| r.id == "sitedef.hosts.probe-hosts")
            .expect("row");
        assert_eq!(row.status, Status::Fail);
        assert!(row.observed.contains("rack9"), "{}", row.observed);
    }

    #[test]
    fn implications_fill_in_what_the_stated_decisions_imply() {
        let sd = parse(SAMPLE).expect("sample parses");
        let effective = effective_decisions(&sd, Register::builtin());
        let one = |s: &str| vec![s.to_string()];
        // An air-gapped staging host implies the external registry answer.
        assert_eq!(
            effective.values.get("external-registry-access"),
            Some(&one("Not applicable"))
        );
        assert_eq!(
            effective
                .implied_by
                .get("external-registry-access")
                .map(String::as_str),
            Some("staging-host")
        );
        // A `lacks` rule: the pinned Optional components list has no RMS box, so
        // rms is implied Not applicable, and through it rack-profiles.
        assert_eq!(effective.values.get("rms"), Some(&one("Not applicable")));
        assert_eq!(
            effective
                .implied_by
                .get("rack-profiles")
                .map(String::as_str),
            Some("rms")
        );
        // Stated entries are never overwritten by an implication.
        assert_eq!(effective.values.get("build-host"), Some(&one("Yes")));
        assert!(!effective.implied_by.contains_key("build-host"));
    }

    /// One broken value per invariant; each case names the row id that must fail
    /// and a fragment the row's observed text must carry.
    #[test]
    fn each_invariant_names_its_offender() {
        struct Case {
            name: &'static str,
            from: &'static str,
            to: &'static str,
            row: &'static str,
            observed: &'static str,
        }
        let cases = [
            Case {
                name: "unknown decision",
                from: "  build-host: Yes\n",
                to: "  build-host: Yes\n  colour-scheme: blue\n",
                row: "sitedef.decisions.colour-scheme",
                observed: "not in the register",
            },
            Case {
                name: "invalid option",
                from: "staging-host: Air-gapped with a transfer path",
                to: "staging-host: Sometimes connected",
                row: "sitedef.decisions.staging-host",
                observed: "Sometimes connected",
            },
            Case {
                name: "stated decision absent",
                from: "  build-host: Yes\n",
                to: "",
                row: "sitedef.decisions.build-host",
                observed: "absent",
            },
            Case {
                name: "implied decision may be stated when it agrees",
                from: "  build-host: Yes\n",
                to: "  build-host: Yes\n  external-registry-access: Not applicable\n",
                row: "sitedef.decisions",
                observed: "6 stated",
            },
            Case {
                name: "stated value contradicts an implication",
                from: "  build-host: Yes\n",
                to: "  build-host: Yes\n  external-registry-access: Direct\n",
                row: "sitedef.decisions.external-registry-access",
                observed: "`Direct`",
            },
            Case {
                name: "conflict between held options",
                from: "  build-host: Yes\n",
                to: "  build-host: Yes\n  source-of-the-nico-images: Pulled from NGC\n",
                row: "sitedef.decisions.conflict",
                observed: "source-of-the-nico-images = `Pulled from NGC`",
            },
            Case {
                name: "conflict with an unticked box",
                from: "  build-host: Yes\n",
                to: "  build-host: Yes\n  rack-profiles: Built-in profiles\n",
                row: "sitedef.decisions.conflict",
                observed: "rack-profiles = `Built-in profiles`",
            },
            Case {
                name: "list given to a single-select",
                from: "  build-host: Yes\n",
                to: "  build-host: [Yes, No]\n",
                row: "sitedef.decisions.build-host",
                observed: "2 values",
            },
            Case {
                name: "misaligned prefix",
                from: "forgeServiceVipPrefix: 10.10.126.192/27",
                to: "forgeServiceVipPrefix: 10.10.126.193/27",
                row: "sitedef.network.prefix-aligned",
                observed: "network.forgeServiceVipPrefix",
            },
            Case {
                name: "overlapping prefixes",
                from: "admin:           { prefix: 10.10.60.0/24, gateway: 10.10.60.1",
                to: "admin:           { prefix: 10.10.63.128/26, gateway: 10.10.63.129",
                row: "sitedef.network.no-overlap",
                observed: "network.segments.admin.prefix",
            },
            Case {
                name: "gateway outside its prefix",
                from: "gateway: 10.10.248.62, mtu: 1500 }",
                to: "gateway: 10.10.248.1, mtu: 1500 }",
                row: "sitedef.network.gateway-inside",
                observed: "network.segments.controlPlaneOob.gateway",
            },
            Case {
                name: "inverted pool",
                from: "vlan: { start: 100, end: 501 }",
                to: "vlan: { start: 501, end: 100 }",
                row: "sitedef.network.pool-order",
                observed: "network.pools.vlan",
            },
            Case {
                name: "datacenter ASN inside the per-DPU pool",
                from: "datacenterAsn: 65034",
                to: "datacenterAsn: 4244766865",
                row: "sitedef.network.asn-layout",
                observed: "network.datacenterAsn",
            },
            Case {
                name: "ASN range too small for the site controllers and a DPU",
                from: "bgpAsnEnd: 4244766869",
                to: "bgpAsnEnd: 4244766863",
                row: "sitedef.network.asn-layout",
                observed: "no ASN left for DPUs",
            },
            Case {
                name: "ASN override for a node that does not exist",
                from: "bgpAsnEnd: 4244766869",
                to: "bgpAsnEnd: 4244766869\n  asnLayout: { siteControllers: { 7: 4244766867 } }",
                row: "sitedef.network.asn-layout",
                observed: "nodeId 7",
            },
            Case {
                name: "node id gap",
                from: "nodeId: 3, bmc: 10.10.248.51",
                to: "nodeId: 4, bmc: 10.10.248.51",
                row: "sitedef.hosts.node-ids",
                observed: "[1, 2, 4]",
            },
            Case {
                name: "duplicate hostname",
                from: "hostName: sc3,",
                to: "hostName: sc1,",
                row: "sitedef.hosts.unique",
                observed: "hostName `sc1`",
            },
            Case {
                name: "unparsable MAC",
                from: "p0Mac: \"aa:bb:cc:dd:ee:03\"",
                to: "p0Mac: \"not-a-mac\"",
                row: "sitedef.hosts.p0-mac",
                observed: "sc3: `not-a-mac`",
            },
        ];
        for case in &cases {
            assert!(
                SAMPLE.contains(case.from),
                "{}: fixture anchor missing",
                case.name
            );
            let sd = parse(&SAMPLE.replacen(case.from, case.to, 1))
                .unwrap_or_else(|e| panic!("{}: fixture must parse: {e}", case.name));
            let rows = run(&sd, Register::builtin());
            let hit = rows
                .iter()
                .find(|r| r.id == case.row && r.observed.contains(case.observed));
            assert!(
                hit.is_some(),
                "{}: no row {} with `{}` in {:#?}",
                case.name,
                case.row,
                case.observed,
                rows
            );
            let expect_pass = case.row == "sitedef.decisions";
            assert_eq!(
                hit.map(|r| r.status == Status::Pass),
                Some(expect_pass),
                "{}: status",
                case.name
            );
        }
    }
}
