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

//! `nico-pre-config`: the collaterals against each other and against the site definition, from a laptop.

use ipnet::IpNet;

use crate::harness::{Check, Ctx, Outcome, todo};

pub(crate) const CHECKS: &[Check] = &[
    Check {
        id: "config.collaterals.present",
        expected: "every collateral the site definition names exists",
        detail: "Reads: every path under `collaterals`, resolved against the site definition's \
                 directory; the `dpu-in-the-site-controllers` decision. Reads no file contents.\n\
                 Pass: every named path exists; observed `7 files present`.\n\
                 Warn: `cpDpuSiteYaml` named while the site controllers carry no DPU; fix: drop \
                 it or correct the decision.\n\
                 Fail: `cpDpuSiteYaml` absent while the decision is a DPU, or any named path \
                 missing; observed names the key and the path (`coreValues: \
                 ./helm-prereqs/values/nico-core.yaml`); fix: correct the path or put the file \
                 there.\n\
                 Skip: never.\n\
                 Test: the example site-def against a directory of empty files; cases: all \
                 present passes; one missing fails naming key and path; `cpDpuSiteYaml` absent \
                 with a DPU fails; named with `no DPU` warns.",
        run: collaterals_present,
    },
    Check {
        id: "config.segments.admin",
        expected: "the admin segment fits the fleet: usable addresses beyond the reserved block cover counts.hosts with headroom, and its MTU is within pathMtu",
        detail: "Reads: `network.segments.admin`, `network.pathMtu`, `counts.hosts`.\n\
                 Pass: usable addresses (prefix size minus network, broadcast and the 5 the \
                 TOML's `[networks.admin] reserve_first` keeps by default) at or above \
                 `counts.hosts` with 20% spare, and the segment MTU within `pathMtu`; observed \
                 `10.10.60.0/24: 249 usable for 128 hosts, mtu 9000 within pathMtu 9000`.\n\
                 Warn: under 20% spare; observed `leaves 9 spare addresses for 240 hosts`; fix: \
                 widen the prefix before the fleet grows.\n\
                 Fail: fewer usable addresses than hosts, or MTU above `pathMtu`; observed names \
                 both numbers; fix: widen the prefix or lower `counts.hosts`, keep the MTU within \
                 `pathMtu`.\n\
                 Skip: never. Alignment and the gateway are the gate's rows and are not repeated.\n\
                 Test: the example spliced one line at a time; cases: 128 hosts passes; 300 \
                 fails; 240 warns; `pathMtu: 1500` fails naming both MTUs.",
        run: admin_segment,
    },
    Check {
        id: "config.consistency.prefixes",
        expected: "prefixes across all collaterals aligned and disjoint, gateways inside",
        detail: "Reads: every prefix in the collaterals: the CP-DPU site yaml (control plane, \
                 service VIP, DPU loopback, admin, BMC, hostInband, overlay), the TOML's network \
                 segments and security prefixes, the MetalLB pools, the Kubernetes pod and \
                 service CIDRs; all through `collaterals`.\n\
                 Pass: every prefix aligned to its mask, pairwise disjoint, every gateway inside \
                 its prefix; observed `N prefixes from 4 files, disjoint`.\n\
                 Fail: a misaligned prefix, an overlap, or a gateway outside; observed names both \
                 prefixes and the files that hold them (`admin 10.10.60.0/24 (site.yaml) overlaps \
                 rack1 10.10.60.0/23 (nico-core.yaml)`); fix: correct the file named.\n\
                 Skip: never. The gate proves the same for site-def alone; this check proves it \
                 for the files, so a prefix found only in site-def is not re-checked here.\n\
                 Test: a fixture directory of collaterals agreeing with the example site, edited \
                 per case; cases: the consistent set passes; a TOML segment shifted into \
                 another's range fails naming both files; a gateway moved outside its prefix in \
                 the site yaml fails; a misaligned prefix fails.",
        run: todo,
    },
    Check {
        id: "config.consistency.pools",
        expected: "pools sized to counts; the per-DPU ASN range clear",
        detail: "Reads: `forgeControlPlanePrefix`, `forgeServiceVipPrefix`, \
                 `forgeDpuLoopbackPrefix`, the BMC and hostInband segments, `counts`, the ASN \
                 layout (`bgpAsnStart`, `bgpAsnEnd`, `asnLayout`, `datacenterAsn`), the \
                 `optional-components` decision for the VIP count; the same values where the \
                 CP-DPU site yaml and the MetalLB config repeat them.\n\
                 Pass: the control-plane prefix holds the site controllers plus the gateway; the \
                 service VIP prefix holds the VIPs the ticked components need (api, dhcp, dns0, \
                 dns1, pxe, sshConsole, plus REST, Keycloak and observability when ticked); the \
                 DPU loopback pool and the per-DPU ASN range hold `counts.dpus`; the BMC and \
                 hostInband segments together hold `counts.hosts`; the per-DPU ASN range contains \
                 none of `datacenterAsn`, the site-controller ASNs or the common tenant ASN; \
                 observed one clause per pool with capacity and demand.\n\
                 Warn: a pool with under 20% spare.\n\
                 Fail: a pool too small or an ASN collision; observed the pool, its capacity and \
                 the demand (`loopback /26: 61 for 128 dpus`); fix names the field to widen.\n\
                 Skip: never. The DPU rows pass with `0 dpus` observed when `counts.dpus` is 0.\n\
                 Test: the example spliced; cases: the example passes; `counts.dpus: 200` fails \
                 the loopback and ASN rows; `datacenterAsn` set inside the per-DPU range fails; a \
                 hostInband prefix narrowed to /26 for 128 hosts fails; `counts.dpus: 0` passes.",
        run: todo,
    },
    Check {
        id: "config.consistency.site-controllers",
        expected: "at least three site controllers and Postgres sized to the host count",
        detail: "Reads: `hosts.siteControllers`, `counts.hosts`, the Postgres connection limit \
                 and PVC size in the helm-prereqs values, the sizing table (a built-in \
                 placeholder until the provider guide lands).\n\
                 Pass: three or more site controllers; connection limit and PVC size at or above \
                 the table row for `counts.hosts`; observed `3 controllers; postgres 500 \
                 connections, 200Gi for 128 hosts`.\n\
                 Warn: the sizing table is still the placeholder (reported on every run until the provider \
                 guide lands).\n\
                 Fail: fewer than three controllers; a Postgres value below the row; observed \
                 names the value and the minimum; fix: add a controller or raise the value in the \
                 helm-prereqs values.\n\
                 Skip: the Postgres rows when `core-postgres` is `External Postgres`; the \
                 controller count never.\n\
                 Test: the example spliced plus a helm-prereqs values fixture; cases: the example \
                 passes with the placeholder warning; two controllers fails; a PVC below the row \
                 fails naming both sizes; external Postgres skips the Postgres rows only.",
        run: todo,
    },
    Check {
        id: "config.sitedef.agreement",
        expected: "every quantity present in both equal, key for key",
        detail: "Reads: every quantity that appears both in site-def and in a collateral: \
                 segments and prefixes, VLANs and VNIs, ASNs, `apiHostname`, name and NTP \
                 servers, VIPs, `versions` as image tags, controller names and BMC addresses; \
                 through the collateral loaders.\n\
                 Pass: every pair equal; observed `N keys compared across 5 files, all equal`.\n\
                 Fail: any pair differs; observed one clause per mismatch with the key, both \
                 values and the file (`network.nameServer 10.10.0.11 vs nico-core.yaml [dns] \
                 upstream 10.10.0.13`); fix: pick one as the source and copy it.\n\
                 Skip: never. A key absent from a file is not reported here; that is the file's \
                 `.rules` check.\n\
                 Test: a fixture directory of collaterals agreeing with the example; cases: the \
                 agreeing set passes; one value changed in the TOML fails naming key, both values \
                 and file; the same value changed in site-def fails identically; a key absent \
                 from one file is not a mismatch.",
        run: todo,
    },
    Check {
        id: "config.cp-dpu-yaml.rules",
        expected: "the CP-DPU site yaml rows of the register",
        detail: "Reads: `collaterals.cpDpuSiteYaml`, parsed with the schema of \
                 `scripts/control-plane-dpu`; the `smn` decision; the site-def network values.\n\
                 Pass: required fields present, the `fnn` block present when SMN is on the \
                 control-plane DPU, values equal to site-def, and the builder's own validations \
                 hold (schema, ASN and prefix sizes, `routeTargetsToImport` shape, leaf-password \
                 rules), re-expressed in Rust with the builder unchanged; observed `site.yaml: N \
                 fields, builder rules hold`.\n\
                 Warn: a placeholder `p0` MAC; observed names the controller.\n\
                 Fail: a required field missing, a builder rule violated, or a value differing \
                 from site-def; observed quotes the rule as the builder would print it; fix: edit \
                 the field named.\n\
                 Skip: when `dpu-in-the-site-controllers` is `no DPU`.\n\
                 Test: `scripts/control-plane-dpu/site-sample.yaml` adapted to the example; \
                 cases: the sample passes; `fnn` removed with SMN on the DPU fails; a \
                 `routeTargetsToImport` entry in the wrong shape fails with the builder's \
                 message; a placeholder MAC warns; `no DPU` skips.",
        run: todo,
    },
    Check {
        id: "config.toml.rules",
        expected: "the NICo TOML inside the Core values parses with the real config types and meets its rows",
        detail: "Reads: the TOML string under `nicoApiSiteConfig` in `collaterals.coreValues`, \
                 parsed with the config types of `crates/api-core/src/cfg`; the decisions \
                 `managed-host-kinds-in-the-fleet`, `credential-store`, `evpn-overlay-peering`, \
                 `measured-boot-attestation`; the site-def network values.\n\
                 Pass: the TOML parses with the real types; site identity equals `site`; one \
                 network segment per managed host kind and address family, each MTU within \
                 `pathMtu`; `dpu_policy` equals the decision; routing profiles and route targets \
                 equal `network.fnn`; the network security prefixes include every BMC and \
                 control-plane prefix; the deprecated anycast key absent; observed \
                 `nico-core.yaml: TOML parses, N rules hold`.\n\
                 Warn: the deprecated anycast key present; an optional table absent that a ticked \
                 component needs.\n\
                 Fail: a parse error (quoted with its line), a missing segment for a host kind, a \
                 segment MTU above `pathMtu`, `dpu_policy` not matching the decision, a route \
                 target differing; observed names the table and key; fix names the key to edit.\n\
                 Skip: never.\n\
                 Test: a nico-core values fixture carrying a TOML known to parse (the nico-dev \
                 one); cases: the fixture passes; a misspelled key fails with the parser's line; \
                 `dpu_policy` flipped fails naming both values; a segment MTU above `pathMtu` \
                 fails; the anycast key added warns.",
        run: todo,
    },
    Check {
        id: "config.helm-prereqs.rules",
        expected: "the helm-prereqs values rows",
        detail: "Reads: `collaterals.helmPrereqsValues`, `restValues`, `siteAgentValues`; the \
                 decisions `recursive-resolver-for-managed-machines`, `credential-store`, \
                 `managed-host-dpu-generation`, `rest-api-auth`, `core-postgres`, `root-ca`.\n\
                 Pass: MetalLB mode and BGPPeer node selectors as the site controllers; every VIP \
                 the pods need inside the pools with the per-pod VIP counts; the DHCP server \
                 identifier and relay placeholders set; unbound local data or the external \
                 resolver per the decision; NTP equal to `ntpServers`; Kea rendering, hook \
                 architecture and memfile set; the Vault KV seed list with the BF4 entry when \
                 that generation is chosen; REST common values, Keycloak and Temporal flags per \
                 `rest-api-auth`; site-agent values pointing at the Core API; observed \
                 `values.yaml: N rules hold`.\n\
                 Warn: an optional value left at the chart default where the site differs (NTP, \
                 resolver).\n\
                 Fail: a rule violated; observed names the key, the value and the expectation; \
                 fix names the key.\n\
                 Skip: the REST rows when `optional-components` lacks `REST group`; the Vault \
                 rows when `credential-store` is `Postgres envelope-encrypted`.\n\
                 Test: the nico-dev values files as fixture; cases: the fixture passes; a VIP \
                 moved outside the pool fails; NTP differing from site-def fails; `BF4 generic` \
                 chosen without its seed entry fails; REST unticked skips the REST rows.",
        run: todo,
    },
    Check {
        id: "config.metallb.rules",
        expected: "MetalLB pools hold every service VIP; peers match the site controllers",
        detail: "Reads: `collaterals.metallbConfig` (IPAddressPools, BGPPeers, \
                 BGPAdvertisements); `hosts.siteControllers` with `torPeers` when present, the \
                 ASN layout, `network.serviceVips` when pinned.\n\
                 Pass: every service VIP inside a pool; one BGPPeer per site controller (or per \
                 ToR with `no DPU`) with `myASN`, `peerASN` and `peerAddress` as the layout \
                 states; observed `6 VIPs in 1 pool; 3 peers match`.\n\
                 Fail: a VIP outside every pool; a peer missing, or with a wrong ASN or address; \
                 observed names the VIP or the peer and both values; fix: edit the pool or the \
                 peer.\n\
                 Skip: never.\n\
                 Test: a metallb-config fixture matching the example; cases: the fixture passes; \
                 a pool narrowed so a VIP falls outside fails naming it; one peer's `peerASN` off \
                 by one fails naming both; with `no DPU` and `torPeers` the peers are compared \
                 with the ToRs.",
        run: todo,
    },
    Check {
        id: "config.setup.command",
        expected: "--skip-* flags agree with the decisions",
        detail: "Reads: `setup.command`; the decisions `optional-components`, `dpf`, `rms`, \
                 `observability`; `collaterals.coreValues` and `metallbConfig`.\n\
                 Pass: every `--skip-*` and `--with-*` flag agrees with the decisions \
                 (`--skip-rms` with RMS unticked, no `--skip-dpf` with `dpf: Install`); \
                 `--core-values` and `--metallb-config` name the collaterals; observed the flags \
                 found.\n\
                 Fail: a flag contradicting a decision, or missing for an unticked component; a \
                 values flag naming another file; observed names the flag and the decision; fix: \
                 edit the command or the decision.\n\
                 Skip: never.\n\
                 Test: `setup.command` spliced in the example; cases: the example passes; \
                 `--skip-rms` removed with RMS unticked fails; `--skip-dpf` added with `Install` \
                 fails; `--core-values other.yaml` fails naming both paths.",
        run: todo,
    },
    Check {
        id: "config.setup.env",
        expected: "required NICO_* variables set for the ticked components; image tags equal versions",
        detail: "Reads: `setup.env`; `versions`; the decisions `dpf`, `registry-authentication`, \
                 `rms`, `observability`; the TOML's `[dpf] helm_repo_url`.\n\
                 Pass: every `NICO_*` variable the ticked components need present by name (a `{ \
                 from: ... }` source counts as present); `NICO_CORE_IMAGE_TAG` equals \
                 `versions.infraController`, `NICO_REST_IMAGE_TAG` equals `versions.nicoRest`; \
                 `NICO_DPF_HELM_REPO_CARBIDE` equals the TOML's `helm_repo_url`; observed `12 \
                 variables; tags equal versions`.\n\
                 Fail: a required variable absent, a tag differing from `versions`, the DPF \
                 repository differing from the TOML; observed names the variable and both values; \
                 fix: set it.\n\
                 Skip: the DPF variables when `dpf` is `Skip`.\n\
                 Test: `setup.env` spliced; cases: the example passes; `NICO_CORE_IMAGE_TAG` \
                 changed fails naming both tags; `NICO_DPF_DPU_INTERFACE` removed with `Install` \
                 fails; the same with `Skip` passes.",
        run: todo,
    },
    Check {
        id: "config.derived.decisions",
        expected: "derived decisions read from the files agree with any pinned value",
        detail: "Reads: every register entry with `stated: false`, read from its source (the \
                 setup command, `setup.env`, the TOML tables, the helm values) through the \
                 collateral loaders; the pinned values under `decisions`.\n\
                 Pass: every derived entry resolves to one option and every pinned value equals \
                 what the files say; observed `N derived, M pinned, all agree`.\n\
                 Fail: a pinned value the files contradict (`dpf: pinned Skip, setup.command says \
                 Install`), or a derived entry the files leave ambiguous; fix: drop the pin or \
                 change the file.\n\
                 Skip: never.\n\
                 Test: `decisions` spliced with a fixture directory; cases: the example passes; \
                 `dpf: Skip` pinned against the example command fails naming both; \
                 `credential-store` pinned to the value the TOML holds passes; a TOML without \
                 `[secrets]` leaves `credential-store` ambiguous and fails.",
        run: todo,
    },
];

/// Every path under `collaterals` resolves to a file, and the CP-DPU site yaml
/// is named exactly when the site controllers carry a DPU. Written first because
/// every other row of this checklist reads one of them, and as the worked example
/// of a check: look at `ctx`, return what you observed, the harness does the rest.
fn collaterals_present(ctx: &Ctx) -> eyre::Result<Outcome> {
    let c = &ctx.sd.collaterals;
    let no_dpu = ctx.decided("dpu-in-the-site-controllers", "no DPU");
    if !no_dpu && c.cp_dpu_site_yaml.is_none() {
        return Ok(Outcome::Fail {
            observed: "cpDpuSiteYaml absent while dpu-in-the-site-controllers is a DPU".to_string(),
            fix: "name the scripts/control-plane-dpu site yaml under collaterals".to_string(),
        });
    }
    let named: Vec<(&str, &std::path::Path)> = [
        ("cpDpuSiteYaml", c.cp_dpu_site_yaml.as_deref()),
        ("helmPrereqsValues", Some(c.helm_prereqs_values.as_path())),
        ("coreValues", Some(c.core_values.as_path())),
        ("restValues", Some(c.rest_values.as_path())),
        ("siteAgentValues", Some(c.site_agent_values.as_path())),
        ("metallbConfig", Some(c.metallb_config.as_path())),
        ("expectedMachines", c.expected_machines.as_deref()),
    ]
    .into_iter()
    .filter_map(|(k, p)| p.map(|p| (k, p)))
    .collect();
    let missing: Vec<String> = named
        .iter()
        .filter(|(_, p)| !ctx.resolve(p).exists())
        .map(|(k, p)| format!("{k}: {}", p.display()))
        .collect();
    if missing.is_empty() && no_dpu && c.cp_dpu_site_yaml.is_some() {
        Ok(Outcome::Warn {
            observed: "cpDpuSiteYaml named although the site controllers carry no DPU".to_string(),
            fix: "drop it, or correct dpu-in-the-site-controllers".to_string(),
        })
    } else if missing.is_empty() {
        Ok(Outcome::Pass(format!("{} files present", named.len())))
    } else {
        Ok(Outcome::Fail {
            observed: format!("missing {}", missing.join(", ")),
            fix: "fix the path in site-def.yaml or put the file there".to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::register::Register;

    #[test]
    fn a_missing_collateral_names_the_key_and_path() {
        let sd = crate::sitedef::parse(include_str!("../../examples/site-def.yaml")).unwrap();
        let dec = crate::gate::effective_decisions(&sd, Register::builtin());
        let ctx = Ctx {
            sd: &sd,
            site_def_path: std::path::Path::new("/nonexistent/site-def.yaml"),
            decisions: &dec,
            evidence: None,
        };
        match collaterals_present(&ctx).unwrap() {
            Outcome::Fail { observed, .. } => {
                assert!(observed.contains("coreValues:"), "{observed}");
            }
            other => panic!("expected a failure, got {other:?}"),
        }
    }
}

/// Addresses the TOML's `[networks.admin] reserve_first` keeps back by default,
/// gateway included, before any host gets one.
const ADMIN_RESERVED: u64 = 5;
/// Below this share of spare addresses the segment passes with a warning.
const HEADROOM_PERCENT: u64 = 20;

/// Unit-of-work sample: one row, one function, one test table. The admin
/// segment is where every managed host gets its address, so it has to hold
/// `counts.hosts` beyond the reserved block, with room to grow, and its MTU
/// cannot exceed the path MTU the fabric guarantees. Alignment and the gateway
/// being inside the prefix are the gate's rows and are not repeated here.
fn admin_segment(ctx: &Ctx) -> eyre::Result<Outcome> {
    let n = &ctx.sd.network;
    let seg = &n.segments.admin;
    let hosts = u64::from(ctx.sd.counts.hosts);
    let addresses = match seg.prefix {
        IpNet::V4(p) => 1u64 << (32 - u32::from(p.prefix_len())),
        IpNet::V6(_) => u64::MAX,
    };
    // network and broadcast, then the reserved block
    let usable = addresses.saturating_sub(2).saturating_sub(ADMIN_RESERVED);
    let mut problems = Vec::new();
    let mut warnings = Vec::new();

    if usable < hosts {
        problems.push(format!(
            "{} has {usable} usable addresses for {hosts} hosts",
            seg.prefix
        ));
    } else if usable.saturating_sub(hosts) * 100 < hosts * HEADROOM_PERCENT {
        warnings.push(format!(
            "{} leaves {} spare addresses for {hosts} hosts, under {HEADROOM_PERCENT}% headroom",
            seg.prefix,
            usable - hosts
        ));
    }
    if seg.mtu > n.path_mtu {
        problems.push(format!("mtu {} exceeds pathMtu {}", seg.mtu, n.path_mtu));
    }

    if !problems.is_empty() {
        return Ok(Outcome::Fail {
            observed: problems.join("; "),
            fix:
                "widen the admin prefix or lower counts.hosts; keep the segment MTU within pathMtu"
                    .to_string(),
        });
    }
    if !warnings.is_empty() {
        return Ok(Outcome::Warn {
            observed: warnings.join("; "),
            fix: "widen the admin prefix before the fleet grows".to_string(),
        });
    }
    Ok(Outcome::Pass(format!(
        "{}: {usable} usable for {hosts} hosts, mtu {} within pathMtu {}",
        seg.prefix, seg.mtu, n.path_mtu
    )))
}

#[cfg(test)]
mod admin_segment_tests {
    use super::*;
    use crate::register::Register;

    const EXAMPLE: &str = include_str!("../../examples/site-def.yaml");

    /// The example with one line changed, run through the check.
    fn outcome(from: &str, to: &str) -> Outcome {
        assert!(
            EXAMPLE.contains(from),
            "splice target `{from}` not in the example"
        );
        let text = EXAMPLE.replacen(from, to, 1);
        let sd = crate::sitedef::parse(&text).unwrap();
        let dec = crate::gate::effective_decisions(&sd, Register::builtin());
        let ctx = Ctx {
            sd: &sd,
            site_def_path: std::path::Path::new("examples/site-def.yaml"),
            decisions: &dec,
            evidence: None,
        };
        admin_segment(&ctx).unwrap()
    }

    #[test]
    fn admin_segment_outcomes() {
        struct Case {
            name: &'static str,
            from: &'static str,
            to: &'static str,
            want: fn(&Outcome) -> bool,
            observed_has: &'static str,
        }
        let cases = [
            Case {
                name: "the example fits: /24 for 128 hosts",
                from: "counts: { hosts: 128,",
                to: "counts: { hosts: 128,",
                want: |o| matches!(o, Outcome::Pass(_)),
                observed_has: "249 usable for 128 hosts",
            },
            Case {
                name: "too small fails naming both numbers",
                from: "counts: { hosts: 128,",
                to: "counts: { hosts: 300,",
                want: |o| matches!(o, Outcome::Fail { .. }),
                observed_has: "249 usable addresses for 300 hosts",
            },
            Case {
                name: "fits with under 20% headroom warns",
                from: "counts: { hosts: 128,",
                to: "counts: { hosts: 240,",
                want: |o| matches!(o, Outcome::Warn { .. }),
                observed_has: "9 spare addresses for 240 hosts",
            },
            Case {
                name: "segment MTU above the path MTU fails",
                from: "pathMtu: 9000",
                to: "pathMtu: 1500",
                want: |o| matches!(o, Outcome::Fail { .. }),
                observed_has: "mtu 9000 exceeds pathMtu 1500",
            },
        ];
        for c in cases {
            let o = outcome(c.from, c.to);
            assert!((c.want)(&o), "{}: got {o:?}", c.name);
            let observed = match &o {
                Outcome::Pass(s) | Outcome::Skip(s) => s.as_str(),
                Outcome::Warn { observed, .. } | Outcome::Fail { observed, .. } => {
                    observed.as_str()
                }
                Outcome::Todo => "",
            };
            assert!(
                observed.contains(c.observed_has),
                "{}: observed `{observed}`",
                c.name
            );
        }
    }
}
