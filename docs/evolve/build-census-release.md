# Build census release checklist

Implementation ownership and privacy boundaries are fixed by
[ADR-0088](/decisions/0088-build-census-is-compiler-owned). The compiler currently permits
local preview and hard-disables public reporting. The collector has ingestion and statistics materialisation disabled,
no production route, and a placeholder database binding. These are release gates, not
user configuration switches.

## Before live reporting

1. Publish the separate census repository and protocol; name collector and privacy-request owners.
2. Approve lawful basis, terminal-storage/ePrivacy treatment, processors/transfers, retention,
   provider IP/security logs, access restrictions, and deletion/restore procedures.
3. Provision the dedicated Worker/D1/rate limiter; inspect account ownership and route isolation.
4. Apply migrations and verify ingestion, quotas, expiry, suppression, and restore cleanup using
   synthetic events. Publish schema, selection rules, retention, and operator contact information.
5. Enable an explicitly agreed pilot through the final client path. Measure retained identity,
   short-lived javac delivery loss, and synchronous overhead against representative applications.
6. Release default-on only after those approvals, changing the reviewed compiler release gate
   and the collector deployment configuration together. Keep opt-out and local preview documented.
7. Publish statistics after a complete calendar month and sufficient contributing installations.

## Compatibility and follow-up

Keep v1 available when introducing another schema. Do not reinterpret fields or remotely
expand collection. Catalogue/provider/representation/Block IDs require a separately reviewed
allowlist and schema version; private IDs collapse to `other`. Execution hints and compiler-owned
persistence/cache declarations may follow when authoritative. Compensation and runtime-selected
QUEUE_ASYNC remain excluded. The website must consume only public aggregate releases.

The collector runbook owns retention (including provider recovery history), deletion requests,
restore reapplication, rate-key rotation, access review, and privacy incident handling.
Record pilot measurements and legal/operator approvals there before removing release gates.


## Preview-stage verification

The review targets the `26.11.2-SNAPSHOT` [compiler candidate](https://github.com/The-Pipeline-Framework/pipelineframework-compiler/pull/54)
(`550e7f6`), [contracts metadata candidate](https://github.com/The-Pipeline-Framework/pipelineframework-contracts/pull/58)
(`fb6d7d46e8d9`), and [collector candidate](https://github.com/The-Pipeline-Framework/pipelineframework-census/pull/1)
(`145b54754ca3`). The protocol copies compare byte for byte, and the collector accepts a Java-produced fixture.

| Obligation | Evidence and limit |
| --- | --- |
| Successful logical application only | `CensusCompilationTest` runs the production processor, distinguishes application, service and invalid compilations, and inspects custom packaged output. The CLI consumes releases and has no build observation path. |
| Coarse logical counts and excluded data | `CensusAcceptanceTest` parses V3, imports real Block packages, links reused and recursive definitions, tests canonical type and feature bands, and verifies invariance under layouts, generated role copies and renamed private values. This is logical-model evidence, not a live deployment across every layout. |
| Retained identity and deduplication | State tests and two separate compiler JVMs demonstrate identity retention, competing first use, weekly UTC renewal, change detection and daily attempt bounds. Version reads are constrained to the owning artefact. |
| Build failure isolation and local inspection | Queue pressure, locked/corrupt state, daemon exit, preview suppression, redirects, local TLS handshake failure, response timeouts and rejection responses are covered with local fixtures. Synchronous projection/handoff p95 was 4.9 ms on the 501-location fixture during full verification. |
| Strict ingestion, retention and privacy | Twelve collector tests cover strict schema, immutable deduplication, quotas, weekly/monthly unions, suppression and expiry despite aggregation failure. The generated deletion SQL runs against local D1. No provider deployment, live restore or edge-log claim follows from these tests. |
| Cross-repository compatibility | Local compiler verification passed 1,050 tests and the contracts reactor verified. Require the coordinated `tpf/system-tests` result for the immutable candidate heads. The docs test suite passed 36 tests and the site build passed locally. Docs CI checks the published candidate. |

Independent compiler and collector review identified and exercised failure cases beyond the original
fixtures. GitNexus mapped the shared compilation-context impact against the actual worktree;
source review and tests cover callback and field flows that its taint model cannot prove. The
collector has no GitNexus index and was reviewed against its source and local D1 behaviour.

This evidence supports local preview. It does not establish delivery coverage for short-lived hosts,
real DNS/TLS failure behaviour on all build hosts, alternate production frontend parity, or
provider privacy controls. Confirm those applicable production conditions during the gated pilot.
