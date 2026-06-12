# Self-Hosted Milestone

The near-term adoption goal is a self-hosted durable coordinator path that users can run, inspect, and operate directly.

The first reference is `examples/restaurant-approval/self-host`, which runs one batteries-included coordinator process from the packaged `monolith-svc` artifact. It uses the local in-process transition worker by default so users can exercise hosted-style submission, release activation, await completion, and result inspection without starting a second service.

## Current Proof

| Capability | Status |
| --- | --- |
| Batteries-included local coordinator demo | present in `restaurant-approval` |
| Coordinator and worker split-process proof | present in `RestaurantApprovalHostedCoordinatorRestWorkerIT` |
| Release registration and activation | present |
| Execution pinning to active release version | present |
| Worker availability check before submit | present |
| Await pending query and completion | present |
| Accepted/declined terminal results | present |
| Failure/DLQ incident walkthrough | present in `restaurant-approval/self-host` |
| Single-execution operator re-drive | present for terminal `DLQ` and explicit `FAILED` executions |
| Operator walkthrough | present in `restaurant-approval/self-host` |
| Production-ish deployment recipe | present in [Self-Hosted Deployment](/guide/evolve/durable-coordinator/self-hosted-deployment) |
| Durable release metadata | Dynamo registry with immutable release records and append-only activation events |
| Shared/replicated artifact storage | local filesystem or S3-compatible blob store for coordinator-managed artifacts; OCI/Maven-style repositories remain preferred for native artifact forms |
| Minimal worker lifecycle | registration, heartbeat, drain, stale detection, and submit-time healthy-worker gate |
| Kafka await over stream | covered separately by `csv-payments` |

## Remaining Gap

| Gap | Why it matters |
| --- | --- |
| Bulk DLQ replay campaigns | single-execution re-drive exists, but there is no DLQ-message consumer or batch replay surface |
| Append-only execution/await stores | existing Dynamo execution and await stores still use conditional updates for leases and state transitions |

The `COMPUTE` self-host path is now credible for local adoption and operator walkthroughs. The main remaining HA gap is not the release metadata model, the minimum worker lifecycle gate, or single-execution re-drive; it is making execution/await state fully append-only and deciding whether bulk replay belongs in the framework or in operator/application runbooks.
