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
| Operator walkthrough | present in `restaurant-approval/self-host` |
| Production-ish deployment recipe | present in [Self-Hosted Deployment](/guide/evolve/durable-coordinator/self-hosted-deployment) |
| Durable release metadata | Dynamo registry with immutable release records and append-only activation events |
| Kafka await over stream | covered separately by `csv-payments` |

## Remaining Gap

| Gap | Why it matters |
| --- | --- |
| Built-in DLQ replay | current self-host incident flow shows status, terminal error details, and DLQ publication, but re-drive remains application-owned |
| Shared/replicated artifact storage | Dynamo release metadata is durable, but local artifact storage remains deployment-owned |
| Append-only execution/await stores | existing Dynamo execution and await stores still use conditional updates for leases and state transitions |
| Worker lifecycle | healthy, stale, draining, and unavailable states should be added only when self-host workflows require them |

The `COMPUTE` self-host path is now credible for local adoption and operator walkthroughs. The main remaining HA gap is not the release metadata model; it is making the rest of the coordinator state, worker lifecycle, and artifact availability operationally credible across multiple coordinator instances.
