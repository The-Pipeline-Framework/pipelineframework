# Self-Hosted Milestone

The near-term adoption goal is a public, self-hosted durable coordinator path that users can run and inspect without private code.

The first reference is `examples/restaurant-approval/self-host`, which runs one batteries-included coordinator process from the packaged `monolith-svc` artifact. It uses the local in-process transition worker by default so users can exercise hosted-style submission, bundle activation, await completion, and result inspection without starting a second service.

## Current Proof

| Capability | Status |
| --- | --- |
| Batteries-included local coordinator demo | present in `restaurant-approval` |
| Coordinator and worker split-process proof | present in `RestaurantApprovalHostedCoordinatorRestWorkerIT` |
| Bundle registration and activation | present |
| Execution pinning to active bundle version | present |
| Worker availability check before submit | present |
| Await pending query and completion | present |
| Accepted/declined terminal results | present |
| Failure/DLQ incident walkthrough | present in `restaurant-approval/self-host` |
| Operator walkthrough | present in `restaurant-approval/self-host` |
| Production-ish deployment recipe | present in [Self-Hosted Deployment](/guide/evolve/durable-coordinator/self-hosted-deployment) |
| Kafka await over stream | covered separately by `csv-payments` |

## Remaining Gap

| Gap | Why it matters |
| --- | --- |
| Built-in DLQ replay | current self-host incident flow shows status, terminal error details, and DLQ publication, but re-drive remains application-owned |
| Contract/bundle correction | the long-term concept is a pipeline contract, with executable artifacts as one implementation form |
| Worker lifecycle | healthy, stale, draining, and unavailable states should be added only when self-host workflows require them |

Until this milestone is credible, durable coordinator semantics, worker protocols, bundle/contract identity, and function-platform parity primitives should remain public and self-hostable.
