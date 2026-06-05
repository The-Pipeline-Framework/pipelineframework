# Self-Hosted Milestone

The near-term adoption goal is a public, self-hosted durable coordinator path that users can run and inspect without private code.

The first reference is `examples/restaurant-approval/self-host`, which runs one coordinator process and one REST worker process from the same packaged monolith.

## Current Proof

| Capability | Status |
| --- | --- |
| Coordinator and worker run as separate processes | present in `restaurant-approval` |
| Bundle registration and activation | present |
| Execution pinning to active bundle version | present |
| Worker availability check before submit | present |
| Await pending query and completion | present |
| Accepted/declined terminal results | present |
| Kafka await over stream | covered separately by `csv-payments` |

## Remaining Gap

| Gap | Why it matters |
| --- | --- |
| Operator walkthrough | users need a step-by-step runbook for status, await, result, logs, replay, and DLQ |
| Replay/DLQ incident demo | durable orchestration needs visible recovery paths, not only happy paths |
| Production-ish deployment recipe | self-host users need coordinator, worker, store, queue, secret, and artifact layout guidance |
| Contract/bundle correction | the long-term concept is a pipeline contract, with executable artifacts as one implementation form |
| Worker lifecycle | healthy, stale, draining, and unavailable states should be added only when self-host workflows require them |

Until this milestone is credible, durable coordinator semantics, worker protocols, bundle/contract identity, and function-platform parity primitives should remain public and self-hostable.
