# Self-Hosted HA Roadmap

This page records the closeout state for the compute-first self-host HA milestone and separates adoption-ready capability from deferred hardening work.

## Milestone Complete

The self-host HA path is credible for adoption and demos. The restaurant and CSV container references prove the durable coordinator model locally:

1. coordinator and REST worker run as separate containers,
2. LocalStack provides DynamoDB-, SQS-, and S3-compatible endpoints,
3. release registration, activation, and worker lifecycle are exercised,
4. happy-path await completion and terminal result inspection are automated,
5. incident handling reaches terminal failure and demonstrates single-execution re-drive,
6. restaurant recovery proof restarts coordinator and worker processes while preserving parked await state,
7. CSV Payments proves stream-await execution with both SQS and Kafka await-provider lanes.

The milestone is intentionally compute-first. `FUNCTION` remains serverless invocation/adapter support; it is not the current TPF-owned durable HA path.

## Deferred Hardening

The following work is useful but not required for the current self-host HA adoption milestone:

| Hardening item | Tracking | Notes |
| --- | --- | --- |
| Append-only execution and await state storage | [#396](https://github.com/The-Pipeline-Framework/pipelineframework/issues/396) | Existing Dynamo execution and await stores still use conditional updates for leases and state transitions. |
| Bulk DLQ replay campaigns | [#406](https://github.com/The-Pipeline-Framework/pipelineframework/issues/406) | Single-execution re-drive exists; batch selection, rate limits, audit, and poison-record handling remain separate work. |
| Mid-transition crash/lease-takeover campaign | none yet | The current restart proof uses a deterministic await boundary. In-flight crash campaigns need failure injection and side-effect idempotency assertions. |
| Kubernetes/Helm/Terraform/IAM packaging | none yet | Deployment packaging belongs after the adoption path proves demand. |
| Worker autoscaling or fleet routing | none yet | The current lifecycle gate is deliberately minimal: healthy, stale, draining, unavailable. |
| Production tenancy/RBAC/support console | none yet | This is managed-product work, not required for OSS self-host adoption. |

## Storage Direction

New coordinator metadata stores should keep following the immutable-record rule: conditional writes, immutable records, and append-only event records where practical.

Existing execution and await Dynamo stores still use conditional updates for leases and state transitions. Moving those stores to an append-only model is issue [#396](https://github.com/The-Pipeline-Framework/pipelineframework/issues/396) because it changes read paths, write volume, retention, and recovery semantics.

## Replay Direction

Single-execution re-drive is present and reads the durable execution record. DLQ messages are operational evidence, not the replay source.

Bulk replay remains issue [#406](https://github.com/The-Pipeline-Framework/pipelineframework/issues/406). It needs operator controls for selection, rate limits, idempotency warnings, and audit trails before it belongs in the runtime.
