---
search: false
---

# Checkpoint Handoff

Reliable cross-pipeline handoff is orchestrator-owned and checkpoint-based.

Supported in this release:

1. source: final pipeline checkpoint publication from a `QUEUE_ASYNC` orchestrator,
2. target: downstream orchestrator background admission bound by a named logical publication,
3. idempotency: preserve incoming dispatch identifiers when present, otherwise derive a repeat-safe handoff key from configured checkpoint fields,
4. ownership: downstream retry/DLQ remains orchestrator-owned after background admission.

Declare reliable handoff in `pipeline.yaml`:

```yaml
input:
  subscription:
    publication: "checkout.orders.ready.v1"
    mapper: "com.example.pipeline.mapper.ReadyOrderMapper"

output:
  checkpoint:
    publication: "checkout.orders.dispatched.v1"
    idempotencyKeyFields: ["orderId", "customerId", "readyAt"]
```

## Runtime Behaviour

1. Build-time validation checks checkpoint boundary declarations and mapper compatibility.
2. Runtime endpoint bindings come from `pipeline.handoff.bindings.<publication>.targets.*`.
3. Publication is generated into existing orchestrator ownership; no separate connector runtime or deployment role is introduced.
4. Subscriber admission is handled by framework-owned HTTP and gRPC checkpoint publication endpoints instead of runtime subscription discovery.
5. Protobuf-over-HTTP and gRPC use the same framework-owned checkpoint protobuf envelope for transport-native admission.
6. Reliable handoff is supported only for `QUEUE_ASYNC` orchestrators and is rejected for `FUNCTION` pipelines.
7. Live `Subscribe` remains a weaker observer/tap API and is not the reliable checkpoint handoff path.

## Not Yet Supported

1. generic broker-message re-drive,
2. a separate durable checkpoint-publication service or publication-specific DLQ,
3. dynamic runtime publication discovery or runtime subscription registration,
4. broker-backed publication targets such as `SQS` or `KAFKA`.
