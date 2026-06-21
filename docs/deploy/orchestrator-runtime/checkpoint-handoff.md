# Checkpoint Handoff

Reliable cross-pipeline handoff is orchestrator-owned and checkpoint-based.

Supported in this release:

1. source: final pipeline checkpoint publication from a `QUEUE_ASYNC` orchestrator,
2. target: downstream orchestrator background admission bound by a named logical publication,
3. idempotency: preserve incoming dispatch identifiers when present, otherwise derive a repeat-safe handoff key from configured checkpoint fields,
4. ownership: downstream retry/DLQ remains orchestrator-owned after background admission.
5. broker substrate: Kafka publication/subscription through framework-owned handoff envelopes when configured at runtime.

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
6. Kafka checkpoint handoff uses a strict JSON envelope over a configured publication topic and routes into the same subscriber admission service.
7. Reliable handoff is supported only for `QUEUE_ASYNC` orchestrators and is rejected for `FUNCTION` pipelines.
8. Live `Subscribe` remains a weaker observer/tap API and is not the reliable checkpoint handoff path.

## Kafka Handoff Targets

Configure a Kafka checkpoint publication target with the same logical publication binding shape:

```properties
pipeline.handoff.bindings."checkout.orders.ready.v1".targets.next.kind=KAFKA
pipeline.handoff.bindings."checkout.orders.ready.v1".targets.next.topic=checkout.orders.ready.v1
```

Enable the Kafka checkpoint publisher on the source orchestrator:

```properties
tpf.checkpoint.kafka.publisher.enabled=true
kafka.bootstrap.servers=${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
mp.messaging.outgoing.tpf-checkpoint-kafka-publications.connector=smallrye-kafka
mp.messaging.outgoing.tpf-checkpoint-kafka-publications.value.serializer=org.apache.kafka.common.serialization.StringSerializer
```

Enable the Kafka checkpoint consumer on the subscriber orchestrator:

```properties
tpf.checkpoint.kafka.consumer.enabled=true
kafka.bootstrap.servers=${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
mp.messaging.incoming.tpf-checkpoint-kafka-publications.connector=smallrye-kafka
mp.messaging.incoming.tpf-checkpoint-kafka-publications.topic=checkout.orders.ready.v1
mp.messaging.incoming.tpf-checkpoint-kafka-publications.group.id=checkout-orders-ready-subscriber
mp.messaging.incoming.tpf-checkpoint-kafka-publications.value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
```

The Kafka record carries TPF-owned control metadata plus the checkpoint payload. Kafka offsets remain broker delivery cursors, not TPF replay state.
