# Dispatch Substrates

Dispatch substrate is the mechanism used to move a TPF command, payload, or envelope across a boundary.

It is not the same decision as transport mode, platform mode, runtime layout, or payload policy.

## Substrate Matrix

| Substrate | Best fit | Caution |
| --- | --- | --- |
| Local | tests, demos, monoliths, low-friction adoption | no crash-surviving handoff by itself |
| REST | simple request/response step hosts and transition workers | immediate call semantics; not durable buffering |
| gRPC | efficient typed request/response and protobuf contracts | still immediate unless paired with await/checkpoint semantics |
| SQS | AWS-shaped durable coordinator dispatch, await request/completion queues, DLQ/re-drive, and queue-style self-host HA | standard queues only for SQS await v1; weaker ordering/grouping model than Kafka |
| Kafka | enterprise broker backbone, Kafka await, stream pressure boundaries, topic fan-out, retained event streams, and Kafka-native estates | do not treat offsets as TPF replay semantics |

## Not `transport: KAFKA` Or `transport: SQS`

Do not flatten Kafka or SQS into the same top-level decision as REST or gRPC.

REST and gRPC are immediate call transports. Kafka and SQS are brokered dispatch substrates with different timing, correlation, retry, and ownership implications.

Prefer boundary-specific configuration shapes:

```yaml
kind: await
await:
  transport:
    type: kafka
```

```yaml
kind: await
await:
  transport:
    type: sqs
```

```yaml
checkpoint:
  publish:
    target:
      kind: kafka
```

The checkpoint snippet is illustrative. Supported runtime configuration uses `pipeline.handoff.bindings.<publication>.targets.<target>.*` with `kind=KAFKA` and `topic=<topic>`.

```properties
pipeline.orchestrator.dispatcher-provider=sqs
```

Kafka and SQS await are supported runtime adapters. Kafka checkpoint publication/subscription is supported as a broker-backed handoff provider. `pipeline.orchestrator.dispatcher-provider=sqs` is supported for queue-async work dispatch. Kafka dispatcher-provider examples remain design direction, not committed public API.

## Relationship To Existing Work

This guide extends existing boundary seams:

- [Step-Aware Invocation Runtime](/evolve/durable-coordinator/boundary-invocation-model) defines shared invocation points for step, transition-worker, and transport-boundary execution.
- [Worker Protocols](/evolve/durable-coordinator/worker-protocols) already model transition-worker command/result envelopes over local, REST, gRPC, and SQS.
- [Await Unit Runtime](/evolve/await-unit-runtime/) defines durable suspend/resume semantics that can use different transports.
- [Immutable Segment And Boundary Model](/evolve/await-unit-runtime/immutable-boundaries) defines the internal fact model shared by await completion and checkpoint handoff admissions.
- [Checkpoint Handoff](/deploy/orchestrator-runtime/checkpoint-handoff) is the natural user-facing shape for pipeline-to-pipeline publication.
- [Runtime Core Decoupling](/evolve/runtime-core-decoupling) keeps runtime-adapter concerns out of core semantics.

Broker-backed await providers should extend these seams. They should not introduce an independent workflow engine inside TPF.

## Pressure And Replay

```mermaid
sequenceDiagram
    participant TPF as "TPF boundary"
    participant Broker as "Broker substrate"
    participant Host as "Step host"
    participant Store as "TPF state/cache/persistence"

    TPF->>Broker: publish envelope with TPF lineage
    Broker-->>TPF: lag/pressure signal
    Host->>Broker: consume by boundary key
    Host-->>TPF: result envelope / completion
    TPF->>Store: commit replay-aware state
```

Broker retention can help recover delivery. TPF replay still needs step identity, contract version, item lineage, fan-in completion, and cache/persistence policy.
