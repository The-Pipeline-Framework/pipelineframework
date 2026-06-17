---
search: false
---

# Adoption And Slices

This page captures the product value and implementation ordering for brokered boundary work.

## Value Proposition

Brokered boundaries are attractive because many organisations already trust brokers operationally. They have teams, dashboards, retention policies, queue policies, ACLs, incident playbooks, and scaling patterns around Kafka, SQS, or both.

The TPF value proposition is not "you no longer need brokers." It is:

- Application teams do not have to encode pipeline semantics as topic naming, offset handling, Redis key conventions, and sidecar logic.
- Platform teams can choose Kafka, SQS, REST, gRPC, or local substrates per boundary.
- TPF keeps the execution model explicit: authored steps, declared DTO/envelope contracts, deterministic lineage, replay topology, and generated/runtime validation.
- Self-hosted TPF deployments can keep backing broker/cache choices explicit while preserving one TPF model for application code.

This lowers adoption risk for enterprise users without weakening the core TPF model.

Kafka/MSK is the stronger fit when the platform already needs a Kafka estate: topic fan-out, retained streams, partition-aware throughput, Kafka-native consumers, and stream observability. SQS is the simpler AWS-shaped fit for queue/request-reply/DLQ-oriented workloads, especially when the goal is an await request/completion queue rather than a retained event stream.

## Adoption Ramp

```mermaid
flowchart LR
    Loose["Envelope payload<br/>fast onboarding"]
    Hints["Envelope + schema hints<br/>declared drift boundary"]
    Proto["Generated protobuf<br/>polyglot typed contract"]
    DTO["Typed DTO + mapper validation<br/>strongest TPF path"]

    Loose --> Hints --> Proto --> DTO
```

Platform teams can start with a loose envelope where adoption matters, then move stable domains toward generated protobuf or typed DTO contracts.

## Candidate Slices

Prefer these implementation slices if this work becomes active:

1. SQS-backed await transport for AWS-shaped self-host HA. Implemented by the CSV container reference.
2. Kafka await provider for event-stream await proofs. Implemented by the default CSV topology.
3. Kafka-backed checkpoint publication/subscription.
4. Kafka-backed transition-worker dispatcher using the existing command/result envelope.
5. Protobuf-backed external step-host contract generation for non-Java implementations.
6. Optional envelope compatibility lane with strict TPF control metadata and loose payload.
7. Brokered step-host dispatch only after the earlier boundary types are proven.

Avoid starting with broad "Kafka transport" or "SQS transport" PRs. They mix unrelated risks and blur TPF semantics.

## Non-Goals

Envelope and brokered boundary work should not:

1. replace typed TPF as the default,
2. make mapper pair validation irrelevant for typed paths,
3. hide drift by treating all payloads as arbitrary objects,
4. turn Kafka offsets, SQS visibility timeouts, or broker redelivery into TPF replay state,
5. force users to operate Kafka when local, REST, gRPC, or SQS runtime boundaries are enough,
6. introduce a second workflow engine inside TPF.

## Guardrails

Preserve these invariants:

1. Typed TPF remains the default path.
2. Envelope compatibility is explicit and weaker than typed DTO mode.
3. Dispatch policy is independent from payload policy.
4. Broker-backed dispatch does not move core validation from build time to runtime when typed contracts are available.
5. Kafka, SQS, REST, gRPC, and local substrates are implementation choices under TPF-owned boundaries.
6. Replay viewer and telemetry interpret TPF events, not broker offsets.
7. Runtime layout and build topology remain distinct from broker topology.
