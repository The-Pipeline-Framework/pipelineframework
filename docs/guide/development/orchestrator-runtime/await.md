# Await Boundaries

Await steps suspend `QUEUE_ASYNC` execution at an external boundary. TPF persists the interaction, dispatches through the configured adapter, and resumes the owning execution after a correlated completion is admitted.

Internally, await is backed by durable await units. For implementation diagrams and the state model, see [Await Unit Runtime](/guide/evolve/await-unit-runtime/).

## Supported Shapes

| Cardinality | Interaction unit | Replay shape | App guidance |
| --- | --- | --- | --- |
| `ONE_TO_ONE` | one input unit, one external interaction | one output unit | Use for human approval, webhook callback, or brokered request/reply that returns one result. |
| `ONE_TO_ONE` over a stream | one owning unit with one item interaction per input item | completed item outputs replayed in input order | Use when each stream item has its own external decision. |
| `ONE_TO_MANY` | one input unit, one external interaction | one materialized multi-item output unit replayed as a stream | Keep completion payloads bounded. |
| `MANY_TO_ONE` | one materialized input unit, one external interaction | one output unit | Use when the external system decides on the whole batch. |
| `MANY_TO_MANY` | one materialized input unit, one external interaction | one materialized multi-item output unit replayed as a stream | Keep input and completion payloads bounded. |

`csv-payments` uses authored `ONE_TO_ONE` await over a stream of `PaymentRecord` items. That is a stream of unary await interactions, not a hidden dispatch mode.

The built-in `interaction-api` adapter is for human/UI inboxes and mock-provider style flows where another client queries pending interactions and later calls the generated completion API. The built-in `webhook` adapter dispatches an HTTP request to an external system and includes a signed resume token in the envelope. The built-in `kafka` adapter publishes a request envelope to Kafka and admits response envelopes from a configured response channel.

For runnable examples, use [`examples/restaurant-approval`](https://github.com/The-Pipeline-Framework/pipelineframework/tree/main/examples/restaurant-approval) for human/UI await and [`examples/csv-payments`](https://github.com/The-Pipeline-Framework/pipelineframework/tree/main/examples/csv-payments) for Kafka unary await over a stream.

## Await Versus Operator

Await is the only durable suspend/resume primitive in TPF. Use operators or remote execution when the external system replies within the current invocation. Use `kind: await` when the request leaves the current execution turn and the result is admitted later through correlation.

| External shape | Use |
| --- | --- |
| Inline HTTP/gRPC call returning now | Operator / remote execution |
| Broker request/reply with later correlated message | Await step |
| Webhook callback later | Await step |
| UI/human approval | Await step |

If a remote system returns `accepted` now and the final business result arrives later, do not model that as a remote operator. That is an await boundary.

## Await Versus Checkpoint Handoff

Await and checkpoint handoff both cross a process boundary, but they have different ownership.

| Concern | Await | Checkpoint handoff |
| --- | --- | --- |
| Execution ownership | one execution parks and later resumes | one pipeline publishes and another pipeline admits independent work |
| Boundary | mid-pipeline external wait | terminal or named publication boundary |
| Completion | correlated interaction completion | downstream checkpoint admission |
| Retry and DLQ | owning execution remains responsible | downstream orchestrator owns retry and DLQ after admission |
| Use when | the external result belongs to the same business flow | another pipeline should own the next lifecycle |

Use await for human approvals, webhook callbacks, and brokered provider decisions that must resume the same execution. Use checkpoint handoff when the receiving workflow has separate ownership, scaling, or operational responsibility.

## Application Design Notes

Await has the same side-effect rule as the rest of `QUEUE_ASYNC`: orchestrator state transitions are guarded, but external dispatch and external side effects are at-least-once. Use stable business idempotency keys at the external boundary.

Aggregate await shapes materialize input and/or output units in the current runtime. Do not use unbounded payloads for `ONE_TO_MANY`, `MANY_TO_ONE`, or `MANY_TO_MANY` await boundaries. If replay of a materialized multi-item output fails halfway through downstream execution, TPF restarts that output unit as a whole; it does not claim exactly-once partial stream progress inside the unit.

The runtime also enforces aggregate materialization guardrails:

| Config key | Default | Applies to |
| --- | --- | --- |
| `pipeline.orchestrator.await-aggregate-max-input-items` | `10000` | materialized input units for `MANY_TO_ONE` and `MANY_TO_MANY` await steps |
| `pipeline.orchestrator.await-aggregate-max-output-items` | `10000` | materialized output units for `ONE_TO_MANY` and `MANY_TO_MANY` await steps |

Set either value to `0` only when the application has its own upstream size control and storage budget. Prefer stable business limits at the API/file/broker boundary rather than relying on these guards as the first line of defense.

Transport choice changes operational responsibility. `interaction-api` requires an API consumer to query and complete pending work. `webhook` requires stable resume-token signing and callback reachability. `kafka` requires broker channel configuration, consumer health, and response-envelope monitoring.

That matters for plugin-style side effects after an await boundary. A resumed queue-async execution can replay the remainder of the pipeline after a downstream retry, so once-only side-effect checkpointing is a separate concern from await durability itself.

## Webhook Example

```yaml
steps:
  - name: "Fraud Check"
    kind: "await"
    cardinality: "ONE_TO_ONE"
    input: "com.example.FraudCheckRequest"
    output: "com.example.FraudCheckDecision"
    timeout: "PT10M"
    idempotencyKeyFields: ["orderId"]
    await:
      correlation:
        strategy: "signedResumeToken"
      transport:
        type: "webhook"
        request:
          url: "https://partner.example/fraud-check"
        callback:
          baseUrl: "https://orchestrator.example"
```

Webhook dispatch sends an envelope containing the interaction id, correlation id, resume token, deadline, request payload, tenant id, step id, output type, and callback metadata when configured. Completion is submitted through the generated REST/gRPC completion APIs; TPF validates the token before accepting the response snapshot.

## Kafka Example

```yaml
steps:
  - name: "Brokered Fraud Check"
    kind: "await"
    cardinality: "ONE_TO_ONE"
    input: "com.example.FraudCheckRequest"
    output: "com.example.FraudCheckDecision"
    timeout: "PT10M"
    idempotencyKeyFields: ["orderId"]
    await:
      correlation:
        strategy: "signedResumeToken"
      transport:
        type: "kafka"
        request:
          topic: "fraud-check.requests"
          key: "correlationId" # optional: interactionId or correlationId
        response:
          topic: "fraud-check.decisions"
        consumer:
          group: "fraud-check-orchestrator" # optional; channel config remains authoritative
        headers:
          x-source: "tpf"
```

Kafka dispatch sends a framework-owned JSON envelope containing tenant id, execution id, interaction id, correlation id, step id, deadline, input/output types, resume token, request payload, and dispatch metadata. The response envelope is consumed by TPF and completed directly through `AwaitCoordinator`, not by looping back through REST. Use the generated REST/gRPC completion APIs for human/UI or webhook clients that are not broker consumers.

```yaml
steps:
  - name: "Await Payment Provider"
    kind: "await"
    cardinality: "ONE_TO_ONE"
    input: "org.pipelineframework.csv.common.domain.PaymentRecord"
    output: "org.pipelineframework.csv.common.domain.PaymentStatus"
    timeout: "PT5M"
    idempotencyKeyFields: ["csvId", "recipient", "amount", "currency"]
    await:
      correlation:
        strategy: "signedResumeToken"
      transport:
        type: "kafka"
        request:
          topic: "csv-payments.payment.requests"
          key: "correlationId"
        response:
          topic: "csv-payments.payment.results"
```

Add the Quarkus Kafka messaging extension to the application that hosts the orchestrator, enable the default Kafka bridge with `pipeline.await.kafka.reactive-messaging.enabled=true`, then configure the SmallRye channels:

```properties
mp.messaging.outgoing.tpf-await-kafka-requests.connector=smallrye-kafka
mp.messaging.outgoing.tpf-await-kafka-requests.value.serializer=org.apache.kafka.common.serialization.StringSerializer
mp.messaging.incoming.tpf-await-kafka-responses.connector=smallrye-kafka
mp.messaging.incoming.tpf-await-kafka-responses.topic=csv-payments.payment.results
mp.messaging.incoming.tpf-await-kafka-responses.value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
```

`pipeline.orchestrator.resume-token-secret` must be stable for the lifetime of outstanding webhook and Kafka interactions. If `pipeline.orchestrator.resume-token-secret` is missing, signed dispatch and token validation fail with a clear error rather than allowing insecure or unsigned resumptions.
