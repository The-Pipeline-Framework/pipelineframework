# Await Boundaries

Await boundaries model external reality inside a typed pipeline without turning the external actor into a pipeline step. Use `kind: await` when the business flow must pause, wait for a correlated completion, and then resume the same execution with an explicit output type.

Typical awaits include:

- human approvals,
- webhook callbacks,
- provider decisions,
- brokered request/reply over Kafka or SQS,
- long-running jobs that return a business result later.

The important design choice is ownership. The pipeline still owns the business flow and the continuation. The external actor owns the real-world decision or effect.

## When To Use Await

Use await when the request leaves the current execution turn and the final business result arrives later.

| External shape | Model as |
| --- | --- |
| Inline HTTP/gRPC call returning now | Operator or remote execution |
| Provider accepts now and decides later | Await boundary |
| Broker request with later correlated response | Await boundary |
| Webhook callback later | Await boundary |
| UI or human approval | Await boundary |

If a remote system returns `accepted` now and the final decision comes back later, do not model that as a remote operator. Model the later result as an await completion.

## Shape The Contract

Await is still a typed step. The request and completion should be ordinary business types, not loose transport envelopes.

```yaml
steps:
  - name: "Fraud Check"
    kind: "await"
    cardinality: "ONE_TO_ONE"
    input: "com.example.FraudCheckRequest"
    output: "com.example.FraudCheckDecision"
    timeout: "PT10M"
    idempotencyKeyFields: ["orderId"]
```

The input type is what the pipeline sends to external reality. The output type is what the pipeline expects before it can continue. TPF handles the interaction identity, correlation, persistence, replay, and transport adapter around that contract.

## Cardinality Shapes

Cardinality defines what the pipeline is waiting for and what must be replayable after completion.

| Cardinality | Design meaning | Use when |
| --- | --- | --- |
| `ONE_TO_ONE` | one request produces one completion | a single approval, callback, or provider decision |
| `ONE_TO_ONE` over a stream | each item gets its own external decision | each input row, payment, or document needs an independent completion |
| `ONE_TO_MANY` | one request produces a bounded set of output items | an external job expands one request into several typed results |
| `MANY_TO_ONE` | a bounded batch produces one completion | the external system decides on the whole batch |
| `MANY_TO_MANY` | a bounded batch produces a bounded result set | the external system transforms a batch into another batch |

Keep aggregate await payloads bounded. If the design needs unbounded streaming, split the flow into smaller await boundaries or hand off to another pipeline with its own lifecycle.

## Flow Across Await

Await separates a pipeline into live reactive segments and a durable external wait.

Inside a live segment, normal reactive demand and backpressure can apply between adjacent steps. A streaming input step can slow down when the downstream step cannot accept more items, and terminal Object Publish can accept each output chunk before the runtime advances.

Across an await boundary, there is no continuous in-memory demand signal. The request has left the process and the completion may arrive later through a different transport, worker, or JVM. TPF therefore turns the boundary into durable coordination instead of pretending one `Multi` subscription still spans the whole flow:

1. dispatch the await interactions,
2. record dispatch completion for the await unit,
3. park the parent execution as `WAITING_EXTERNAL`,
4. admit completions by correlation/idempotency,
5. resume item continuations only after the durable gates are satisfied,
6. release the parent execution when the itemized unit is complete,
7. publish terminal output before the execution is marked successful.

That is why `ONE_TO_ONE` await over a stream is not a hidden batch mode. It is a stream of item interactions owned by one durable await unit. The external provider is not a pipeline step; it is external reality behind a framework-owned I/O shell.

```mermaid
sequenceDiagram
    participant Source as "Live source segment"
    participant Await as "Await step"
    participant Unit as "Await unit"
    participant Store as "Execution store"
    participant External as "External actor"
    participant Continue as "Continuation segment"
    participant Publish as "Object Publish"

    Source->>Await: emit typed item(s)
    Await->>Unit: create item or aggregate interactions
    Await->>External: dispatch request(s)
    Await->>Unit: mark dispatch complete
    Await-->>Store: suspend parent execution
    Store->>Store: persist WAITING_EXTERNAL(awaitUnitId)
    External-->>Unit: admit correlated completion(s)
    Unit->>Unit: release only when unit shape is complete
    Unit-->>Continue: resume typed output(s)
    Continue-->>Publish: terminal domain output
    Publish-->>Store: publish before markSucceeded
```

The await unit acts as a gatekeeper. For itemized `ONE_TO_ONE` over a stream, it groups item interactions so completions can be ordered and released safely; it does not turn the provider call into a batch request. For aggregate cardinalities, the unit is the durable batch shape: input and/or output is materialized as one replayable unit.

## Await Versus Checkpoint Handoff

Await and checkpoint handoff both cross a process boundary, but they assign ownership differently.

| Concern | Await | Checkpoint handoff |
| --- | --- | --- |
| Execution ownership | same execution parks and resumes | another pipeline admits independent work |
| Boundary | mid-pipeline external wait | terminal or named publication boundary |
| Completion | correlated interaction completion | downstream checkpoint admission |
| Retry and DLQ | owning execution remains responsible | downstream orchestrator owns retry and DLQ after admission |
| Use when | the external result belongs to the same business flow | another flow should own the next lifecycle |

Use await for human approvals, webhook callbacks, and provider decisions that must resume the same business flow. Use checkpoint handoff when the next workflow has separate ownership, scaling, or operational responsibility.

## Design Responsibilities

Design each await boundary with:

1. a stable business idempotency key,
2. explicit request and completion types,
3. a timeout that matches the business expectation,
4. duplicate-safe external effects,
5. a clear owner for late or rejected completions.

The transport can be `interaction-api`, `webhook`, Kafka, or SQS, but that is not the core modeling decision. The core decision is that the pipeline pauses at an explicit business boundary and resumes only when a typed completion is admitted.

## Where To Go Next

- [Await runtime setup](/deploy/orchestrator-runtime/await) covers adapters, runtime mode, and configuration.
- [Concurrency and backpressure sizing](/deploy/concurrency-and-backpressure) explains how backpressure changes at durable boundaries.
- [Await operations](/operate/await-boundaries) covers pending interactions, duplicate completions, replay events, and operational checks.
- [Await Unit Runtime](/evolve/await-unit-runtime/) covers the internal durable model.
- [Operators](/design/operators) covers immediate external calls that do not suspend and resume later.
