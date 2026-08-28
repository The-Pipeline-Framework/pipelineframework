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

### Constructing output from the suspended request

Human interfaces and external providers should submit only the facts they own. When
the canonical Await output also needs trusted fields from the suspended request,
declare a smaller completion type and a pure projector:

```yaml
steps:
  - name: Confirm Property
    kind: await
    cardinality: ONE_TO_ONE
    input: PendingConfirmation
    output: ConfirmedInvoice
    timeout: PT8H
    await:
      correlation:
        strategy: interactionId
      completion:
        type: com.example.PropertyChoice
        projector: com.example.ConfirmedInvoiceProjector
      transport:
        type: interaction-api
```

The projector implements `AwaitCompletionProjector<PendingConfirmation,
PropertyChoice, ConfirmedInvoice>`. It receives the canonical request, the admitted
actor payload, and framework-authored completion metadata such as `completedAt`.
It must be public, have a public no-argument constructor, and remain deterministic
and side-effect free.

TPF persists the request and projected completion as canonical values. Recovery
therefore rebuilds the descriptor and resumes from the already projected output;
it does not ask the browser to echo trusted invoice state and does not call the
projector again for normally admitted canonical completions.

### Await on a union alternative

In v3, `accepts` selects the union alternatives that invoke an Await step, just as it
selects the alternatives that invoke an ordinary step:

```yaml
steps:
  - name: Clarify
    kind: await
    cardinality: ONE_TO_ONE
    input: PreparationDecision
    accepts: [ClarificationRequired]
    output: Prepared
    timeout: PT8H
    await:
      correlation: { strategy: interactionId }
      completion:
        type: com.example.ClarificationAnswer
        projector: com.example.ClarificationProjector
      transport: { type: interaction-api }
```

Here the projector implements `AwaitCompletionProjector<ClarificationRequired,
ClarificationAnswer, Prepared>`. The compiler proves all three generic arguments and
generates the Await boundary with `ClarificationRequired` as its request type. If the
step accepts multiple variants, or omits `accepts`, the projector input remains the
declared `PreparationDecision` union.

Alternatives not accepted by the Await do not invoke it and continue unchanged through
the ordinary v3 pipeline flow. There is no Await-specific pass-through mapper and no implicit
collection-to-stream or stream-to-collection conversion.

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

Await separates a pipeline into live reactive segments and durable recovery state.

Inside a live segment, normal reactive demand and backpressure can apply between adjacent steps. A streaming input step can slow down when the downstream step cannot accept more items, and terminal Object Publish can accept each output chunk before the runtime advances.

For brokered `ONE_TO_ONE` await over a stream, `QUEUE_ASYNC` can keep a live await session open while the parent transition is still running. The session is keyed by the durable await unit. Each input item creates a durable interaction and is dispatched through the await transport; each completion is recorded durably before it is offered to the live resumed segment. Source parsing then advances by demand and the configured in-flight window, not by a forced sleep or demand pacer.

This provider-facing admission budget is backpressure: it bounds unresolved interactions. It does not make outbound provider dispatch a circuit boundary. Circuit admission currently protects generated remote calls and eligible shared transition-worker dispatch; it is complementary to await admission, not a replacement for it. See [Execution Safety](/design/execution-safety).

The durable await model still matters. If the process restarts, the worker lease is lost, or a completion arrives after the live session is gone, TPF falls back to durable coordination:

1. record dispatched interactions and dispatch completion for the await unit,
2. park the parent execution as `WAITING_EXTERNAL` when the transition suspends,
3. admit completions by correlation/idempotency,
4. resume item continuations from durable state when no live session accepted the completion,
5. release the parent execution when the itemized unit is complete,
6. publish terminal output before the execution is marked successful.

This fallback reconstructs an immutable completed MANY result for the parent continuation. It is intentionally more conservative than the healthy live segment; TPF does not claim to resume the same in-memory `Multi` at its prior demand position after owner loss.

For portable transition workers, durable fallback applies only when the contract is ineligible for the live itemized shape or no live session accepts the completion. Eligible portable workers retain the live session and terminal stream while the worker remains active.

That is why `ONE_TO_ONE` await over a stream is not a hidden batch mode. It is a stream of item interactions owned by one durable await unit. The external provider is not a pipeline step; it is external reality behind a framework-owned I/O shell.

```mermaid
sequenceDiagram
    participant Source as "Live source segment"
    participant Await as "Await step"
    participant Interaction as "Await interaction"
    participant Unit as "Await unit / fallback state"
    participant Live as "Live await session"
    participant External as "External actor"
    participant Coordinator as "Coordinator"
    participant Continue as "Continuation segment"
    participant Publish as "Object Publish"
    participant Store as "Execution store"

    Source->>Await: emit typed item(s)
    Await->>Interaction: create durable item interaction(s)
    Await->>External: dispatch request(s)
    External-->>Interaction: admit correlated completion(s)
    alt active eligible live owner (in-process or portable)
      Interaction-->>Live: signal admitted completion
      Live-->>Continue: emit typed output when downstream requests
      Continue-->>Publish: terminal domain output
      Publish-->>Store: worker publishes before markSucceeded
    else interaction/webhook, no live session, or ineligible portable shape
      Await-->>Store: suspend parent execution
      Store->>Store: persist WAITING_EXTERNAL(awaitUnitId)
      Unit-->>Coordinator: completion is admitted
      Coordinator-->>Continue: schedule canonical continuation
    end
```

The await unit is the durable identity for the boundary. For itemized `ONE_TO_ONE` over a stream, it groups item interactions for ordering, dedupe, recovery, and fallback release; it does not turn the provider call into a batch request. For aggregate cardinalities, the unit is the durable batch shape: input and/or output is materialized as one replayable unit.

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
