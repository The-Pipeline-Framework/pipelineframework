# Await Boundary Operations

Await boundaries are operationally different from ordinary remote calls. A `kind: await` step parks a `QUEUE_ASYNC` execution, dispatches work to an external actor, and resumes only after a correlated completion is admitted.

Use this page with [Await Boundaries](/design/await-boundaries) for application design, [Await runtime setup](/deploy/orchestrator-runtime/await) for adapter configuration, and [Replay & Live Topology](/operate/observability/replay) for replay inspection.

## Runtime Requirements

Await requires `QUEUE_ASYNC`. The owning execution must be stored before it can wait on an external result.

At minimum:

```properties
pipeline.orchestrator.mode=QUEUE_ASYNC
pipeline.orchestrator.resume-token-secret=${PIPELINE_ORCHESTRATOR_RESUME_TOKEN_SECRET}
```

For crash-surviving environments, use durable queue-async providers:

```properties
pipeline.orchestrator.state-provider=dynamo
pipeline.orchestrator.dispatcher-provider=sqs
pipeline.orchestrator.dlq-provider=sqs
```

The state transition that parks or resumes the execution is guarded by the orchestrator store. External dispatch and external side effects remain at-least-once.

## Transport Responsibilities

| Transport | Operational responsibility |
| --- | --- |
| `interaction-api` | A UI or client must list pending interactions and call the generated completion API. |
| `webhook` | Configure a stable resume-token secret, reachable callback URLs, and partner retry/idempotency handling. |
| `kafka` | Configure request and response channels, monitor broker/consumer health, and keep correlation ids stable. |
| `sqs` | Configure request and response queues, monitor poller health, size visibility timeouts, and attach queue DLQ policy. |

Kafka and SQS await use framework-owned request and completion envelopes. The external provider is not a pipeline step; it is the actor that completes the await interaction.

## Idempotency And Completion

Await protects TPF-owned execution state, not external business effects.

Design each external boundary with:

1. stable business idempotency keys,
2. duplicate-safe provider requests,
3. duplicate-safe completion admission,
4. durable or queryable business records where the external effect matters.

Late or duplicate completions can be dropped when the target interaction is already terminal, stale, or otherwise not admissible. Monitor:

- `tpf.await.completion.dropped.total`

## Replay And Tracing

Replay and trace events expose the lifecycle of the await unit:

- `await_interaction_dispatched`
- `await_unit_dispatch_complete`
- `await_execution_waiting`
- `await_unit_item_completed`
- `await_unit_completed`
- `await_resume_released`
- `await_unit_terminal`

Use these events to separate runtime behavior from viewer interpretation. For example, in `csv-payments`, `Await Payment Provider` is `ONE_TO_ONE` over a stream, so item completions should appear as provider responses are admitted. The replay viewer should show those real lifecycle events rather than inventing smoothed timing.

## Aggregate Limits

`ONE_TO_ONE` over a stream is itemized. Aggregate await shapes materialize input and/or output units in the current runtime:

| Config key | Default | Applies to |
| --- | --- | --- |
| `pipeline.orchestrator.await-aggregate-max-input-items` | `10000` | materialized input units for `MANY_TO_ONE` and `MANY_TO_MANY` await steps |
| `pipeline.orchestrator.await-aggregate-max-output-items` | `10000` | materialized output units for `ONE_TO_MANY` and `MANY_TO_MANY` await steps |

Do not use unbounded aggregate await payloads. If replay of a materialized output unit fails halfway through downstream execution, TPF restarts that output unit as a whole.

## Await Versus Checkpoint Handoff

Await and checkpoint handoff both cross a process boundary, but they assign ownership differently.

| Concern | Await | Checkpoint handoff |
| --- | --- | --- |
| Execution ownership | one execution parks and later resumes | one pipeline publishes and another pipeline admits independent work |
| Boundary | mid-pipeline external wait | terminal or named publication boundary |
| Completion | correlated interaction completion | downstream checkpoint admission |
| Retry and DLQ | owning execution remains responsible | downstream orchestrator owns retry and DLQ after admission |
| Use when | the external result belongs to the same business flow | another pipeline should own the next lifecycle |

Use await for human approvals, webhook callbacks, and brokered provider decisions that must resume the same execution. Use checkpoint handoff when the receiving workflow has separate ownership, scaling, or operational responsibility.
