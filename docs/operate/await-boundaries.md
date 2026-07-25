# Await Boundary Operations

Await boundaries are operationally different from ordinary remote calls. A `kind: await` step dispatches work to an external actor and admits only correlated completions. Some paths park a `QUEUE_ASYNC` execution as `WAITING_EXTERNAL`; brokered itemized streams can keep a live await session open and use the parked state as the recovery fallback.

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

## Runtime Signals

In `QUEUE_ASYNC`, itemized await has a live path and a durable fallback path.

In the live path, a brokered `ONE_TO_ONE` stream keeps an in-memory await session open while the parent transition is alive. A completion is still recorded durably first, then the live session emits it to the resumed segment when downstream requests it. This is the normal connector-first CSV Payments path.

The durable fallback path is used when the live session is unavailable, after worker loss, or when a later claim must resume from stored state. In that path, the runtime uses durable coordination gates:

1. **Interaction dispatched**: TPF created await interactions and handed requests to the configured await transport.
2. **Unit dispatch complete**: the await unit has finished dispatching the known item set for that live segment.
3. **Parent wait durable**: the parent execution is stored as `WAITING_EXTERNAL` for that await unit when the transition suspends.
4. **Completion admitted**: a provider completion matched an interaction and was recorded idempotently.
5. **Early completion held**: a completion arrived when no live session could accept it and before the fallback release gates were true, so it was recorded but not used to start continuation yet.
6. **Resume released**: dispatch is complete, the parent wait is durable, and enough completions exist to resume the next segment from stored state.
7. **Unit terminal**: the await unit completed, timed out, or failed.

The matching metrics are:

| Gate | Metric |
| --- | --- |
| Interaction dispatched | `tpf.await.interaction.dispatched.total` |
| Unit dispatch complete | `tpf.await.unit.dispatch_complete.total` |
| Completion admitted | `tpf.await.completion.admitted.total` |
| Item completed | `tpf.await.item.completed.total` |
| Early completion held | `tpf.await.completion.early_held.total` |
| Resume released | `tpf.await.resume.released.total` |
| Unit terminal | `tpf.await.unit.terminal.total` |
| Completion latency | `tpf.await.completion.latency` |
| Unit duration | `tpf.await.unit.duration` |

Operational interpretation:

1. In the live path, admitted completions may move directly into downstream step telemetry without a separate durable resume release per item.
2. Admitted completions rising without downstream step progress points to live-session demand, provider/broker ordering, downstream backpressure, or fallback-release pressure.
3. Early-held completions are normal during races where providers answer quickly and no live session accepts the completion, but they should drain after the parent execution is durably waiting.
4. Dropped completions indicate stale, duplicate, or non-admissible completions; correlate them with transport retries and replay events.
5. Queue depth and provider lag remain provider-native signals. TPF does not scan the await store to synthesize backlog gauges.

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

## Troubleshooting Runbook

### Parser Appears To Race Ahead

In a healthy connector-first stream, the parser can be ahead of the provider, but it should not blast the whole source before downstream work starts. Compare replay times:

1. first parser `emit`,
2. first await dispatch,
3. first downstream step start,
4. last parser event,
5. last await completion.

If downstream status processing starts while parser and await dispatch are still active, the live path is working. If every parser event happens before the first downstream event, check `pipeline.max-concurrency`, admission wait and pending signals, step buffer metrics, and whether the await step is running through the live brokered path or durable fallback only. For an enabled durable `ONE_TO_ONE` await, interpret `pipeline.max-concurrency` as the shared unresolved-work budget for the external provider, not as a target for local CPU utilization.

### Await Stays Hot Or Red In Replay

Treat await heat as a question about where time is spent:

1. If dispatch finishes quickly but completions arrive slowly, inspect provider permits, provider latency, and broker lag.
2. If completions arrive but downstream steps do not start, inspect live-session demand and durable fallback metrics such as `tpf.await.completion.early_held.total` and `tpf.await.resume.released.total`.
3. If `await_resume_released` is low but downstream steps are running, that can be normal live-path behavior. Live handoff is visible through downstream step events, not through a durable release event per item.

### Completions Lag

Check the provider and broker before tuning TPF:

1. provider-side rate limit or permits,
2. request topic/queue publish latency,
3. response topic/queue consumer lag,
4. completion admission drops or duplicate completions.

The await store is the idempotency point, but it is not the external provider backlog. Use provider-native and broker-native dashboards for backlog age.

### Object Publish Drips Slowly

Object Publish is the terminal output boundary. It should start after terminal step output exists and must finish before execution success. If it appears slow:

1. compare `object_publish_grouped` and `object_publish_published` replay events,
2. check target provider write duration and bytes published,
3. check whether the terminal step is still producing output slowly,
4. verify `grouping.maxOpenGroups` is intentional for the input shape.

For CSV Payments, `grouping.maxOpenGroups: 1` is expected because Object Ingest admits one source object per execution.

### Kafka Topic Warnings

Short-lived local stacks can log transient `UNKNOWN_TOPIC_OR_PARTITION` warnings while topics are being created or containers are starting. Treat them as benign only if the consumer later joins, requests are dispatched, completions are admitted, and the run finishes. Persistent warnings mean the topic/bootstrap configuration or startup ordering is wrong.

### Stale Runtime Or Packaged Classes

If local behavior disagrees with source changes, rebuild with a worktree-local Maven cache and clear corrupted image/layer cache only when the build reports cache extraction errors. For CSV Payments, rebuild the selected layout before rerunning E2E so generated classes and packaged `org.pipelineframework` artifacts match the branch under test.

### Journal Or Index Misconfiguration

When the control-plane journal is enabled, due-work and timeout behavior depend on journal writes and indexes as well as the existing projections. If executions stop sweeping or timeouts do not fire, verify journal append success, projection version conflicts, due-work index configuration, and timeout index configuration before redriving executions.

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
