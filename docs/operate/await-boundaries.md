# Await Boundary Operations

Deferred completion is operationally different from an ordinary remote call. An operation decorated with `await:` produces its trusted immediate result, dispatches a completion request, and admits only correlated final observations. Some paths park a `QUEUE_ASYNC` execution as `WAITING_EXTERNAL`; brokered itemised streams can keep a live session open and use the parked state as the recovery fallback.

Use this page with [Await Boundaries](/architecture/await-boundaries) for application design, [Await runtime setup](/deploy/orchestrator-runtime/await) for adapter configuration, and [Replay & Live Topology](/operate/observability/replay) for replay inspection.

## Runtime Requirements

For a native Command using `await.callback`, completion is registered before effect dispatch. The
Quarkus HTTP ingress is `pipeline/callbacks/{signed-token}` relative to the application's public base.
Configure the application endpoint resolver and authenticator, keep the resume-token secret stable
across replicas, and route the public path to the owning application release. Authenticate provider
requests even though the resume token is signed.

Treat callback URLs as credentials: redact their token-bearing paths in reverse-proxy and HTTP
access logs, and do not put raw signatures or payloads in application authentication diagnostics.
The ingress bounds bodies to 1 MiB and headers to 32 KiB, and emits empty rejection responses.
Invalid requests and terminal admission rejections return 400. Internal lookup, authentication-service,
or completion failures return 503 so the provider can retry without receiving internal diagnostics.
Provider retries may receive the pinned success acknowledgement after duplicate admission. An
early callback cannot advance execution until Command dispatch settles; a late callback resumes
through ordinary Await admission. See [callback setup](/develop/connectors/openapi-import#command-completion-callbacks).

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

## Await Adapter Responsibilities

| Await adapter | Operational responsibility |
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

In `QUEUE_ASYNC`, itemised deferred completion has a live path and a durable fallback path.

In the live path, a brokered operation stream keeps an in-memory await session open while the parent transition is alive. A completion is still recorded durably first, then the live session emits it to the resumed segment when downstream requests it. This is the normal connector-first CSV Payments path.

For the eligible portable shape, the transition worker is the live owner. Durable interaction admission and retry state remain framework-owned; the coordinator does not need to take ownership of the live `Multi` simply because the completion transport is remote.

The durable fallback path is used when the live session is unavailable, after worker loss, or when a later claim must resume from stored state. In that path, the runtime uses durable coordination gates:

1. **Interaction dispatched**: TPF created await interactions and handed requests to the configured await transport.
2. **Unit dispatch complete**: the durable-fallback await unit has finished dispatching its known item set.
3. **Parent wait durable**: the parent execution is stored as `WAITING_EXTERNAL` for that await unit when the transition suspends.
4. **Completion admitted**: a provider completion matched an interaction and was recorded idempotently.
5. **Early completion held**: a completion arrived when no live session could accept it and before the fallback release gates were true, so it was recorded but not used to start continuation yet.
6. **Resume released**: dispatch is complete, the parent wait is durable, and enough completions exist to resume the next segment from stored state.
7. **Unit terminal**: the await unit completed, timed out, or failed.

The unit dispatch and parent-wait gates are durable-recovery gates. They are not prerequisites for a live item handoff and should not appear as a per-item critical path while a live owner is healthy.

The matching metrics are:

| Gate | Metric |
| --- | --- |
| Interaction dispatched | `tpf.await.interaction.dispatched.total` |
| Unit dispatch complete | `tpf.await.unit.dispatch_complete.total` |
| Completion admitted | `tpf.await.completion.admitted.total` |
| Item completed (fallback aggregate) | `tpf.await.item.completed.total` |
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

Replay and trace events expose the durable-fallback lifecycle of the await unit:

- `await_interaction_dispatched`
- `await_unit_dispatch_complete`
- `await_execution_waiting`
- `await_unit_item_completed`
- `await_unit_completed`
- `await_resume_released`
- `await_unit_terminal`

Use these events to separate durable fallback from viewer interpretation. A healthy live itemised handoff is instead visible through interaction completion and downstream step events; it does not synthesise await-unit item-completion or resume-release events. For example, in `csv-payments`, `Process Csv Payments Input` emits one pending `PaymentRecord` per row and decorates each with deferred completion, so downstream progress should begin as provider responses are admitted. Replay shows a completion overlay on that semantic operation rather than a separate Await node.

## Troubleshooting Runbook

### Parser Appears To Race Ahead

In a healthy connector-first stream, the parser can be ahead of the provider, but it should not blast the whole source before downstream work starts. Compare replay times:

1. first parser `emit`,
2. first await dispatch,
3. first downstream step start,
4. last parser event,
5. last await completion.

If downstream status processing starts while parser and completion dispatch are still active, the live path is working. If every parser event happens before the first downstream event, check `pipeline.max-concurrency`, admission wait and pending signals, step buffer metrics, and whether the decorated operation is running through the live brokered path or durable fallback only. Interpret `pipeline.max-concurrency` as the shared unresolved-work budget for the external provider, not as a target for local CPU utilisation.

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

## Cardinality And Limits

Deferred completion applies once to every result emitted by the authored operation.
There is no aggregate Await cardinality. If a provider acts on a whole batch, represent
that batch with an explicit bounded canonical collection type. Existing concurrency,
payload-size, deadline, and transport limits still apply to each interaction and its
completion.

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
