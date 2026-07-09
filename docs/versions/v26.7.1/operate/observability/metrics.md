---
search: false
---

# Metrics

The framework exposes metrics through Quarkus and Micrometer, giving step-level visibility into throughput, latency, and failures.

## Built-in Metrics

Typical metrics you can expect to expose:

1. Execution duration per step
2. Success and failure counts
3. End-to-end pipeline latency
4. Throughput and backpressure signals
5. Error rates by step and error type

## Micrometer Integration

Micrometer is the default metrics façade. You can export to Prometheus or other backends supported by Quarkus.

```properties
quarkus.micrometer.export.prometheus.enabled=true
quarkus.micrometer.export.prometheus.path=/q/metrics
```

## Dashboards

Pair metrics with Grafana dashboards that show:

1. Step latency percentiles (p95/p99)
2. Throughput per step
3. Error rate by step
4. Pipeline end-to-end latency

## Execution Channels and Signals

Queue-async operations involve three distinct channels that should be monitored separately:

| Channel | What it means | Core signals |
|---|---|---|
| Worker/dispatcher control plane | orchestration coordination and progress | queue depth, worker lag, lease conflicts, stale commits, sweeper recoveries |
| Execution DLQ | terminal execution failures | DLQ publish count, provider queue depth, the oldest message age |
| Item Reject Sink | item-level recover-and-continue business rejects | `tpf.step.reject.total`, provider queue depth (when durable), reject fingerprint concentration |

Operational interpretation:

1. High worker lag or stale/lease contention points to orchestration pressure or dependency latency.
2. Execution DLQ growth points to systemic execution failures that require execution-level triage.
3. Item reject growth often indicates data-quality/business-rule drift and should route to business remediation and selective re-drive.

## LGTM Metrics Pipeline

LGTM Dev Services ship an OTLP collector and Prometheus. Grafana's built-in dashboards read
from the Prometheus datasource, so Prometheus scraping must be enabled even if OTLP export
is configured. For OTLP-first dashboards, you need a Grafana datasource that reads OTLP
metrics storage (for example Mimir) instead of Prometheus.

Prometheus cadence only affects the metrics view. It does not affect live trace delivery to Tempo.
For the split between metrics dashboards, Tempo live topology, and replay playback, see
[Replay & Live Topology](/versions/v26.7.1/operate/observability/replay).

## Parallelism and Backpressure

TPF emits additional metrics and span attributes to showcase parallelism and buffer pressure:

Metrics (OTel/Micrometer):
- `tpf.step.inflight` (gauge): in-flight items per step (`tpf.step.class` attribute)
- `tpf.step.buffer.queued` (gauge): queued items in the backpressure buffer (`tpf.step.class` attribute)
- `tpf.step.buffer.capacity` (gauge): configured backpressure buffer capacity per step (`tpf.step.class` attribute)
- `tpf.step.parent` (attribute): parent step class for plugin steps (same as `tpf.step.class` for regular steps)
- `tpf.pipeline.max_concurrency` (gauge): configured max concurrency for the pipeline run
- `tpf.item.produced` (counter): items produced at the configured item boundary
- `tpf.item.consumed` (counter): items consumed at the configured item boundary
- `tpf.slo.rpc.server.*` (counters): SLO-ready totals for RPC server reliability and latency (gRPC + REST)
- `tpf.slo.rpc.client.*` (counters): SLO-ready totals for RPC client reliability and latency (gRPC + REST)
- `tpf.slo.item.throughput.*` (counters): SLO-ready totals for item throughput per run

Prometheus exports these as `*_items` because the unit is set to `items`.

Note: `tpf.step.*` metrics represent step executions (not domain items). Use the
`tpf.item.*` counters when you want throughput for a specific domain type.

Note: New Relic dimensional metrics treat `tpf.slo.item.throughput.*` as event-counted counters, so
SLOs should use `COUNT` (not `SUM`) over `metricName = 'tpf.slo.item.throughput.total|good'`.

Aspect position note: AFTER_STEP observes the output of each step. This captures every boundary
except the very first input boundary (before the pipeline starts). Conversely, BEFORE_STEP captures
every boundary except the final output boundary (after the pipeline completes). Use two aspects if
you need complete boundary coverage.

Run-level span attributes (on `tpf.pipeline.run`):
- `tpf.parallel.max_in_flight`
- `tpf.parallel.avg_in_flight`

These are designed for batch-style pipelines where parallelism should be inspected while the pipeline is running.

Tip: gauges report the instantaneous value, so after a run finishes they will return to 0.
When querying, use a max over time window to surface the peak:

```text
max(tpf_step_inflight_items) by (tpf_step_class)
max(tpf_step_buffer_queued_items) by (tpf_step_class)
```

## Custom Metrics

Use Micrometer to add counters and timers inside your services:

```java
@Inject
MeterRegistry registry;

Timer timer = registry.timer("payment.processing.duration");
Counter success = registry.counter("payment.processing.success");

return timer.recordCallable(() -> processPayment(record));
```

## Connector And Await Boundary Metrics

Connector-owned I/O and await boundaries use low-cardinality metrics. Keep object keys, execution ids, await unit ids, interaction ids, and correlation ids in spans/replay events, not metric attributes.

Object Ingest metrics:

| Metric | Type | Attributes | Meaning |
| --- | --- | --- | --- |
| `tpf.object_ingest.list.total` | counter | `tpf.object_ingest.source`, `tpf.object_ingest.provider` | Source listing attempts. |
| `tpf.object_ingest.listed.objects.total` | counter | `tpf.object_ingest.source`, `tpf.object_ingest.provider` | Objects returned by listing. |
| `tpf.object_ingest.submitted.total` | counter | `tpf.object_ingest.source`, `tpf.object_ingest.provider` | Objects accepted as queue-async execution inputs. |
| `tpf.object_ingest.duplicate.total` | counter | `tpf.object_ingest.source`, `tpf.object_ingest.provider` | Duplicate object admissions resolved by idempotency. |
| `tpf.object_ingest.failed.total` | counter | `tpf.object_ingest.source`, `tpf.object_ingest.provider` | Listing, mapping, or submission failures. |

Object Publish metrics:

| Metric | Type | Attributes | Meaning |
| --- | --- | --- | --- |
| `tpf.object_publish.grouped.total` | counter | `tpf.object_publish.target` | Terminal output grouping operations. |
| `tpf.object_publish.grouped.items.total` | counter | `tpf.object_publish.target` | Terminal items seen by Object Publish. |
| `tpf.object_publish.grouped.groups.total` | counter | `tpf.object_publish.target` | Object groups created for publication. |
| `tpf.object_publish.published.total` | counter | `tpf.object_publish.target`, `tpf.object_publish.provider` | Objects successfully written. |
| `tpf.object_publish.published.bytes.total` | counter | `tpf.object_publish.target`, `tpf.object_publish.provider` | Bytes written by successful publishes. |
| `tpf.object_publish.skipped.total` | counter | `tpf.object_publish.target` | Empty terminal outputs skipped. |
| `tpf.object_publish.failed.total` | counter | `tpf.object_publish.target`, `tpf.object_publish.provider` | Publish failures. |
| `tpf.object_publish.write.duration` | histogram | `tpf.object_publish.target`, `tpf.object_publish.provider` | Provider write duration in milliseconds. |

Await boundary metrics:

| Metric | Type | Attributes | Meaning |
| --- | --- | --- | --- |
| `tpf.await.interaction.dispatched.total` | counter | step, status, transport | Await interactions dispatched to an external actor. |
| `tpf.await.unit.dispatch_complete.total` | counter | step, cardinality, status | Await units whose dispatch phase completed. |
| `tpf.await.completion.admitted.total` | counter | step, status, transport | Completion envelopes admitted into await state. |
| `tpf.await.item.completed.total` | counter | step, cardinality, status, transport | Itemized await completions recorded. |
| `tpf.await.completion.early_held.total` | counter | step, cardinality, status, transport | Item completions held for durable fallback because live release was not available yet. |
| `tpf.await.resume.released.total` | counter | step, cardinality/status/transport when known | Await resumes released from durable fallback state. |
| `tpf.await.unit.terminal.total` | counter | step, cardinality, status, transport | Await units reaching terminal state. |
| `tpf.await.completion.latency` | histogram | step, status, transport | Time from interaction creation to completion admission. |
| `tpf.await.unit.duration` | histogram | step, cardinality, status, transport | Time from await unit creation to terminal state. |
| `tpf.await.completion.dropped.total` | counter | transport, reason | Completions that cannot be admitted because the target interaction is already terminal, stale, or otherwise not admissible. |

Prometheus exports OpenTelemetry units in the metric name. For example, the duration histograms are exported with `_milliseconds_*` suffixes in the Quarkus Prometheus endpoint.

SLO-friendly derived indicators:

1. Await admission reliability: admitted completions divided by admitted plus dropped completions.
2. Await flow health: admitted completions followed by downstream step progress in the live path; durable resume releases and early-held completions draining in fallback paths.
3. Object publish reliability: published objects divided by published plus failed objects.
4. Object publish latency: p95/p99 of `tpf.object_publish.write.duration` by provider and target.
5. CSV output completeness: Object Publish grouped item count matches the terminal `PaymentOutput` count for the run.

Keep demo expectations separate from production SLOs. The CSV Payments demo may report provider permits/sec, wall time, throughput, output count, and checksum, but production thresholds should come from service-specific latency/error budgets and provider-native backlog/lag signals.

Use replay JSON for high-cardinality drill-down. It includes object keys, await unit ids, interaction ids, execution ids, and correlation fields that must not become metric dimensions.

## Orchestrator Queue-Async Signals

For `QUEUE_ASYNC`, include control-plane metrics in addition to step metrics.
Treat these as required operational signals for GA readiness:

1. lease claim conflicts (OCC contention),
2. stale commit rejections,
3. retry scheduling rate and retry-saturation ratio,
4. due-sweeper recovery count (persisted-before-dispatch gap recovery),
5. execution DLQ publish count and backlog depth,
6. item reject sink publish count and backlog depth,
7. queue depth and worker lag.

Use these to separate dependency outages (high retries, low success) from coordination issues (high stale/lease conflicts).

Implementation note:

1. TPF core already emits step/pipeline telemetry.
2. Control-plane metrics may be emitted by provider integration or surrounding platform telemetry (queue, datastore, worker runtime).
3. Keep metric names stable per environment even if data comes from different backends.

Step-level reject signal:

- `tpf.step.reject.total` (counter): rejected step items published to item reject sinks.

Await execution logs include the parked await unit when a `QUEUE_ASYNC` execution waits or resumes. Replay and trace events expose await unit lifecycle transitions, including dispatch, waiting, item completion, unit completion, resume release, and terminal timeout/failure states. Metrics expose aggregate await-boundary health without high-cardinality ids.

Command signal:

- command steps participate in normal `tpf.step.*` metrics and `tpf.step` spans,
- command effect lifecycle is exposed through `tpf.command.effect.*` metrics,
- effect lifecycle state is also recorded in the configured `CommandEffectStore`,
- provider backlog, throttling, and external latency should come from the provider connector or platform telemetry.

Replay topology marks command steps with `renderRole: "command"` and `actorKind` equal to the command name. That topology signal is for inspection and playback; it is not a replacement for provider-native command metrics.

Command effect metrics:

| OpenTelemetry metric | Prometheus name | Type | Key attributes |
| --- | --- | --- | --- |
| `tpf.command.effect.transition.total` | `tpf_command_effect_transition_total` | counter | `tpf.command`, `tpf.command.step`, `tpf.command.status` |
| `tpf.command.effect.duplicate.total` | `tpf_command_effect_duplicate_total` | counter | `tpf.command`, `tpf.command.step`, `tpf.command.duplicate_policy`, `tpf.command.duplicate_result` |
| `tpf.command.effect.duration` | `tpf_command_effect_duration_*` | histogram | `tpf.command`, `tpf.command.step`, `tpf.command.status` |

Command status values are `pending`, `dispatching`, `succeeded`, `failed_retryable`, and `dlq`.
Duplicate result values are `returned_recorded`, `rejected`, and `in_progress`.

Command step SLO examples:

| SLO | Primary signal | Example objective |
| --- | --- | --- |
| Command effect completion | `tpf.command.effect.transition.total{tpf_command_status="succeeded"}` | 99.9% of command effects reach `SUCCEEDED` within 5 minutes. |
| Terminal command failure budget | `tpf.command.effect.transition.total{tpf_command_status="dlq"}` | Fewer than 0.1% of command effects enter terminal DLQ over 30 days. |
| Command step latency | `tpf.step` latency for the command step | p95 under the service target for the provider, such as 2 seconds for search indexing. |
| Replay duplicate safety | `tpf.command.effect.duplicate.total{tpf_command_duplicate_result="returned_recorded"}` plus provider write count | `RETURN_RECORDED` duplicates return stored output with zero extra provider writes. |
| Provider health | connector or provider metrics | Provider throttling and backlog stay below the connector's retry budget. |

PromQL examples:

```text
# Success ratio by command over 5 minutes.
sum by (tpf_command) (rate(tpf_command_effect_transition_total{tpf_command_status="succeeded"}[5m]))
/
sum by (tpf_command) (rate(tpf_command_effect_transition_total{tpf_command_status=~"succeeded|failed_retryable|dlq"}[5m]))
```

```text
# Terminal DLQ rate by command.
sum by (tpf_command) (rate(tpf_command_effect_transition_total{tpf_command_status="dlq"}[5m]))
```

```text
# Recorded duplicate replays by command.
sum by (tpf_command) (increase(tpf_command_effect_duplicate_total{tpf_command_duplicate_result="returned_recorded"}[1h]))
```

```text
# p95 command effect duration by command.
histogram_quantile(
  0.95,
  sum by (le, tpf_command) (rate(tpf_command_effect_duration_bucket{tpf_command_status="succeeded"}[5m]))
)
```

For a Search/OpenSearch indexing command, use `tpf.command.effect.transition.total` to count
`succeeded`, `failed_retryable`, and `dlq` effects by `tpf_command="opensearch-index-document"`.
Use `tpf.step.duration` for the `Write Search Index Document` step's pipeline latency, and use
OpenSearch client or cluster metrics for provider latency, throttling, and indexing failures.

Backlog signal note:

1. TPF emits `tpf.step.reject.total` for reject throughput.
2. Backlog depth is provider-native:
3. use SQS queue depth/age for durable sinks (`provider=sqs`),
4. use retained-size logs/metrics for in-memory sink (`provider=memory`).

## Design Tips

1. Prefer low-cardinality labels
2. Track user-visible latency
3. Align metrics with SLIs/SLOs
4. Measure queue depth if you use streaming steps
