# Observability Best Practices

## Configure All Three Layers

Treat build capability, TPF telemetry policy, and exporter/backend configuration as separate
decisions. Verify the capability in every deployable role, including coordinators and transition
workers, before debugging collectors or dashboards.

Instrumentation enabled with no exporter is a valid non-failing setup. An exporter configured for
an artifact that was not built with the signal capability is not.

## Observe Semantic Boundaries

Instrument the runtime fact where ownership changes, work queues, durable state changes, a retry can
occur, or execution crosses a process or network boundary. For a stage where latency can accumulate,
keep enough start/end facts to locate that time.

Do not infer important lifecycle facts from method names or synchronous method duration. A method
that returns `Uni` or `Multi` has not completed its work when it returns; asynchronous telemetry must
cover subscription through completion, failure, or cancellation.

## Keep Signals Coherent

Use metrics for low-cardinality rates, distributions, and current pressure. Put execution,
interaction, correlation, request, and item identity in traces, replay, or structured logs. Do not
add those identities to metric labels to make a single investigation easier.

Preserve parent/child context while execution remains live. At an intentional durable boundary,
persist origin context and use an explicit span link when parentage cannot legitimately continue.
Correlation attributes help investigation but do not replace parentage or a link.

## Operate the Whole Journey

Build dashboards around semantic stages, not implementation methods. Pair throughput and latency
with in-flight/queue pressure, retry/reject/timeout signals, live-versus-durable routing, and terminal
publication outcomes. A healthy run may legitimately have no retry or fallback samples; the primary
stage journey must still be present.

Keep replay, traces, and metrics in their proper roles:

- metrics: aggregate health and SLOs;
- traces: live topology, causal relationships, and cross-boundary latency;
- replay: deterministic, high-cardinality post-run reconstruction.

See [Metrics](/operate/observability/metrics), [Tracing](/operate/observability/tracing),
[Replay & Live Topology](/operate/observability/replay), and
[LGTM](/operate/observability/lgtm) for the concrete contracts.
