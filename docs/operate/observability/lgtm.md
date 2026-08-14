# Dev Mode Behavior (LGTM)

The Pipeline Framework (TPF) keeps observability lightweight by default in dev. You opt in to external collectors via env vars.

## LGTM (explicit opt-in)

LGTM Dev Services are off by default. Enable them explicitly:

```bash
export QUARKUS_OBSERVABILITY_LGTM_ENABLED=true
export QUARKUS_MICROMETER_EXPORT_PROMETHEUS_ENABLED=true
./mvnw quarkus:dev
```

This enables Prometheus metrics for Grafana dashboards and activates the LGTM stack.

Note: when LGTM Dev Services are enabled, Quarkus may override some OTel timing defaults
for dev convenience (for example `quarkus.otel.metric.export.interval=10s`).

LGTM is an export/runtime choice, not a substitute for build capability or TPF telemetry policy.
The application still needs the matching Quarkus capability, and TPF still needs the corresponding
`pipeline.telemetry.*.enabled` signal policy. A disabled exporter must not be interpreted as a
disabled framework signal; backend health and exporter retry remain platform concerns.

## Dashboard discovery

LGTM Dev Services discovers Grafana dashboards from classpath resources under `META-INF/grafana/`
that use the `grafana-dashboard-*.json` naming convention.

For `csv-payments`, the repo now ships separate resources for:

- `grafana-dashboard-csv-payments.json`: the Prometheus-backed operator dashboard, with the
  eight-stage proof journey as its first row
- `grafana-dashboard-csv-payments-tempo.json`: executable Tempo journey and continuity panels,
  including the Await rootless/unlinked diagnostic

Keep Tempo separate from the Prometheus dashboard. Use Tempo for live topology and trace drill-down,
and use Prometheus-backed panels for throughput, latency, queue depth, inflight, and retries.

For `csv-payments`, the dedicated Tempo verification E2E does not rely on nested LGTM Dev Services inside the service containers. It starts an explicit LGTM stack, provisions both dashboards, and points the modular services plus packaged orchestrator at its OTLP collector. This is the telemetry-capable modular/LGTM proof profile: tracing and metrics are build-capable, the framework policy enables them, Prometheus supplies the metrics dashboard, and OTLP supplies Tempo.

The CSV self-host HA profile does not emit telemetry in this verification setup, so do not expect it
to populate either Grafana dashboard. See the
[CSV Payments runbook](https://github.com/The-Pipeline-Framework/pipelineframework/tree/main/examples/csv-payments)
for the fast verification and opt-in 10k operator-proof commands.

## Tempo versus Prometheus

- Tempo receives spans through OTLP exporters in real time.
- Prometheus scrapes metrics on its own interval.

If a panel looks stale because of scrape timing, that is a metrics issue, not a tracing issue.
For the full surface split, see [Replay & Live Topology](/operate/observability/replay).

## Prometheus/Micrometer Defaults

Templates and example services default to:

```properties
quarkus.micrometer.export.prometheus.enabled=${QUARKUS_MICROMETER_EXPORT_PROMETHEUS_ENABLED:false}
```

so Prometheus/LGTM are opt-in and do not slow down normal dev runs.
