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

## Dashboard discovery

LGTM Dev Services discovers Grafana dashboards from classpath resources under `META-INF/grafana/`
that use the `grafana-dashboard-*.json` naming convention.

For `csv-payments`, the repo now ships separate resources for:

- Prometheus-backed metrics dashboard
- Tempo tracing entry surface

Keep Tempo separate from the Prometheus dashboard. Use Tempo for live topology and trace drill-down,
and use Prometheus-backed panels for throughput, latency, queue depth, inflight, and retries.

## Tempo versus Prometheus

- Tempo receives spans through OTLP exporters in real time.
- Prometheus scrapes metrics on its own interval.

If a panel looks stale because of scrape timing, that is a metrics issue, not a tracing issue.
For the full surface split, see [Replay & Live Topology](/guide/operations/observability/replay).

## Prometheus/Micrometer Defaults

Templates and example services default to:

```properties
quarkus.micrometer.export.prometheus.enabled=${QUARKUS_MICROMETER_EXPORT_PROMETHEUS_ENABLED:false}
```

so Prometheus/LGTM are opt-in and do not slow down normal dev runs.
