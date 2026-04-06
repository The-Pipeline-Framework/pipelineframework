# Dev Mode Behavior (New Relic vs LGTM)

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

## Prometheus/Micrometer Defaults

Templates and example services default to:

```properties
quarkus.micrometer.export.prometheus.enabled=${QUARKUS_MICROMETER_EXPORT_PROMETHEUS_ENABLED:false}
```

so Prometheus/LGTM are opt-in and do not slow down normal dev runs.
