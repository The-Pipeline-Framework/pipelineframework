# Observability Overview

Observability in The Pipeline Framework is designed for distributed pipelines: you should be able to see what each step did, how long it took, and where failures occurred.

## What You Get Out of the Box

1. **Metrics**: Step timings, throughput, and failure counts
2. **Tracing**: End-to-end request visibility across steps
3. **Logging**: Structured logs with correlation identifiers
4. **Health Checks**: Liveness/readiness for orchestration
5. **Alerting**: Dashboards and alert rules tuned for pipeline behavior

## Sections

- [Metrics](/guide/operations/observability/metrics)
- [Tracing](/guide/operations/observability/tracing)
- [Logging](/guide/operations/observability/logging)
- [Health Checks](/guide/operations/observability/health-checks)
- [Alerting](/guide/operations/observability/alerting)
- [Security Notes](/guide/operations/observability/security)

## Dev Mode Behavior (NR vs LGTM)

TPF keeps observability lightweight by default in dev. You opt in to external collectors via env vars.

### New Relic (automatic when `NEW_RELIC_LICENSE_KEY` is set)

If `NEW_RELIC_LICENSE_KEY` is present, the runtime config source auto-enables OTel export and disables LGTM.
No application properties changes are required.

Enabled settings (defaults):
- `quarkus.otel.enabled=true`
- `quarkus.otel.traces.enabled=true`
- `quarkus.otel.metrics.enabled=true`
- `quarkus.otel.metric.export.interval=5s`
- `quarkus.otel.traces.sampler=parentbased_traceidratio`
- `quarkus.otel.traces.sampler.arg=0.001`
- `quarkus.otel.exporter.otlp.endpoint=${NEW_RELIC_OTLP_ENDPOINT:https://otlp.eu01.nr-data.net:443}`
- `quarkus.otel.exporter.otlp.protocol=http/protobuf`
- `quarkus.otel.exporter.otlp.compression=gzip`
- `quarkus.otel.exporter.otlp.metrics.temporality.preference=delta`
- `quarkus.otel.exporter.otlp.headers=api-key=${NEW_RELIC_LICENSE_KEY}`
- `quarkus.observability.lgtm.enabled=false`

Usage:
```
export NEW_RELIC_LICENSE_KEY=...
export NEW_RELIC_OTLP_ENDPOINT=https://otlp.nr-data.net:443
./mvnw quarkus:dev
```

### LGTM (explicit opt-in)

LGTM Dev Services are off by default. Enable them explicitly:
```
export QUARKUS_OBSERVABILITY_LGTM_ENABLED=true
export QUARKUS_MICROMETER_EXPORT_PROMETHEUS_ENABLED=true
./mvnw quarkus:dev
```

This enables Prometheus metrics for Grafana dashboards and activates the LGTM stack.

### Prometheus/Micrometer Defaults

Templates and example services default to:
```
quarkus.micrometer.export.prometheus.enabled=${QUARKUS_MICROMETER_EXPORT_PROMETHEUS_ENABLED:false}
```
so Prometheus/LGTM are opt-in and do not slow down normal dev runs.
