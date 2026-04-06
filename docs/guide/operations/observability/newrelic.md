# Using New Relic OTel

If `NEW_RELIC_LICENSE_KEY` is present, the runtime config source auto-enables OTel export and disables LGTM.
No application properties changes are required, automatically switches on when `NEW_RELIC_LICENSE_KEY` is set.

Enabled settings (defaults):
- `quarkus.otel.enabled=true`
- `quarkus.otel.traces.enabled=true`
- `quarkus.otel.metrics.enabled=true`
- `quarkus.otel.metric.export.interval=15s` (override with `NEW_RELIC_METRIC_EXPORT_INTERVAL` or `QUARKUS_OTEL_METRIC_EXPORT_INTERVAL`)
- `quarkus.otel.traces.sampler=parentbased_traceidratio`
- `quarkus.otel.traces.sampler.arg=0.001`
- `quarkus.otel.exporter.otlp.endpoint=${NEW_RELIC_OTLP_ENDPOINT:https://otlp.eu01.nr-data.net:443}`
- `quarkus.otel.exporter.otlp.protocol=http/protobuf`
- `quarkus.otel.exporter.otlp.compression=gzip`
- `quarkus.otel.exporter.otlp.metrics.temporality.preference=delta`
- `quarkus.otel.exporter.otlp.headers=api-key=${NEW_RELIC_LICENSE_KEY}`
- `quarkus.observability.lgtm.enabled=false`

Usage:

```bash
export NEW_RELIC_LICENSE_KEY=...
export NEW_RELIC_OTLP_ENDPOINT=https://otlp.nr-data.net:443
./mvnw quarkus:dev
```

## Forcing gRPC Client Spans (Dependencies)

Some pipelines need dependency edges even with low sampling. You can force sampling
of gRPC client spans for selected services:

```properties
pipeline.telemetry.tracing.client-spans.force=true
pipeline.telemetry.tracing.client-spans.allowlist=ProcessCsvPaymentsInputService,ProcessCsvPaymentsOutputFileService
```

When enabled, the orchestrator will always emit client spans for the allowlisted
services (using a sampled parent context) even if the global sampler is low.

**Important runtime behavior:**
- When `pipeline.telemetry.tracing.client-spans.force=true` is set with an empty `allowlist`, forcing applies to ALL services (not none), which can greatly increase span volume.
- Entries in `pipeline.telemetry.tracing.client-spans.allowlist` are matched by exact service-name equality (no wildcards or prefix matching supported).
- **Recommendation:** Always populate the allowlist with exact service names to limit the impact and avoid excessive span volume.

## Optional: OTel Java Agent for JVM Runtime UI

New Relic’s JVM Runtime UI expects OpenTelemetry Java agent runtime metrics
(for example `process.runtime.jvm.*`). The Micrometer JVM binder exports `jvm.*`
metrics, which show up in Metrics Explorer but do not fully populate the JVM UI.

If you want the full JVM Runtime page in dev, you can opt in to the OTel Java agent:

```bash
curl -L -o otel-javaagent.jar https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/latest/download/opentelemetry-javaagent.jar
```

```bash
export JAVA_TOOL_OPTIONS="-javaagent:$(pwd)/otel-javaagent.jar"
export OTEL_SERVICE_NAME=orchestrator-svc
export OTEL_EXPORTER_OTLP_ENDPOINT=${NEW_RELIC_OTLP_ENDPOINT:-https://otlp.eu01.nr-data.net:443}
export OTEL_EXPORTER_OTLP_HEADERS=api-key=${NEW_RELIC_LICENSE_KEY}
```

Unset `JAVA_TOOL_OPTIONS` to disable the agent.