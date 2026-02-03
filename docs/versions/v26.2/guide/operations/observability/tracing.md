---
search: false
---

# Tracing

Distributed tracing connects a single item across multiple steps and services.

## OpenTelemetry Integration

Enable tracing with standard Quarkus OpenTelemetry settings and export to your collector.

```properties
quarkus.otel.enabled=true
quarkus.otel.exporter.otlp.endpoint=http://otel-collector:4318
quarkus.otel.exporter.otlp.protocol=http/protobuf
```

## Sampling

For high-volume pipelines, use sampling to control overhead while keeping representative traces.
Client spans can be forced for selected services via:

```properties
pipeline.telemetry.tracing.client-spans.force=true
pipeline.telemetry.tracing.client-spans.allowlist=ProcessCsvPaymentsInputService,ProcessCsvPaymentsOutputFileService
```

## Custom Spans

Add spans around external calls or expensive transformations to make hotspots visible:

```java
try (Scope ignored = tracer.spanBuilder("payment.validate").startScopedSpan()) {
    // validation work
}
```

## Context Propagation

For streaming pipelines, ensure context is carried across async boundaries and emitted in downstream services. Use MDC for logs and OpenTelemetry context for traces.

## Tracing Strategy

1. Use meaningful span names (step + action)
2. Capture failures as span events
3. Avoid logging sensitive payloads in span attributes
4. Record step class and pipeline run attributes (`tpf.*`)
5. Enable per-item spans only when needed (`pipeline.telemetry.tracing.per-item=true`)
