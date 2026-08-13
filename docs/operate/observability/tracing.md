# Tracing

Distributed tracing connects a single item across multiple steps and services.

## OpenTelemetry Integration

Enable tracing with standard Quarkus OpenTelemetry settings and export to your collector.

```properties
quarkus.otel.enabled=true
quarkus.otel.exporter.otlp.endpoint=http://otel-collector:4318
quarkus.otel.exporter.otlp.protocol=http/protobuf
```

When OTLP tracing is enabled, spans and span events are emitted to the collector in real time.
That is the live topology source for Tempo. Prometheus does not ingest or poll spans.

## Sampling

For high-volume pipelines, use sampling to control overhead while keeping representative traces.
Client spans can be forced for selected services via:

```properties
pipeline.telemetry.tracing.client-spans.force=true
pipeline.telemetry.tracing.client-spans.allowlist=ProcessCsvPaymentsInputService
```

## Custom Spans

Add spans around external calls or expensive transformations to make hotspots visible:

```java
try (Scope ignored = tracer.spanBuilder("payment.validate").startScopedSpan()) {
    // validation work
}
```

## Context Propagation

For streaming pipelines, context must cover actual asynchronous execution from subscription to
completion, failure, or cancellation; instrumenting only publisher construction is not enough.
Use MDC for logs and OpenTelemetry context for traces.

TPF preserves parent/child context while work remains live. A deliberately durable or asynchronous
boundary may begin a separate physical trace only when the downstream span retains an explicit
OpenTelemetry link or correlation to the originating execution journey. A downstream Await
completion or continuation span that is neither a valid child nor deliberately linked is an
accidental rootless span and should be treated as a conformance failure.

For the representative CSV Payments journey, trace proof follows transition-worker dispatch,
Await interaction creation, provider dispatch, completion admission, live handoff, scalar
continuation, and terminal publication. The Tempo dashboard includes an explicit diagnostic for
unlinked/rootless Await completions; it is not a substitute for checking the journey spans.

## Tracing Strategy

1. Use meaningful span names (step + action)
2. Capture failures as span events
3. Avoid logging sensitive payloads in span attributes
4. Record step class and pipeline run attributes (`tpf.*`)
5. Enable per-item spans only when needed (`pipeline.telemetry.tracing.per-item=true`)

TPF runtime tracing also emits:

- `tpf.pipeline.run` spans
- `tpf.step` spans
- `tpf.step.start`
- `tpf.step.emit`
- `tpf.step.retry`
- `tpf.step.success`
- `tpf.step.error`
- `tpf.step.cancelled`

For replay JSON versus live Tempo usage, see
[Replay & Live Topology](/operate/observability/replay).

## AWS Lambda and X-Ray

For Lambda deployments that use AWS X-Ray, add Quarkus Lambda X-Ray support:

```xml
<dependency>
  <groupId>io.quarkus</groupId>
  <artifactId>quarkus-amazon-lambda-xray</artifactId>
</dependency>
```

For SnapStart/tiered compilation operational guidance, see:
- [AWS Lambda SnapStart (Operate)](/operate/aws-lambda-snapstart)
