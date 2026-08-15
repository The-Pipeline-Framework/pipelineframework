# Tracing

Distributed tracing connects a single item across multiple steps and services.

## OpenTelemetry Integration

Enable tracing with standard Quarkus OpenTelemetry settings and export to your collector.

```properties
quarkus.otel.enabled=true
quarkus.otel.exporter.otlp.endpoint=http://otel-collector:4318
quarkus.otel.exporter.otlp.protocol=http/protobuf
```

Enable TPF's tracing policy separately:

```properties
pipeline.telemetry.enabled=true
pipeline.telemetry.tracing.enabled=true
```

The Quarkus OpenTelemetry extension and trace capability must be present when the deployable is
built. The TPF properties cannot retrofit tracing into an artifact that lacks that capability.

When OTLP tracing is enabled, spans and span events are emitted to the collector in real time.
That is the live topology source for Tempo. Prometheus does not ingest or poll spans.

## Sampling

For high-volume pipelines, use sampling to control overhead while keeping representative traces.
Client spans can be forced for selected services via:

```properties
pipeline.telemetry.tracing.client-spans.force=true
pipeline.telemetry.tracing.client-spans.allowlist=ProcessCsvPaymentsInputService
```

## Application Spans

Add application spans around domain-specific external calls or expensive transformations to make
hotspots visible:

```java
Span span = tracer.spanBuilder("payment.validate").startSpan();
try (Scope ignored = span.makeCurrent()) {
    // validation work
} finally {
    span.end();
}
```

Do not use an interceptor or application span to infer a framework semantic transition such as
durable Await admission or live handoff. TPF emits those facts explicitly at the runtime seam that
owns them.

## Context Propagation

For streaming pipelines, context must cover actual asynchronous execution from subscription to
completion, failure, or cancellation; instrumenting only publisher construction is not enough.
Use MDC for logs and OpenTelemetry context for traces.

TPF preserves parent/child context while work remains live. A deliberately durable or asynchronous
boundary may begin a separate physical trace only when the downstream span retains an explicit
OpenTelemetry span link to the originating execution journey. Correlation attributes are supplemental
diagnostics, not a substitute for parentage or a span link. A downstream Await completion or
continuation span that is neither a valid child nor deliberately linked is an accidental rootless
span and should be treated as a conformance failure.

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

TPF runtime tracing uses these primary span names:

- `tpf.pipeline.run` spans
- `tpf.step` spans

Failed run and step spans record the exception and use error status. Cancellation closes the span
exactly once without classifying cancellation as an error. When replay/per-item instrumentation is
enabled, replay-scoped step spans also carry lifecycle events such as `tpf.step.start`,
`tpf.step.emit`, `tpf.step.retry`, `tpf.step.success`, `tpf.step.error`, and
`tpf.step.cancelled`. Do not build a tracing conformance check that assumes those replay-scoped
events exist when replay is disabled.

Await, transition-worker, transport, connector, and terminal-publication spans add the semantic
stage detail needed by their boundary. Use the canonical dashboards and conformance journeys as the
contract rather than reconstructing a journey from Java method names.

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
