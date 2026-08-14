package org.pipelineframework.telemetry;

import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Tracer;

/** SDK infrastructure only; semantic telemetry must depend on this abstraction rather than JVM globals. */
public interface TelemetryRuntime {
    Meter meter(String instrumentationScope);

    Tracer tracer(String instrumentationScope);

    void flush();
}
