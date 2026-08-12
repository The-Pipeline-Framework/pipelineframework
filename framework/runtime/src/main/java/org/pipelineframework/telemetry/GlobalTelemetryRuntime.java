package org.pipelineframework.telemetry;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Tracer;

/** Production bridge to the OpenTelemetry global registered by Quarkus. */
final class GlobalTelemetryRuntime implements TelemetryRuntime {
    @Override
    public Meter meter(String instrumentationScope) {
        return GlobalOpenTelemetry.getMeter(instrumentationScope);
    }

    @Override
    public Tracer tracer(String instrumentationScope) {
        return GlobalOpenTelemetry.getTracer(instrumentationScope);
    }

    @Override
    public void flush() {
        TelemetryFlush.flushSdk(GlobalOpenTelemetry.get());
    }
}
