package org.pipelineframework.telemetry;

/** Build/augmentation capabilities of a deployable artifact, distinct from framework policy and exporters. */
public record TelemetryCapabilities(
    boolean openTelemetryPresent,
    boolean tracingCapable,
    boolean metricsCapable,
    boolean logsCapable,
    boolean micrometerCapable
) {
    public boolean effectiveTracing(TelemetryPolicy policy) {
        return tracingCapable && policy.tracingEnabled();
    }

    public boolean effectiveMetrics(TelemetryPolicy policy) {
        return metricsCapable && policy.metricsEnabled();
    }
}
