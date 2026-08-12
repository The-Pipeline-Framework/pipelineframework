package org.pipelineframework.telemetry;

import java.time.Duration;
import org.pipelineframework.config.PipelineStepConfig;

/**
 * Resolved framework telemetry intent. This value deliberately contains no SDK or exporter state.
 */
public record TelemetryPolicy(
    boolean frameworkEnabled,
    boolean metricsEnabled,
    boolean tracingEnabled,
    boolean replayEnabled,
    boolean perItemSpansEnabled,
    boolean retryAmplificationEnabled,
    Duration retryAmplificationWindow,
    double retryAmplificationInflightSlopeThreshold,
    int retryAmplificationSustainSamples,
    RetryAmplificationGuardMode retryAmplificationMode
) {
    public static TelemetryPolicy from(PipelineStepConfig config, boolean replayTopologyAvailable) {
        PipelineStepConfig.TelemetryConfig telemetry = config.telemetry();
        PipelineStepConfig.RetryAmplificationGuardConfig guard = config.killSwitch() == null
            ? null
            : config.killSwitch().retryAmplification();
        boolean frameworkEnabled = telemetry != null && Boolean.TRUE.equals(telemetry.enabled());
        boolean tracingEnabled = frameworkEnabled && telemetry.tracing() != null
            && Boolean.TRUE.equals(telemetry.tracing().enabled());
        boolean perItemSpansEnabled = tracingEnabled && Boolean.TRUE.equals(telemetry.tracing().perItem());
        boolean metricsEnabled = frameworkEnabled && telemetry.metrics() != null
            && Boolean.TRUE.equals(telemetry.metrics().enabled());
        boolean replayRequested = telemetry != null && telemetry.replay() != null
            && Boolean.TRUE.equals(telemetry.replay().enabled());
        boolean fileExporter = replayRequested && "file".equalsIgnoreCase(telemetry.replay().exporter())
            && telemetry.replay().filePath().filter(path -> !path.isBlank()).isPresent();
        Duration window = guard == null || guard.window() == null || guard.window().isZero() || guard.window().isNegative()
            ? Duration.ofSeconds(30)
            : guard.window();
        double threshold = guard == null || guard.inflightSlopeThreshold() == null || guard.inflightSlopeThreshold() <= 0d
            ? 10d
            : guard.inflightSlopeThreshold();
        int samples = guard == null || guard.sustainSamples() == null || guard.sustainSamples() <= 0
            ? 3
            : guard.sustainSamples();
        RetryAmplificationGuardMode mode = guard == null || guard.mode() == null
            ? RetryAmplificationGuardMode.FAIL_FAST
            : guard.mode();
        boolean guardEnabled = guard != null && Boolean.TRUE.equals(guard.enabled());
        return new TelemetryPolicy(
            frameworkEnabled,
            metricsEnabled,
            tracingEnabled,
            replayRequested && fileExporter && tracingEnabled && perItemSpansEnabled && replayTopologyAvailable,
            perItemSpansEnabled,
            guardEnabled,
            window,
            threshold,
            samples,
            mode);
    }
}
