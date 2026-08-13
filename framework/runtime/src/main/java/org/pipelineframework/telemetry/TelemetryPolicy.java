package org.pipelineframework.telemetry;

import java.time.Duration;
import java.util.Optional;
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
    public static TelemetryPolicy disabled() {
        return new TelemetryPolicy(false, false, false, false, false, false,
            Duration.ofSeconds(30), 10d, 3, RetryAmplificationGuardMode.FAIL_FAST);
    }

    public static TelemetryPolicy from(PipelineStepConfig config, boolean replayTopologyAvailable) {
        PipelineStepConfig.TelemetryConfig telemetry = config.telemetry();
        Optional<PipelineStepConfig.RetryAmplificationGuardConfig> guard = Optional.ofNullable(config.killSwitch())
            .map(PipelineStepConfig.KillSwitchConfig::retryAmplification);
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
        Duration window = guard.map(PipelineStepConfig.RetryAmplificationGuardConfig::window)
            .filter(value -> !value.isZero() && !value.isNegative())
            .orElse(Duration.ofSeconds(30));
        double threshold = guard.map(PipelineStepConfig.RetryAmplificationGuardConfig::inflightSlopeThreshold)
            .filter(value -> value > 0d)
            .orElse(10d);
        int samples = guard.map(PipelineStepConfig.RetryAmplificationGuardConfig::sustainSamples)
            .filter(value -> value > 0)
            .orElse(3);
        RetryAmplificationGuardMode mode = guard.map(PipelineStepConfig.RetryAmplificationGuardConfig::mode)
            .orElse(RetryAmplificationGuardMode.FAIL_FAST);
        boolean guardEnabled = guard.map(PipelineStepConfig.RetryAmplificationGuardConfig::enabled)
            .filter(Boolean.TRUE::equals)
            .isPresent();
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
