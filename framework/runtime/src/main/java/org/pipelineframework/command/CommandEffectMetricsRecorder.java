package org.pipelineframework.command;

import jakarta.inject.Singleton;

import org.pipelineframework.telemetry.TelemetryCompatibilityAccess;

import java.util.Locale;

import org.pipelineframework.telemetry.TelemetryRuntimes;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;

/**
 * Command effect observability helpers.
 */
@Singleton
final class CommandEffectMetricsRecorder {

    static final String TRANSITION_TOTAL = "tpf.command.effect.transition.total";
    static final String DUPLICATE_TOTAL = "tpf.command.effect.duplicate.total";
    static final String DURATION = "tpf.command.effect.duration";
    static final String ADMISSION_TOTAL = "tpf.command.effect.admission.total";

    private final AttributeKey<String> COMMAND = AttributeKey.stringKey("tpf.command");
    private final AttributeKey<String> COMMAND_STEP = AttributeKey.stringKey("tpf.command.step");
    private final AttributeKey<String> COMMAND_STATUS = AttributeKey.stringKey("tpf.command.status");
    private final AttributeKey<String> DUPLICATE_POLICY = AttributeKey.stringKey("tpf.command.duplicate_policy");
    private final AttributeKey<String> DUPLICATE_RESULT = AttributeKey.stringKey("tpf.command.duplicate_result");
    private final AttributeKey<String> ADMISSION = AttributeKey.stringKey("tpf.command.admission");

    CommandEffectMetricsRecorder() {
    }

    public long startNanos() {
        return System.nanoTime();
    }

    public void recordTransition(CommandDescriptor descriptor, CommandEffectStatus status) {
        if (descriptor == null || status == null) {
            return;
        }
        transitionCounter().add(1, transitionAttributes(descriptor, status));
    }

    public void recordTerminalTransition(
        CommandDescriptor descriptor,
        CommandEffectStatus status,
        long startNanos
    ) {
        if (descriptor == null || status == null) {
            return;
        }
        Attributes attributes = transitionAttributes(descriptor, status);
        transitionCounter().add(1, attributes);
        durationHistogram().record(elapsedMillis(startNanos), attributes);
    }

    public void recordDuplicate(CommandDescriptor descriptor, String duplicateResult) {
        if (descriptor == null) {
            return;
        }
        duplicateCounter().add(1, Attributes.builder()
            .put(COMMAND, normalize(descriptor.command()))
            .put(COMMAND_STEP, normalize(descriptor.stepId()))
            .put(DUPLICATE_POLICY, descriptor.duplicatePolicy().name())
            .put(DUPLICATE_RESULT, normalize(duplicateResult))
            .build());
    }

    public void recordAdmission(CommandDescriptor descriptor, String admission) {
        if (descriptor == null) {
            return;
        }
        admissionCounter().add(1, Attributes.builder()
            .put(COMMAND, normalize(descriptor.command()))
            .put(COMMAND_STEP, normalize(descriptor.stepId()))
            .put(ADMISSION, normalize(admission))
            .build());
    }

    private Attributes transitionAttributes(CommandDescriptor descriptor, CommandEffectStatus status) {
        return Attributes.builder()
            .put(COMMAND, normalize(descriptor.command()))
            .put(COMMAND_STEP, normalize(descriptor.stepId()))
            .put(COMMAND_STATUS, statusValue(status))
            .build();
    }

    private LongCounter transitionCounter() {
        return TelemetryCompatibilityAccess.metricsRuntime().meter("org.pipelineframework").counterBuilder(TRANSITION_TOTAL)
            .setDescription("Total command effect lifecycle transitions recorded by TPF").setUnit("events").build();
    }

    private LongCounter duplicateCounter() {
        return TelemetryCompatibilityAccess.metricsRuntime().meter("org.pipelineframework").counterBuilder(DUPLICATE_TOTAL)
            .setDescription("Total duplicate command ids resolved by TPF duplicate policy").setUnit("events").build();
    }

    private LongCounter admissionCounter() {
        return TelemetryCompatibilityAccess.metricsRuntime().meter("org.pipelineframework")
            .counterBuilder(ADMISSION_TOTAL)
            .setDescription("Total initial, replay, retry, and reissue Command admissions")
            .setUnit("events")
            .build();
    }

    private DoubleHistogram durationHistogram() {
        return TelemetryCompatibilityAccess.metricsRuntime().meter("org.pipelineframework").histogramBuilder(DURATION)
            .setDescription("Command effect duration from pending record creation to terminal effect state").setUnit("ms").build();
    }

    private String statusValue(CommandEffectStatus status) {
        return status.name().toLowerCase(Locale.ROOT);
    }

    private double elapsedMillis(long startNanos) {
        if (startNanos <= 0) {
            return 0.0d;
        }
        return Math.max(0.0d, (System.nanoTime() - startNanos) / 1_000_000.0d);
    }

    private String normalize(String value) {
        if (value == null || value.isBlank()) {
            return "unknown";
        }
        return value;
    }
}
