package org.pipelineframework.command;

import java.util.Locale;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;

/**
 * Command effect observability helpers.
 */
public final class CommandEffectMetrics {

    static final String TRANSITION_TOTAL = "tpf.command.effect.transition.total";
    static final String DUPLICATE_TOTAL = "tpf.command.effect.duplicate.total";
    static final String DURATION = "tpf.command.effect.duration";

    private static final AttributeKey<String> COMMAND = AttributeKey.stringKey("tpf.command");
    private static final AttributeKey<String> COMMAND_STEP = AttributeKey.stringKey("tpf.command.step");
    private static final AttributeKey<String> COMMAND_STATUS = AttributeKey.stringKey("tpf.command.status");
    private static final AttributeKey<String> DUPLICATE_POLICY = AttributeKey.stringKey("tpf.command.duplicate_policy");
    private static final AttributeKey<String> DUPLICATE_RESULT = AttributeKey.stringKey("tpf.command.duplicate_result");

    private static volatile LongCounter transitionCounter;
    private static volatile LongCounter duplicateCounter;
    private static volatile DoubleHistogram durationHistogram;

    private CommandEffectMetrics() {
    }

    public static long startNanos() {
        return System.nanoTime();
    }

    public static void recordTransition(CommandDescriptor descriptor, CommandEffectStatus status) {
        if (descriptor == null || status == null) {
            return;
        }
        ensureInitialized();
        transitionCounter.add(1, transitionAttributes(descriptor, status));
    }

    public static void recordTerminalTransition(
        CommandDescriptor descriptor,
        CommandEffectStatus status,
        long startNanos
    ) {
        if (descriptor == null || status == null) {
            return;
        }
        ensureInitialized();
        Attributes attributes = transitionAttributes(descriptor, status);
        transitionCounter.add(1, attributes);
        durationHistogram.record(elapsedMillis(startNanos), attributes);
    }

    public static void recordDuplicate(CommandDescriptor descriptor, String duplicateResult) {
        if (descriptor == null) {
            return;
        }
        ensureInitialized();
        duplicateCounter.add(1, Attributes.builder()
            .put(COMMAND, normalize(descriptor.command()))
            .put(COMMAND_STEP, normalize(descriptor.stepId()))
            .put(DUPLICATE_POLICY, descriptor.duplicatePolicy().name())
            .put(DUPLICATE_RESULT, normalize(duplicateResult))
            .build());
    }

    static synchronized void resetForTest() {
        transitionCounter = null;
        duplicateCounter = null;
        durationHistogram = null;
    }

    private static Attributes transitionAttributes(CommandDescriptor descriptor, CommandEffectStatus status) {
        return Attributes.builder()
            .put(COMMAND, normalize(descriptor.command()))
            .put(COMMAND_STEP, normalize(descriptor.stepId()))
            .put(COMMAND_STATUS, statusValue(status))
            .build();
    }

    private static void ensureInitialized() {
        if (transitionCounter != null && duplicateCounter != null && durationHistogram != null) {
            return;
        }
        synchronized (CommandEffectMetrics.class) {
            if (transitionCounter == null) {
                transitionCounter = GlobalOpenTelemetry.getMeter("org.pipelineframework")
                    .counterBuilder(TRANSITION_TOTAL)
                    .setDescription("Total command effect lifecycle transitions recorded by TPF")
                    .setUnit("events")
                    .build();
            }
            if (duplicateCounter == null) {
                duplicateCounter = GlobalOpenTelemetry.getMeter("org.pipelineframework")
                    .counterBuilder(DUPLICATE_TOTAL)
                    .setDescription("Total duplicate command ids resolved by TPF duplicate policy")
                    .setUnit("events")
                    .build();
            }
            if (durationHistogram == null) {
                durationHistogram = GlobalOpenTelemetry.getMeter("org.pipelineframework")
                    .histogramBuilder(DURATION)
                    .setDescription("Command effect duration from pending record creation to terminal effect state")
                    .setUnit("ms")
                    .build();
            }
        }
    }

    private static String statusValue(CommandEffectStatus status) {
        return status.name().toLowerCase(Locale.ROOT);
    }

    private static double elapsedMillis(long startNanos) {
        if (startNanos <= 0) {
            return 0.0d;
        }
        return Math.max(0.0d, (System.nanoTime() - startNanos) / 1_000_000.0d);
    }

    private static String normalize(String value) {
        if (value == null || value.isBlank()) {
            return "unknown";
        }
        return value;
    }
}
