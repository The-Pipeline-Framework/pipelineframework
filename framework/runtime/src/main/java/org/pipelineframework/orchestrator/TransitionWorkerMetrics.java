package org.pipelineframework.orchestrator;

import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;

/**
 * Lightweight queue-async transition worker metrics.
 */
final class TransitionWorkerMetrics {

    private static final AtomicLong ACTIVE_TRANSITIONS = new AtomicLong();
    private static final AttributeKey<String> OUTCOME = AttributeKey.stringKey("tpf.transition.outcome");

    private static volatile Meter meter;
    private static volatile LongCounter saturatedCounter;
    private static volatile LongCounter outcomeCounter;
    private static volatile DoubleHistogram durationHistogram;

    private TransitionWorkerMetrics() {
    }

    static void incrementActive() {
        ensureInitialized();
        ACTIVE_TRANSITIONS.incrementAndGet();
    }

    static void decrementActive() {
        ACTIVE_TRANSITIONS.updateAndGet(current -> Math.max(0L, current - 1L));
    }

    static void recordSaturated() {
        ensureInitialized();
        saturatedCounter.add(1);
    }

    static void recordOutcome(TransitionWorkerOutcome outcome) {
        if (outcome == null) {
            return;
        }
        ensureInitialized();
        outcomeCounter.add(1, Attributes.of(OUTCOME, outcome.name().toLowerCase(Locale.ROOT)));
    }

    static void recordDuration(long startNanos) {
        ensureInitialized();
        durationHistogram.record(Math.max(0.0, (System.nanoTime() - startNanos) / 1_000_000.0));
    }

    private static void ensureInitialized() {
        if (meter != null) {
            return;
        }
        synchronized (TransitionWorkerMetrics.class) {
            if (meter != null) {
                return;
            }
            Meter localMeter = GlobalOpenTelemetry.getMeter("org.pipelineframework.orchestrator");
            saturatedCounter = localMeter.counterBuilder("tpf.orchestrator.transition.saturated")
                .setDescription("Queue-async transition admission saturation count")
                .setUnit("1")
                .build();
            outcomeCounter = localMeter.counterBuilder("tpf.orchestrator.transition.outcome")
                .setDescription("Queue-async transition worker outcomes")
                .setUnit("1")
                .build();
            durationHistogram = localMeter.histogramBuilder("tpf.orchestrator.transition.duration")
                .setDescription("Queue-async transition execution duration")
                .setUnit("ms")
                .build();
            localMeter.gaugeBuilder("tpf.orchestrator.transition.active")
                .setDescription("Active queue-async transition admissions")
                .setUnit("1")
                .ofLongs()
                .buildWithCallback(measurement -> measurement.record(ACTIVE_TRANSITIONS.get()));
            meter = localMeter;
        }
    }
}
