package org.pipelineframework.orchestrator;

import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.pipelineframework.telemetry.NoopTelemetryRuntime;
import org.pipelineframework.telemetry.TelemetryRuntime;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;

/**
 * Lightweight queue-async transition worker metrics.
 */
@ApplicationScoped
final class TransitionWorkerMetrics {

    private static final AtomicLong ACTIVE_TRANSITIONS = new AtomicLong();
    private static final AttributeKey<String> OUTCOME = AttributeKey.stringKey("tpf.transition.outcome");

    private final LongCounter saturatedCounter;
    private final LongCounter dispatchedCounter;
    private final LongCounter outcomeCounter;
    private final DoubleHistogram durationHistogram;
    private final Tracer tracer;

    @Inject
    TransitionWorkerMetrics(TelemetryRuntime runtime) {
        Meter meter = runtime.meter("org.pipelineframework.orchestrator");
        tracer = runtime.tracer("org.pipelineframework.orchestrator");
        saturatedCounter = meter.counterBuilder("tpf.orchestrator.transition.saturated").setDescription("Queue-async transition admission saturation count").setUnit("1").build();
        dispatchedCounter = meter.counterBuilder("tpf.orchestrator.transition.dispatched.total").setDescription("Queue-async transitions dispatched to a worker").setUnit("transitions").build();
        outcomeCounter = meter.counterBuilder("tpf.orchestrator.transition.outcome").setDescription("Queue-async transition worker outcomes").setUnit("1").build();
        durationHistogram = meter.histogramBuilder("tpf.orchestrator.transition.duration").setDescription("Queue-async transition execution duration").setUnit("ms").build();
        meter.gaugeBuilder("tpf.orchestrator.transition.active").setDescription("Active queue-async transition admissions").setUnit("1")
            .ofLongs().buildWithCallback(measurement -> measurement.record(ACTIVE_TRANSITIONS.get()));
    }

    static TransitionWorkerMetrics disabled() {
        return new TransitionWorkerMetrics(new NoopTelemetryRuntime());
    }

    void incrementActive() {
        ACTIVE_TRANSITIONS.incrementAndGet();
    }

    void decrementActive() {
        ACTIVE_TRANSITIONS.updateAndGet(current -> Math.max(0L, current - 1L));
    }

    void recordSaturated() {
        saturatedCounter.add(1);
    }

    void recordDispatched() {
        dispatchedCounter.add(1);
        Span span = tracer.spanBuilder("tpf.transition.dispatched").startSpan();
        span.end();
    }

    void recordOutcome(TransitionWorkerOutcome outcome) {
        if (outcome == null) {
            return;
        }
        outcomeCounter.add(1, Attributes.of(OUTCOME, outcome.name().toLowerCase(Locale.ROOT)));
    }

    void recordDuration(long durationNanos) {
        durationHistogram.record(Math.max(0.0, durationNanos / 1_000_000.0));
    }

}
