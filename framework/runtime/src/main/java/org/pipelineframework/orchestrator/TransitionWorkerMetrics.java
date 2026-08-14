package org.pipelineframework.orchestrator;

import java.util.concurrent.atomic.AtomicLong;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.pipelineframework.telemetry.NoopTelemetryRuntime;
import org.pipelineframework.telemetry.TelemetryRuntime;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import org.pipelineframework.telemetry.TelemetrySdkAttributes;
import org.pipelineframework.telemetry.derivation.TransitionTelemetryDerivation;

/**
 * Lightweight queue-async transition worker metrics.
 */
@ApplicationScoped
final class TransitionWorkerMetrics {

    private static final AtomicLong ACTIVE_TRANSITIONS = new AtomicLong();
    private final LongCounter saturatedCounter;
    private final LongCounter dispatchedCounter;
    private final LongCounter outcomeCounter;
    private final DoubleHistogram durationHistogram;

    @Inject
    TransitionWorkerMetrics(TelemetryRuntime runtime) {
        Meter meter = runtime.meter("org.pipelineframework.orchestrator");
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

    void record(TransitionTelemetryDerivation.MetricSignal signal) {
        var attributes = TelemetrySdkAttributes.from(signal.attributes());
        switch (signal.metric()) {
            case ACTIVE -> ACTIVE_TRANSITIONS.updateAndGet(current ->
                Math.max(0L, current + (long) signal.value()));
            case SATURATED -> saturatedCounter.add((long) signal.value(), attributes);
            case DISPATCHED -> dispatchedCounter.add((long) signal.value(), attributes);
            case OUTCOME -> outcomeCounter.add((long) signal.value(), attributes);
            case DURATION -> durationHistogram.record(signal.value(), attributes);
        }
    }

}
