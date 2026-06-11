package org.pipelineframework.invocation;

import java.util.Objects;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;

final class TransportBoundaryDiagnostics {
    private static final AttributeKey<String> PROTOCOL = AttributeKey.stringKey("tpf.boundary.protocol");
    private static final AttributeKey<String> TARGET = AttributeKey.stringKey("tpf.boundary.target");
    private static final AttributeKey<String> OUTCOME = AttributeKey.stringKey("tpf.boundary.outcome");
    private static final AttributeKey<String> FAILURE_CATEGORY =
        AttributeKey.stringKey("tpf.boundary.failure.category");
    private final TransportBoundaryFailureClassifier failureClassifier;
    private final LongCounter invocations;
    private final DoubleHistogram duration;

    TransportBoundaryDiagnostics() {
        this(new TransportBoundaryFailureClassifier(), GlobalOpenTelemetry.getMeter("org.pipelineframework.invocation"));
    }

    TransportBoundaryDiagnostics(TransportBoundaryFailureClassifier failureClassifier, Meter meter) {
        this.failureClassifier = Objects.requireNonNull(failureClassifier, "failureClassifier must not be null");
        Meter localMeter = Objects.requireNonNull(meter, "meter must not be null");
        this.invocations = localMeter.counterBuilder("tpf.transport.boundary.invocations").build();
        this.duration = localMeter.histogramBuilder("tpf.transport.boundary.duration").setUnit("ms").build();
    }

    void record(
        TransportBoundaryDescriptor descriptor,
        long durationNanos,
        Throwable failure,
        boolean cancelled
    ) {
        TransportBoundaryFailureCategory category = failureClassifier.classify(failure, cancelled);
        Attributes attributes = Attributes.builder()
            .put(PROTOCOL, descriptor.protocol())
            .put(TARGET, descriptor.target())
            .put(OUTCOME, outcome(category))
            .put(FAILURE_CATEGORY, category.metricValue())
            .build();
        invocations.add(1, attributes);
        duration.record(durationNanos / 1_000_000.0, attributes);
    }

    private String outcome(TransportBoundaryFailureCategory category) {
        if (category == TransportBoundaryFailureCategory.CANCELLED) {
            return "cancelled";
        }
        return category == TransportBoundaryFailureCategory.NONE ? "completed" : "failed";
    }
}
