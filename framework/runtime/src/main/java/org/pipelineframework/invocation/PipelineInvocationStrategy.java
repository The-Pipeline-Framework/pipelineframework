package org.pipelineframework.invocation;

import java.util.Objects;
import java.util.function.LongConsumer;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import org.pipelineframework.awaitable.AwaitExecutionContext;
import org.pipelineframework.context.PipelineContext;

interface PipelineInvocationStrategy {

    PipelineContext pipelineContext();

    AwaitExecutionContext awaitContext();

    void recordTermination(long startNanos, Throwable failure, boolean cancelled);

    String nullUniMessage();

    String nullMultiMessage();
}

final class StepInvocationStrategy implements PipelineInvocationStrategy {
    private final PipelineContext pipelineContext;
    private final AwaitExecutionContext awaitContext;

    StepInvocationStrategy(PipelineContext pipelineContext, AwaitExecutionContext awaitContext) {
        this.pipelineContext = pipelineContext;
        this.awaitContext = awaitContext;
    }

    @Override
    public PipelineContext pipelineContext() {
        return pipelineContext;
    }

    @Override
    public AwaitExecutionContext awaitContext() {
        return awaitContext;
    }

    @Override
    public void recordTermination(long startNanos, Throwable failure, boolean cancelled) {
        // Step duration is already recorded by PipelineTelemetry around each cardinality path.
    }

    @Override
    public String nullUniMessage() {
        return "Step invocation returned null Uni";
    }

    @Override
    public String nullMultiMessage() {
        return "Step invocation returned null Multi";
    }
}

final class TransitionWorkerInvocationStrategy implements PipelineInvocationStrategy {
    private static final LongConsumer NO_DURATION_RECORDER = ignored -> {
    };

    private final LongConsumer durationRecorder;

    TransitionWorkerInvocationStrategy(LongConsumer durationRecorder) {
        this.durationRecorder = durationRecorder == null ? NO_DURATION_RECORDER : durationRecorder;
    }

    @Override
    public PipelineContext pipelineContext() {
        return null;
    }

    @Override
    public AwaitExecutionContext awaitContext() {
        return null;
    }

    @Override
    public void recordTermination(long startNanos, Throwable failure, boolean cancelled) {
        durationRecorder.accept(startNanos);
    }

    @Override
    public String nullUniMessage() {
        return "Transition worker invocation returned null Uni";
    }

    @Override
    public String nullMultiMessage() {
        return "Transition worker invocation returned null Multi";
    }
}

final class TransportBoundaryInvocationStrategy implements PipelineInvocationStrategy {
    private final TransportBoundaryDescriptor descriptor;

    TransportBoundaryInvocationStrategy(TransportBoundaryInvocation boundary) {
        if (boundary == null) {
            throw new NullPointerException("boundary must not be null");
        }
        this.descriptor = Objects.requireNonNull(boundary.transportBoundary(), "transport boundary must not be null");
    }

    @Override
    public PipelineContext pipelineContext() {
        return null;
    }

    @Override
    public AwaitExecutionContext awaitContext() {
        return null;
    }

    @Override
    public void recordTermination(long startNanos, Throwable failure, boolean cancelled) {
        TransportBoundaryDiagnostics.record(descriptor, System.nanoTime() - startNanos, failure, cancelled);
    }

    @Override
    public String nullUniMessage() {
        return "Transport boundary invocation returned null Uni";
    }

    @Override
    public String nullMultiMessage() {
        return "Transport boundary invocation returned null Multi";
    }
}

final class TransportBoundaryDiagnostics {
    private static final AttributeKey<String> PROTOCOL = AttributeKey.stringKey("tpf.boundary.protocol");
    private static final AttributeKey<String> TARGET = AttributeKey.stringKey("tpf.boundary.target");
    private static final AttributeKey<String> OUTCOME = AttributeKey.stringKey("tpf.boundary.outcome");
    private static volatile Meter meter;
    private static volatile LongCounter invocations;
    private static volatile DoubleHistogram duration;

    private TransportBoundaryDiagnostics() {
    }

    static void record(
        TransportBoundaryDescriptor descriptor,
        long durationNanos,
        Throwable failure,
        boolean cancelled
    ) {
        ensureInitialized();
        Attributes attributes = Attributes.builder()
            .put(PROTOCOL, descriptor.protocol())
            .put(TARGET, descriptor.target())
            .put(OUTCOME, outcome(failure, cancelled))
            .build();
        invocations.add(1, attributes);
        duration.record(durationNanos / 1_000_000.0, attributes);
    }

    private static String outcome(Throwable failure, boolean cancelled) {
        if (cancelled) {
            return "cancelled";
        }
        return failure == null ? "completed" : "failed";
    }

    private static void ensureInitialized() {
        if (meter != null) {
            return;
        }
        synchronized (TransportBoundaryDiagnostics.class) {
            if (meter != null) {
                return;
            }
            Meter localMeter = GlobalOpenTelemetry.getMeter("org.pipelineframework.invocation");
            invocations = localMeter.counterBuilder("tpf.transport.boundary.invocations").build();
            duration = localMeter.histogramBuilder("tpf.transport.boundary.duration").setUnit("ms").build();
            meter = localMeter;
        }
    }
}
