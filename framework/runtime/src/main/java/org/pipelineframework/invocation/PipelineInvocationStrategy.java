package org.pipelineframework.invocation;

import java.util.Objects;
import java.util.function.LongConsumer;

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

    private final LongConsumer durationNanosRecorder;

    TransitionWorkerInvocationStrategy(LongConsumer durationNanosRecorder) {
        this.durationNanosRecorder = durationNanosRecorder == null ? NO_DURATION_RECORDER : durationNanosRecorder;
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
        durationNanosRecorder.accept(System.nanoTime() - startNanos);
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
    private final TransportBoundaryDiagnostics diagnostics;

    TransportBoundaryInvocationStrategy(TransportBoundaryInvocation boundary, TransportBoundaryDiagnostics diagnostics) {
        if (boundary == null) {
            throw new NullPointerException("boundary must not be null");
        }
        this.descriptor = Objects.requireNonNull(boundary.transportBoundary(), "transport boundary must not be null");
        this.diagnostics = Objects.requireNonNull(diagnostics, "diagnostics must not be null");
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
        diagnostics.record(descriptor, System.nanoTime() - startNanos, failure, cancelled);
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
