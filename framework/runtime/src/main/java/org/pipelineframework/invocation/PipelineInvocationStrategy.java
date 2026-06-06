package org.pipelineframework.invocation;

import java.util.function.LongConsumer;

import org.pipelineframework.awaitable.AwaitExecutionContext;
import org.pipelineframework.context.PipelineContext;

interface PipelineInvocationStrategy {

    PipelineContext pipelineContext();

    AwaitExecutionContext awaitContext();

    void recordDuration(long startNanos);

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
    public void recordDuration(long startNanos) {
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
    public void recordDuration(long startNanos) {
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
