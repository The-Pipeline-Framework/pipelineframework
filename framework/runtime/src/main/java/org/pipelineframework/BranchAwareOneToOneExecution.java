package org.pipelineframework;

import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.branching.BranchExecutionTracker;
import org.pipelineframework.branching.StepBranchingDescriptor;
import org.pipelineframework.telemetry.PipelineStepTelemetry;

/** Applies one-to-one branch routing and observer provenance consistently for one item. */
public final class BranchAwareOneToOneExecution {
    private final Optional<StepBranchingDescriptor> descriptor;
    private final BranchExecutionTracker tracker;
    private final PipelineStepTelemetry telemetry;

    public BranchAwareOneToOneExecution(
        Optional<StepBranchingDescriptor> descriptor,
        BranchExecutionTracker tracker,
        PipelineStepTelemetry telemetry
    ) {
        this.descriptor = Objects.requireNonNull(descriptor, "descriptor");
        this.tracker = tracker;
        this.telemetry = telemetry;
    }

    @SuppressWarnings("unchecked")
    public <I, O> Uni<O> execute(
        Class<?> stepClass,
        I item,
        boolean perItemOperation,
        BiFunction<I, PipelineStepTelemetry.ReplayScope, Uni<O>> invocation
    ) {
        if (descriptor.filter(candidate -> candidate.afterStepObserver()
            && tracker.wasLastStepSkipped(item)).isPresent()) {
            tracker.recordSkipped(item);
            return PipelineStepExecutor.skippedUni(stepClass, item, descriptor, telemetry);
        }
        I applicable = descriptor.isEmpty() ? item : (I) descriptor.orElseThrow().applicableItem(item);
        if (applicable == null) {
            tracker.recordSkipped(item);
            return PipelineStepExecutor.skippedUni(stepClass, item, descriptor, telemetry);
        }
        PipelineStepTelemetry.ReplayScope replayScope =
            telemetry.beginReplayStep(stepClass, perItemOperation, applicable);
        Uni<O> result = invocation.apply(applicable, replayScope)
            .onItem().invoke(output -> {
                tracker.recordExecuted(output);
                telemetry.recordOutput(replayScope, output);
            });
        return telemetry.instrument(stepClass, result, perItemOperation, replayScope);
    }
}
