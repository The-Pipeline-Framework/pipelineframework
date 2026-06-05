package org.pipelineframework;

import java.util.Optional;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.orchestrator.ExecutionRecord;

public interface AwaitItemContinuationHandler {

    Uni<Void> continueAwaitItem(
        AwaitInteractionRecord record,
        AwaitUnitRecord unit,
        int nextStepIndex,
        Optional<ExecutionRecord<Object, Object>> parent,
        long nowEpochMs);

    Uni<Void> releaseAwaitParentIfReady(
        ExecutionRecord<Object, Object> parent,
        AwaitUnitRecord unit,
        int nextStepIndex,
        long nowEpochMs);
}
