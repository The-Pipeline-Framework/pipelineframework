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

    /**
     * Executes one item continuation without creating a child execution or releasing an itemized
     * parent. Implementations that support the durable incremental path return the scalar suffix
     * output for storage on the interaction row.
     */
    default Uni<Object> continueDurableAwaitItem(
        AwaitInteractionRecord record,
        AwaitUnitRecord unit,
        int nextStepIndex,
        Optional<ExecutionRecord<Object, Object>> parent,
        long nowEpochMs) {
        return Uni.createFrom().failure(new UnsupportedOperationException(
            "Durable scalar await continuation execution is not configured"));
    }

    Uni<Void> releaseAwaitParentIfReady(
        ExecutionRecord<Object, Object> parent,
        AwaitUnitRecord unit,
        int nextStepIndex,
        long nowEpochMs);
}
