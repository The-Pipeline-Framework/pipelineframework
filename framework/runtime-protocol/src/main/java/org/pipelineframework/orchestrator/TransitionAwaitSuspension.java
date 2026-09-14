package org.pipelineframework.orchestrator;

import java.util.List;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitUnitRecord;

/**
 * Await suspension metadata returned by a worker.
 */
public record TransitionAwaitSuspension(
    String tenantId,
    String executionId,
    String unitId,
    int stepIndex,
    AwaitUnitRecord unit,
    List<AwaitInteractionRecord> interactions
) {
    public TransitionAwaitSuspension(String tenantId, String executionId, String unitId, int stepIndex) {
        this(tenantId, executionId, unitId, stepIndex, null, List.of());
    }

    public TransitionAwaitSuspension {
        if (tenantId == null || tenantId.isBlank()) {
            throw new IllegalArgumentException("tenantId must not be blank");
        }
        if (executionId == null || executionId.isBlank()) {
            throw new IllegalArgumentException("executionId must not be blank");
        }
        if (unitId == null || unitId.isBlank()) {
            throw new IllegalArgumentException("unitId must not be blank");
        }
        if (stepIndex < 0) {
            throw new IllegalArgumentException("stepIndex must be >= 0");
        }
        interactions = interactions == null ? List.of() : List.copyOf(interactions);
    }
}
