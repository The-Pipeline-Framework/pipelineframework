package org.pipelineframework.orchestrator;

import java.util.Objects;

/**
 * Queue-dispatched work item for progressing one execution.
 *
 * @param tenantId tenant identifier
 * @param executionId execution identifier
 * @param target typed durable work target within the existing coordinator substrate
 */
public record ExecutionWorkItem(String tenantId, String executionId, CoordinatorWorkTarget target) {
    public ExecutionWorkItem(String tenantId, String executionId) {
        this(tenantId, executionId, CoordinatorWorkTarget.execution(requireExecutionId(executionId)));
    }

    public ExecutionWorkItem {
        Objects.requireNonNull(tenantId, "ExecutionWorkItem.tenantId must not be null");
        Objects.requireNonNull(executionId, "ExecutionWorkItem.executionId must not be null");
        target = target == null ? CoordinatorWorkTarget.execution(executionId) : target;
        if (target.execution() && !executionId.equals(target.targetId())) {
            throw new IllegalArgumentException("execution work target must match execution id");
        }
    }

    public static ExecutionWorkItem awaitContinuation(String tenantId, String executionId, String interactionId) {
        if (interactionId == null || interactionId.isBlank()) {
            throw new IllegalArgumentException("await interaction id must not be blank");
        }
        return new ExecutionWorkItem(tenantId, executionId, CoordinatorWorkTarget.awaitInteraction(interactionId));
    }

    public static ExecutionWorkItem streamRegion(String tenantId, String executionId, String regionId) {
        if (regionId == null || regionId.isBlank()) {
            throw new IllegalArgumentException("stream region id must not be blank");
        }
        return new ExecutionWorkItem(tenantId, executionId, CoordinatorWorkTarget.streamRegion(regionId));
    }

    public boolean awaitContinuation() {
        return target.awaitInteraction();
    }

    public String awaitInteractionId() {
        if (!awaitContinuation()) {
            throw new IllegalStateException("execution work item does not target an await interaction");
        }
        return target.targetId();
    }

    public boolean streamRegion() {
        return target.streamRegion();
    }

    public String streamRegionId() {
        if (!streamRegion()) {
            throw new IllegalStateException("execution work item does not target a stream region");
        }
        return target.targetId();
    }

    private static String requireExecutionId(String executionId) {
        return Objects.requireNonNull(executionId, "ExecutionWorkItem.executionId must not be null");
    }
}
