package org.pipelineframework.orchestrator;

import java.util.Objects;

/**
 * Queue-dispatched work item for progressing one execution.
 *
 * @param tenantId tenant identifier
 * @param executionId execution identifier
 */
public record ExecutionWorkItem(String tenantId, String executionId) {
    public ExecutionWorkItem {
        Objects.requireNonNull(tenantId, "ExecutionWorkItem.tenantId must not be null");
        Objects.requireNonNull(executionId, "ExecutionWorkItem.executionId must not be null");
    }
}
