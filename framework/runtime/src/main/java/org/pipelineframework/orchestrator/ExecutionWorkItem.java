package org.pipelineframework.orchestrator;

/**
 * Queue-dispatched work item for progressing one execution.
 *
 * @param tenantId tenant identifier
 * @param executionId execution identifier
 */
public record ExecutionWorkItem(String tenantId, String executionId) {
}
