package org.pipelineframework.orchestrator;

/**
 * Result of create-or-get execution operation.
 *
 * @param record resolved execution record
 * @param duplicate true when an existing execution was reused
 */
public record CreateExecutionResult(ExecutionRecord record, boolean duplicate) {
}
