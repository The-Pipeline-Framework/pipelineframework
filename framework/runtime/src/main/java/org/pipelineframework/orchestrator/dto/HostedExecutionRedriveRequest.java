package org.pipelineframework.orchestrator.dto;

/**
 * Admin request for operator-controlled execution re-drive.
 *
 * @param expectedVersion optional optimistic version expected by the operator
 * @param reason optional operator reason for audit/log context
 * @param allowFailed whether terminal FAILED executions may be re-driven
 */
public record HostedExecutionRedriveRequest(
    Long expectedVersion,
    String reason,
    boolean allowFailed
) {
}
