package org.pipelineframework.orchestrator.dto;

import org.pipelineframework.orchestrator.ExecutionRedriveIntent;

/**
 * Admin request for operator-controlled execution re-drive.
 *
 * @param expectedVersion optional optimistic version expected by the operator
 * @param reason optional operator reason for audit/log context
 * @param allowFailed whether terminal FAILED executions may be re-driven
 * @param intent explicit redrive intent; omitted requests remain ordinary replay
 */
public record HostedExecutionRedriveRequest(
    Long expectedVersion,
    String reason,
    boolean allowFailed,
    ExecutionRedriveIntent intent
) {
    public HostedExecutionRedriveRequest {
        intent = intent == null ? ExecutionRedriveIntent.REPLAY : intent;
    }

    public HostedExecutionRedriveRequest(Long expectedVersion, String reason, boolean allowFailed) {
        this(expectedVersion, reason, allowFailed, ExecutionRedriveIntent.REPLAY);
    }
}
