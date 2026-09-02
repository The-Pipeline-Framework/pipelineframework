package org.pipelineframework.orchestrator.dto;

import org.pipelineframework.orchestrator.ExecutionRedriveIntent;

/**
 * Admin request for operator-controlled execution re-drive.
 *
 * @param expectedVersion optional optimistic version expected by the operator
 * @param reason optional operator reason for audit/log context
 * @param allowFailed whether terminal FAILED executions may be re-driven
 * @param intent explicit redrive intent; omitted requests remain ordinary replay
 * @param targetCommandId exact logical Command effect targeted by {@code REISSUE_COMMAND}
 */
public record HostedExecutionRedriveRequest(
    Long expectedVersion,
    String reason,
    boolean allowFailed,
    ExecutionRedriveIntent intent,
    String targetCommandId
) {
    public HostedExecutionRedriveRequest {
        intent = intent == null ? ExecutionRedriveIntent.REPLAY : intent;
        boolean hasTarget = targetCommandId != null && !targetCommandId.isBlank();
        if (intent == ExecutionRedriveIntent.REISSUE_COMMAND) {
            if (expectedVersion == null) {
                throw new IllegalArgumentException("REISSUE_COMMAND requires expectedVersion");
            }
            if (reason == null || reason.isBlank()) {
                throw new IllegalArgumentException("REISSUE_COMMAND requires a nonblank reason");
            }
            if (!hasTarget) {
                throw new IllegalArgumentException("REISSUE_COMMAND requires targetCommandId");
            }
            if (allowFailed) {
                throw new IllegalArgumentException("REISSUE_COMMAND does not accept allowFailed=true");
            }
            targetCommandId = targetCommandId.trim();
        } else if (hasTarget) {
            throw new IllegalArgumentException("targetCommandId is only valid for REISSUE_COMMAND");
        }
    }

    public HostedExecutionRedriveRequest(Long expectedVersion, String reason, boolean allowFailed) {
        this(expectedVersion, reason, allowFailed, ExecutionRedriveIntent.REPLAY, null);
    }

    public HostedExecutionRedriveRequest(
        Long expectedVersion,
        String reason,
        boolean allowFailed,
        ExecutionRedriveIntent intent
    ) {
        this(expectedVersion, reason, allowFailed, intent, null);
    }
}
