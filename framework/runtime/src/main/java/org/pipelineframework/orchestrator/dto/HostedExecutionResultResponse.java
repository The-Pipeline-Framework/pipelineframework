package org.pipelineframework.orchestrator.dto;

import org.pipelineframework.orchestrator.SerializedTransitionPayload;

/**
 * Generic hosted-control-plane execution result response.
 *
 * @param status latest execution status
 * @param resultPayload serialized terminal output payload when available
 */
public record HostedExecutionResultResponse(
    ExecutionStatusDto status,
    SerializedTransitionPayload resultPayload
) {
}
