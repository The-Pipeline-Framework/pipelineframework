package org.pipelineframework.orchestrator.dto;

import org.pipelineframework.orchestrator.ExecutionStatus;

/**
 * Status payload for async execution polling.
 *
 * @param executionId execution identifier
 * @param status execution status
 * @param stepIndex current step index
 * @param attempt current attempt
 * @param version record version
 * @param nextDueAtEpochMs next due timestamp
 * @param updatedAtEpochMs update timestamp
 * @param errorCode optional error code
 * @param errorMessage optional error message
 */
public record ExecutionStatusDto(
    String executionId,
    ExecutionStatus status,
    int stepIndex,
    int attempt,
    long version,
    long nextDueAtEpochMs,
    long updatedAtEpochMs,
    String errorCode,
    String errorMessage
) {
}
