package org.pipelineframework.awaitable.dto;

import java.util.Map;

import org.pipelineframework.awaitable.AwaitInteractionStatus;

/**
 * Query projection for pending await interactions.
 */
public record AwaitInteractionDto(
    String interactionId,
    String correlationId,
    String executionId,
    String stepId,
    int stepIndex,
    String outputType,
    AwaitInteractionStatus status,
    Object requestPayload,
    String assignee,
    String group,
    String transportType,
    Map<String, Object> transportMetadata,
    long deadlineEpochMs,
    long createdAtEpochMs,
    long updatedAtEpochMs
) {
}
