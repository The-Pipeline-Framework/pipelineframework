package org.pipelineframework.awaitable.dto;

import java.util.Map;

import org.pipelineframework.awaitable.AwaitInteractionStatus;

/**
 * Query projection for pending await interactions.
 *
 * @param interactionId externally visible interaction identifier
 * @param correlationId external correlation identifier
 * @param executionId owning queue-async execution identifier
 * @param stepId authored await step identifier
 * @param stepIndex authored await step index
 * @param outputType expected completion payload type
 * @param status current interaction status
 * @param requestPayload payload dispatched to the external actor
 * @param assignee optional human assignee
 * @param group optional human group
 * @param transportType await transport type
 * @param transportMetadata transport-specific metadata
 * @param deadlineEpochMs completion deadline in epoch milliseconds
 * @param createdAtEpochMs creation time in epoch milliseconds
 * @param updatedAtEpochMs last update time in epoch milliseconds
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
