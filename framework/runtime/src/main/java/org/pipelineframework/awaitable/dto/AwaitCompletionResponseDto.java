package org.pipelineframework.awaitable.dto;

import org.pipelineframework.awaitable.AwaitInteractionStatus;

/**
 * Response DTO returned after completion admission.
 */
public record AwaitCompletionResponseDto(
    String interactionId,
    String executionId,
    String stepId,
    AwaitInteractionStatus status,
    boolean duplicate
) {
}
