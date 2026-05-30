package org.pipelineframework.awaitable.dto;

import org.pipelineframework.awaitable.AwaitInteractionStatus;

/**
 * Response DTO returned after completion admission.
 *
 * @param interactionId completed interaction identifier
 * @param executionId owning queue-async execution identifier
 * @param stepId authored await step identifier
 * @param status resulting interaction status
 * @param duplicate whether the completion was already admitted
 */
public record AwaitCompletionResponseDto(
    String interactionId,
    String executionId,
    String stepId,
    AwaitInteractionStatus status,
    boolean duplicate
) {
}
