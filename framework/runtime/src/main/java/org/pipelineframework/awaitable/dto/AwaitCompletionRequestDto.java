package org.pipelineframework.awaitable.dto;

/**
 * REST/gRPC-neutral request DTO for completing an await interaction.
 *
 * @param interactionId interaction identifier to complete
 * @param correlationId external correlation identifier to complete
 * @param resumeToken signed resume token for webhook-style completion
 * @param idempotencyKey stable completion idempotency key
 * @param responsePayload completion payload
 * @param actor actor submitting the completion
 */
public record AwaitCompletionRequestDto(
    String interactionId,
    String correlationId,
    String resumeToken,
    String idempotencyKey,
    Object responsePayload,
    String actor
) {
}
