package org.pipelineframework.awaitable.dto;

/**
 * REST/gRPC-neutral request DTO for completing an await interaction.
 */
public record AwaitCompletionRequestDto(
    String interactionId,
    String correlationId,
    String idempotencyKey,
    Object responsePayload,
    String actor
) {
}
