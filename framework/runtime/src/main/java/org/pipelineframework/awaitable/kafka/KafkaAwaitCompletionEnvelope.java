package org.pipelineframework.awaitable.kafka;

import java.util.Objects;

/**
 * Framework-owned Kafka await response envelope.
 *
 * @param tenantId required tenant id
 * @param interactionId interaction id when completing by interaction id; one of interactionId or correlationId is required
 * @param correlationId correlation id when completing by correlation id; one of interactionId or correlationId is required
 * @param resumeToken optional signed resume token, required when the interaction was dispatched with signed-token admission
 * @param idempotencyKey optional external completion idempotency key
 * @param responsePayload response snapshot to resume the owning execution with
 * @param actor optional actor or system principal that produced the completion
 */
public record KafkaAwaitCompletionEnvelope(
    String tenantId,
    String interactionId,
    String correlationId,
    String resumeToken,
    String idempotencyKey,
    Object responsePayload,
    String actor
) {
    public KafkaAwaitCompletionEnvelope {
        tenantId = normalize(tenantId);
        if (tenantId == null) {
            throw new IllegalArgumentException("tenantId must not be blank");
        }
        interactionId = normalize(interactionId);
        correlationId = normalize(correlationId);
        resumeToken = normalize(resumeToken);
        idempotencyKey = normalize(idempotencyKey);
        actor = normalize(actor);
        if (interactionId == null && correlationId == null) {
            throw new IllegalArgumentException("interactionId or correlationId must be supplied");
        }
        responsePayload = Objects.requireNonNull(responsePayload, "responsePayload must not be null");
    }

    private static String normalize(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return value.trim();
    }
}
