package org.pipelineframework.awaitable;

/**
 * Command representing a correlated external completion.
 *
 * @param tenantId tenant that owns the interaction
 * @param interactionId interaction identifier to complete
 * @param correlationId external correlation identifier to complete
 * @param resumeToken signed resume token for webhook-style completion
 * @param idempotencyKey stable completion idempotency key
 * @param responsePayload completion payload
 * @param actor actor submitting the completion
 * @param nowEpochMs command time in epoch milliseconds
 */
public record AwaitCompletionCommand(
    String tenantId,
    String interactionId,
    String correlationId,
    String resumeToken,
    String idempotencyKey,
    Object responsePayload,
    String actor,
    long nowEpochMs
) {
    public AwaitCompletionCommand(
        String tenantId,
        String interactionId,
        String correlationId,
        String idempotencyKey,
        Object responsePayload,
        String actor,
        long nowEpochMs
    ) {
        this(tenantId, interactionId, correlationId, null, idempotencyKey, responsePayload, actor, nowEpochMs);
    }

    public AwaitCompletionCommand {
        if (tenantId == null || tenantId.isBlank()) {
            throw new IllegalArgumentException("tenantId must not be blank");
        }
        interactionId = normalizeOptionalIdentifier(interactionId);
        correlationId = normalizeOptionalIdentifier(correlationId);
        resumeToken = normalizeOptionalIdentifier(resumeToken);
        if (interactionId == null && correlationId == null) {
            throw new IllegalArgumentException("interactionId or correlationId must be supplied");
        }
        if (nowEpochMs <= 0) {
            nowEpochMs = System.currentTimeMillis();
        }
    }

    private static String normalizeOptionalIdentifier(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return value.trim();
    }
}
