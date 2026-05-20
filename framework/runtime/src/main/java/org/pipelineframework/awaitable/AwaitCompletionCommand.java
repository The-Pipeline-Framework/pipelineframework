package org.pipelineframework.awaitable;

/**
 * Command representing a correlated external completion.
 */
public record AwaitCompletionCommand(
    String tenantId,
    String interactionId,
    String correlationId,
    String idempotencyKey,
    Object responsePayload,
    String actor,
    long nowEpochMs
) {
    public AwaitCompletionCommand {
        if (tenantId == null || tenantId.isBlank()) {
            throw new IllegalArgumentException("tenantId must not be blank");
        }
        interactionId = normalizeOptionalIdentifier(interactionId);
        correlationId = normalizeOptionalIdentifier(correlationId);
        if (interactionId == null && correlationId == null) {
            throw new IllegalArgumentException("interactionId or correlationId must be supplied");
        }
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            throw new IllegalArgumentException("idempotencyKey must not be blank");
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
