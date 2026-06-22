package org.pipelineframework.awaitable.sqs;

import java.util.Objects;
import java.util.Optional;

/**
 * Framework-owned SQS await response envelope.
 */
public record SqsAwaitCompletionEnvelope(
    String tenantId,
    String interactionId,
    String correlationId,
    String resumeToken,
    String idempotencyKey,
    Object responsePayload,
    String actor
) {
    public SqsAwaitCompletionEnvelope {
        Optional<String> normalizedTenantId = normalize(tenantId);
        if (normalizedTenantId.isEmpty()) {
            throw new IllegalArgumentException("tenantId must not be blank");
        }
        tenantId = normalizedTenantId.orElseThrow();
        interactionId = normalize(interactionId).orElse(null);
        correlationId = normalize(correlationId).orElse(null);
        resumeToken = normalize(resumeToken).orElse(null);
        idempotencyKey = normalize(idempotencyKey).orElse(null);
        actor = normalize(actor).orElse(null);
        if (interactionId == null && correlationId == null) {
            throw new IllegalArgumentException("interactionId or correlationId must be supplied");
        }
        responsePayload = Objects.requireNonNull(responsePayload, "responsePayload must not be null");
    }

    private static Optional<String> normalize(String value) {
        if (value == null || value.isBlank()) {
            return Optional.empty();
        }
        return Optional.of(value.trim());
    }
}
