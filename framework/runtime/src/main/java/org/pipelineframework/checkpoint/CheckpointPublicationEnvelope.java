package org.pipelineframework.checkpoint;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Strict framework-owned checkpoint publication envelope for brokered dispatch.
 *
 * @param publication logical publication name
 * @param tenantId tenant identifier
 * @param idempotencyKey stable handoff idempotency key
 * @param payload serialized checkpoint payload
 * @param publishedAtEpochMs publication timestamp
 */
public record CheckpointPublicationEnvelope(
    String publication,
    String tenantId,
    String idempotencyKey,
    JsonNode payload,
    long publishedAtEpochMs
) {
    public CheckpointPublicationEnvelope {
        publication = normalizeRequired(publication, "publication");
        tenantId = normalizeOptional(tenantId);
        idempotencyKey = normalizeOptional(idempotencyKey);
        if (payload == null || payload.isNull()) {
            throw new IllegalArgumentException("payload must not be null");
        }
        if (publishedAtEpochMs <= 0L) {
            throw new IllegalArgumentException("publishedAtEpochMs must be positive");
        }
    }

    public CheckpointPublicationRequest toRequest() {
        return new CheckpointPublicationRequest(publication, payload);
    }

    private static String normalizeRequired(String value, String field) {
        String normalized = normalizeOptional(value);
        if (normalized == null) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return normalized;
    }

    private static String normalizeOptional(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
