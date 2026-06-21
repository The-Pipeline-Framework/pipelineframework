package org.pipelineframework.objectpublish;

import java.util.Map;

import org.pipelineframework.config.boundary.PipelineObjectPublishConfig;

/**
 * Provider-neutral request to open a streaming object write session.
 */
public record ObjectWriteOpenRequest(
    String targetName,
    PipelineObjectPublishConfig target,
    String objectKey,
    String contentType,
    Map<String, String> metadata,
    String idempotencyKey
) {
    public ObjectWriteOpenRequest {
        targetName = normalize(targetName);
        objectKey = normalize(objectKey);
        contentType = normalize(contentType);
        idempotencyKey = normalize(idempotencyKey);
        metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
        if (targetName == null) {
            throw new IllegalArgumentException("object write targetName must not be blank");
        }
        if (target == null) {
            throw new IllegalArgumentException("object write target must not be null");
        }
        if (objectKey == null) {
            throw new IllegalArgumentException("object write key must not be blank");
        }
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null) {
                throw new IllegalArgumentException("object write metadata must not contain null keys or values");
            }
        }
    }

    private static String normalize(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }
}
