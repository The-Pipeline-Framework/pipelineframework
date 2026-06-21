package org.pipelineframework.objectpublish;

import java.util.Map;

import org.pipelineframework.config.boundary.PipelineObjectPublishConfig;

/**
 * Provider-neutral object write request.
 */
public record ObjectWriteRequest(
    String targetName,
    PipelineObjectPublishConfig target,
    String objectKey,
    byte[] bytes,
    String contentType,
    Map<String, String> metadata,
    String checksum,
    String idempotencyKey
) {
    public ObjectWriteRequest {
        targetName = normalize(targetName);
        objectKey = normalize(objectKey);
        contentType = normalize(contentType);
        checksum = normalize(checksum);
        idempotencyKey = normalize(idempotencyKey);
        bytes = bytes == null ? new byte[0] : bytes.clone();
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

    @Override
    public byte[] bytes() {
        return bytes.clone();
    }

    private static String normalize(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }
}
