package org.pipelineframework.objectpublish;

import java.util.Map;

/**
 * Provider-neutral request to close a streaming object write session.
 */
public record ObjectWriteCloseRequest(
    long bytes,
    String checksum,
    Map<String, String> metadata
) {
    public ObjectWriteCloseRequest {
        checksum = normalize(checksum);
        metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
        if (bytes < 0) {
            throw new IllegalArgumentException("object write close bytes must be >= 0");
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
