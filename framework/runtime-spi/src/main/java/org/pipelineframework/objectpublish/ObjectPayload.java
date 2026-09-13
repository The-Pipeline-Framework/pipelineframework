package org.pipelineframework.objectpublish;

import java.util.Map;

/**
 * Rendered object payload produced by an application publish mapper.
 *
 * @param bytes encoded payload bytes
 * @param contentType payload content type override
 * @param metadata application metadata to attach to the written object
 */
public record ObjectPayload(
    byte[] bytes,
    String contentType,
    Map<String, String> metadata
) {
    public ObjectPayload {
        bytes = bytes == null ? new byte[0] : bytes.clone();
        contentType = normalize(contentType);
        metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null) {
                throw new IllegalArgumentException("object payload metadata must not contain null keys or values");
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
