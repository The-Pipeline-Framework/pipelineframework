package org.pipelineframework.objectingest;

import java.util.Map;

import org.pipelineframework.repository.PayloadReference;

/**
 * Provider result for one listed object before application-domain projection.
 */
public record ObjectSourceItem(
    String provider,
    String container,
    String key,
    String versionId,
    String etag,
    long sizeBytes,
    long lastModifiedEpochMs,
    String contentType,
    Map<String, String> metadata,
    PayloadReference contentRef,
    String localPath
) {
    public ObjectSourceItem {
        provider = ObjectText.normalize(provider).orElse(null);
        container = ObjectText.normalize(container).orElse(null);
        key = ObjectText.normalize(key).orElse(null);
        versionId = ObjectText.normalize(versionId).orElse(null);
        etag = ObjectText.normalize(etag).orElse(null);
        contentType = ObjectText.normalize(contentType).orElse(null);
        localPath = ObjectText.normalize(localPath).orElse(null);
        metadata = copyMetadata(metadata);
        if (provider == null) {
            throw new IllegalArgumentException("object source item provider must not be blank");
        }
        if (key == null) {
            throw new IllegalArgumentException("object source item key must not be blank");
        }
        if (sizeBytes < 0) {
            throw new IllegalArgumentException("object source item sizeBytes must be >= 0");
        }
        if (lastModifiedEpochMs < 0) {
            throw new IllegalArgumentException("object source item lastModifiedEpochMs must be >= 0");
        }
    }

    private static Map<String, String> copyMetadata(Map<String, String> metadata) {
        if (metadata == null) {
            return Map.of();
        }
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null) {
                throw new IllegalArgumentException("object source item metadata must not contain null keys or values");
            }
        }
        return Map.copyOf(metadata);
    }

    public ObjectSnapshot toSnapshot(String sourceName, String textContent) {
        return new ObjectSnapshot(
            sourceName,
            provider,
            container,
            key,
            versionId,
            etag,
            sizeBytes,
            lastModifiedEpochMs,
            contentType,
            metadata,
            contentRef,
            textContent,
            localPath);
    }
}
