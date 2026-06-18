package org.pipelineframework.objectingest;

import java.util.Map;

import org.pipelineframework.repository.PayloadReference;

/**
 * Stable framework snapshot for one object discovered by an object source.
 *
 * @param sourceName configured source name
 * @param provider provider name
 * @param container provider-specific bucket, root, or container
 * @param key provider-specific object key
 * @param versionId provider object version when available
 * @param etag provider object etag or content hash when available
 * @param sizeBytes object size in bytes
 * @param lastModifiedEpochMs last modified time in epoch milliseconds, or zero when unknown
 * @param contentType object content type when known
 * @param metadata provider/application metadata
 * @param contentRef provider-native claim-check reference
 * @param textContent loaded text content when payload mode is text
 * @param localPath local filesystem path when provider can expose one
 */
public record ObjectSnapshot(
    String sourceName,
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
    String textContent,
    String localPath
) {
    public ObjectSnapshot {
        sourceName = ObjectText.normalize(sourceName).orElse(null);
        provider = ObjectText.normalize(provider).orElse(null);
        container = ObjectText.normalize(container).orElse(null);
        key = ObjectText.normalize(key).orElse(null);
        versionId = ObjectText.normalize(versionId).orElse(null);
        etag = ObjectText.normalize(etag).orElse(null);
        contentType = ObjectText.normalize(contentType).orElse(null);
        textContent = ObjectText.normalize(textContent).orElse(null);
        localPath = ObjectText.normalize(localPath).orElse(null);
        metadata = copyMetadata(metadata);
        if (sourceName == null) {
            throw new IllegalArgumentException("object snapshot sourceName must not be blank");
        }
        if (provider == null) {
            throw new IllegalArgumentException("object snapshot provider must not be blank");
        }
        if (key == null) {
            throw new IllegalArgumentException("object snapshot key must not be blank");
        }
        if (sizeBytes < 0) {
            throw new IllegalArgumentException("object snapshot sizeBytes must be >= 0");
        }
        if (lastModifiedEpochMs < 0) {
            throw new IllegalArgumentException("object snapshot lastModifiedEpochMs must be >= 0");
        }
    }

    private static Map<String, String> copyMetadata(Map<String, String> metadata) {
        if (metadata == null) {
            return Map.of();
        }
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null) {
                throw new IllegalArgumentException("object snapshot metadata must not contain null keys or values");
            }
        }
        return Map.copyOf(metadata);
    }
}
