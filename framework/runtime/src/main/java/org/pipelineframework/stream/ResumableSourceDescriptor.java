package org.pipelineframework.stream;

import java.util.Objects;

/**
 * Release-pinned identity of a provider-owned resumable expansion source.
 *
 * <p>The runtime persists this identity but never interprets provider cursor contents.
 */
public record ResumableSourceDescriptor(String providerKey, String sourceId, String fingerprint) {
    public ResumableSourceDescriptor {
        providerKey = requireText(providerKey, "providerKey");
        sourceId = requireText(sourceId, "sourceId");
        fingerprint = requireText(fingerprint, "fingerprint");
    }

    private static String requireText(String value, String name) {
        Objects.requireNonNull(value, name + " must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value.trim();
    }
}
