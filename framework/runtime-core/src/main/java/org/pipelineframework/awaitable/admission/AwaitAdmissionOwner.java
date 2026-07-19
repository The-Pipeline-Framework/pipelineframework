package org.pipelineframework.awaitable.admission;

import java.util.Objects;

/**
 * Deterministic identity for one unresolved await interaction.
 */
public record AwaitAdmissionOwner(String key) {
    public AwaitAdmissionOwner {
        Objects.requireNonNull(key, "key must not be null");
        if (key.isBlank()) {
            throw new IllegalArgumentException("key must not be blank");
        }
    }
}
