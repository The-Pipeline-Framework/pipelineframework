package org.pipelineframework.connector;

import java.util.Objects;

/**
 * Configuration-level secret reference. Resolved secret material remains runtime-only.
 */
public record SecretRef(String value) {
    public SecretRef {
        Objects.requireNonNull(value, "secret reference must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException("secret reference must not be blank");
        }
    }
}
