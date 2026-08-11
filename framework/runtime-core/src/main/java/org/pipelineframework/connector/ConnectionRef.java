package org.pipelineframework.connector;

import java.util.Objects;

/**
 * Logical deployment-owned connection reference; it is not a resolved connection handle.
 */
public record ConnectionRef(String value) {
    public ConnectionRef {
        Objects.requireNonNull(value, "connection reference must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException("connection reference must not be blank");
        }
    }
}
