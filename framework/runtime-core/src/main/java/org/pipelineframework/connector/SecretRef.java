package org.pipelineframework.connector;

import java.util.Objects;

/**
 * Legacy configuration-level secret reference.
 *
 * @deprecated This context-free reference cannot safely select tenant-aware authenticated
 * connector access. Connectors that call external systems must use a {@link ConnectionRef}
 * resolved through {@link ConnectionResolutionRequest} instead.
 */
@Deprecated(forRemoval = true)
public record SecretRef(String value) {
    public SecretRef {
        Objects.requireNonNull(value, "secret reference must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException("secret reference must not be blank");
        }
    }
}
