package org.pipelineframework.connector;

import java.util.Objects;

/**
 * A provider-declared safe correlation or reconciliation reference.
 */
public record CommandReference(String kind, String value, CommandReferencePurpose purpose) {
    public CommandReference {
        kind = ConnectorProviderId.require(kind, "command reference kind");
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("command reference value must not be blank");
        }
        if (value.length() > 256) {
            throw new IllegalArgumentException("command reference value must not exceed 256 characters");
        }
        purpose = Objects.requireNonNull(purpose, "command reference purpose must not be null");
    }
}
