package org.pipelineframework.connector;

import java.util.Objects;

/**
 * A provider-declared safe correlation or reconciliation reference.
 *
 * <p>Declaring a reference kind in {@link CommandCapabilities#durableReferenceKinds()} is a
 * provider data-classification decision: values must be non-sensitive opaque identifiers, never
 * credentials, tokens, arbitrary evidence, URLs, or provider payloads.</p>
 */
public record CommandReference(String kind, String value, CommandReferencePurpose purpose) {
    public static final int MAX_VALUE_LENGTH = 128;
    public static final int MAX_REFERENCES_PER_OUTCOME = 16;

    public CommandReference {
        kind = ConnectorProviderId.require(kind, "command reference kind");
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("command reference value must not be blank");
        }
        if (value.length() > MAX_VALUE_LENGTH) {
            throw new IllegalArgumentException(
                "command reference value must not exceed " + MAX_VALUE_LENGTH + " characters");
        }
        if (!value.matches("[A-Za-z0-9][A-Za-z0-9._:/-]*") || value.contains("://")) {
            throw new IllegalArgumentException(
                "command reference value must be an opaque identifier, not a URL or arbitrary metadata");
        }
        purpose = Objects.requireNonNull(purpose, "command reference purpose must not be null");
    }
}
