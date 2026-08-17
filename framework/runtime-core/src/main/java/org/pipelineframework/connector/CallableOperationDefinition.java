package org.pipelineframework.connector;

import java.util.Objects;
import java.util.Optional;

/** One normalized, model-safe operation in an authorized callable snapshot. */
public record CallableOperationDefinition(
    ConnectorOperationIdentity identity,
    String description,
    ConnectorOperationTypeContract typeContract,
    Optional<CallableCommandCapabilities> commandCapabilities,
    Optional<CallableQueryCapabilities> queryCapabilities
) implements Comparable<CallableOperationDefinition> {
    public CallableOperationDefinition {
        identity = Objects.requireNonNull(identity, "callable operation identity must not be null");
        if (!ConnectorOperationKind.QUERY.equals(identity.kind())
            && !ConnectorOperationKind.COMMAND.equals(identity.kind())) {
            throw new IllegalArgumentException("callable operation kind must be query or command");
        }
        description = requireDescription(description);
        typeContract = Objects.requireNonNull(typeContract, "callable type contract must not be null");
        commandCapabilities = Objects.requireNonNull(
            commandCapabilities, "callable command capabilities must not be null");
        queryCapabilities = Objects.requireNonNull(queryCapabilities, "callable query capabilities must not be null");
        if (ConnectorOperationKind.COMMAND.equals(identity.kind()) != commandCapabilities.isPresent()) {
            throw new IllegalArgumentException("callable Command definition must declare command capabilities only");
        }
        if (ConnectorOperationKind.QUERY.equals(identity.kind()) != queryCapabilities.isPresent()) {
            throw new IllegalArgumentException("callable Query definition must declare query capabilities only");
        }
    }

    @Override
    public int compareTo(CallableOperationDefinition other) {
        return identity.compareTo(Objects.requireNonNull(other, "other must not be null").identity);
    }

    private static String requireDescription(String value) {
        Objects.requireNonNull(value, "callable description must not be null");
        String normalized = value.trim();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException("callable description must not be blank");
        }
        if (normalized.codePoints().anyMatch(Character::isISOControl)) {
            throw new IllegalArgumentException("callable description must not contain control characters");
        }
        return normalized;
    }
}
