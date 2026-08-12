package org.pipelineframework.connector;

import java.util.Objects;

/**
 * Stable operation identity used for registry uniqueness and lookup.
 */
public record ConnectorOperationIdentity(
    ConnectorProviderId providerId,
    String operationId,
    ConnectorOperationKind kind,
    int majorVersion
) implements Comparable<ConnectorOperationIdentity> {
    public ConnectorOperationIdentity {
        providerId = Objects.requireNonNull(providerId, "provider ID must not be null");
        operationId = ConnectorProviderId.require(operationId, "operation ID");
        kind = Objects.requireNonNull(kind, "operation kind must not be null");
        if (majorVersion < 1) {
            throw new IllegalArgumentException("operation major version must be positive");
        }
    }

    public static ConnectorOperationIdentity of(ConnectorProviderDescriptor provider, ConnectorOperationDescriptor operation) {
        Objects.requireNonNull(provider, "provider descriptor must not be null");
        Objects.requireNonNull(operation, "operation descriptor must not be null");
        return new ConnectorOperationIdentity(provider.id(), operation.id(), operation.kind(), operation.majorVersion());
    }

    @Override
    public int compareTo(ConnectorOperationIdentity other) {
        Objects.requireNonNull(other, "other must not be null");
        int providerComparison = providerId.compareTo(other.providerId);
        if (providerComparison != 0) {
            return providerComparison;
        }
        int operationComparison = operationId.compareTo(other.operationId);
        if (operationComparison != 0) {
            return operationComparison;
        }
        int kindComparison = kind.compareTo(other.kind);
        return kindComparison != 0 ? kindComparison : Integer.compare(majorVersion, other.majorVersion);
    }
}
