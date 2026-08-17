package org.pipelineframework.connector;

import java.util.Objects;
import java.util.Set;

/**
 * Immutable result of an authorization decision made outside the connector registry.
 */
public record CallableOperationAuthorization(Set<ConnectorOperationIdentity> grantedOperations) {
    public CallableOperationAuthorization {
        Objects.requireNonNull(grantedOperations, "granted operations must not be null");
        for (ConnectorOperationIdentity identity : grantedOperations) {
            Objects.requireNonNull(identity, "granted operation identity must not be null");
        }
        grantedOperations = Set.copyOf(grantedOperations);
    }
}
