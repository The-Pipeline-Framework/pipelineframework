package org.pipelineframework.connector;

import java.util.Objects;
import java.util.Optional;

/**
 * Static operation descriptor. The provider identity is supplied by its containing provider.
 */
public record ConnectorOperationDescriptor(
    String id,
    ConnectorOperationKind kind,
    int majorVersion,
    Optional<ConnectorConfigSchemaDescriptor> configurationSchema
) {
    public ConnectorOperationDescriptor {
        id = ConnectorProviderId.require(id, "operation ID");
        kind = Objects.requireNonNull(kind, "operation kind must not be null");
        if (majorVersion < 1) {
            throw new IllegalArgumentException("operation major version must be positive");
        }
        configurationSchema = Objects.requireNonNull(configurationSchema, "configuration schema must not be null");
    }

    public ConnectorOperationDescriptor(String id, ConnectorOperationKind kind, int majorVersion) {
        this(id, kind, majorVersion, Optional.empty());
    }
}
