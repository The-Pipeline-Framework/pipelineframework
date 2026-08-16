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
    Optional<ConnectorConfigSchemaDescriptor> configurationSchema,
    Optional<CommandCapabilities> commandCapabilities,
    Optional<QueryCapabilities> queryCapabilities
) {
    public ConnectorOperationDescriptor {
        id = ConnectorProviderId.require(id, "operation ID");
        kind = Objects.requireNonNull(kind, "operation kind must not be null");
        if (majorVersion < 1) {
            throw new IllegalArgumentException("operation major version must be positive");
        }
        configurationSchema = Objects.requireNonNull(configurationSchema, "configuration schema must not be null");
        commandCapabilities = Objects.requireNonNull(commandCapabilities, "command capabilities must not be null");
        queryCapabilities = Objects.requireNonNull(queryCapabilities, "query capabilities must not be null");
        if (commandCapabilities.isPresent() && !ConnectorOperationKind.COMMAND.equals(kind)) {
            throw new IllegalArgumentException("command capabilities require command operation kind");
        }
        if (queryCapabilities.isPresent() && !ConnectorOperationKind.QUERY.equals(kind)) {
            throw new IllegalArgumentException("query capabilities require query operation kind");
        }
    }

    public ConnectorOperationDescriptor(String id, ConnectorOperationKind kind, int majorVersion) {
        this(id, kind, majorVersion, Optional.empty(), Optional.empty(), Optional.empty());
    }

    public ConnectorOperationDescriptor(
        String id,
        ConnectorOperationKind kind,
        int majorVersion,
        Optional<ConnectorConfigSchemaDescriptor> configurationSchema
    ) {
        this(id, kind, majorVersion, configurationSchema, Optional.empty(), Optional.empty());
    }

    public ConnectorOperationDescriptor(
        String id,
        ConnectorOperationKind kind,
        int majorVersion,
        Optional<ConnectorConfigSchemaDescriptor> configurationSchema,
        Optional<CommandCapabilities> commandCapabilities
    ) {
        this(id, kind, majorVersion, configurationSchema, commandCapabilities, Optional.empty());
    }
}
