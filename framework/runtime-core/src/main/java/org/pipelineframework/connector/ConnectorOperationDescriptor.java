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
    Optional<QueryCapabilities> queryCapabilities,
    Optional<QueryOperationCardinality> queryCardinality,
    Optional<ConnectorOperationTypeContract> typeContract
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
        queryCardinality = Objects.requireNonNull(queryCardinality, "query cardinality must not be null");
        typeContract = Objects.requireNonNull(typeContract, "operation type contract must not be null");
        if (commandCapabilities.isPresent() && !ConnectorOperationKind.COMMAND.equals(kind)) {
            throw new IllegalArgumentException("command capabilities require command operation kind");
        }
        if (queryCapabilities.isPresent() && !ConnectorOperationKind.QUERY.equals(kind)) {
            throw new IllegalArgumentException("query capabilities require query operation kind");
        }
        if (queryCardinality.isPresent() != ConnectorOperationKind.QUERY.equals(kind)) {
            throw new IllegalArgumentException("query operation kind and query cardinality must be declared together");
        }
        if (queryCardinality.filter(cardinality -> cardinality == QueryOperationCardinality.ONE_TO_MANY).isPresent()
            && queryCapabilities.isPresent()) {
            throw new IllegalArgumentException(
                "streaming Query operations must not declare unary Query cache capabilities");
        }
    }

    public ConnectorOperationDescriptor(String id, ConnectorOperationKind kind, int majorVersion) {
        this(id, kind, majorVersion, Optional.empty(), Optional.empty(), Optional.empty(),
            defaultQueryCardinality(kind), Optional.empty());
    }

    public ConnectorOperationDescriptor(
        String id,
        ConnectorOperationKind kind,
        int majorVersion,
        Optional<ConnectorConfigSchemaDescriptor> configurationSchema
    ) {
        this(id, kind, majorVersion, configurationSchema, Optional.empty(), Optional.empty(),
            defaultQueryCardinality(kind), Optional.empty());
    }

    public ConnectorOperationDescriptor(
        String id,
        ConnectorOperationKind kind,
        int majorVersion,
        Optional<ConnectorConfigSchemaDescriptor> configurationSchema,
        Optional<CommandCapabilities> commandCapabilities
    ) {
        this(id, kind, majorVersion, configurationSchema, commandCapabilities, Optional.empty(),
            defaultQueryCardinality(kind), Optional.empty());
    }

    public ConnectorOperationDescriptor(
        String id,
        ConnectorOperationKind kind,
        int majorVersion,
        Optional<ConnectorConfigSchemaDescriptor> configurationSchema,
        Optional<CommandCapabilities> commandCapabilities,
        Optional<QueryCapabilities> queryCapabilities,
        Optional<ConnectorOperationTypeContract> typeContract
    ) {
        this(id, kind, majorVersion, configurationSchema, commandCapabilities, queryCapabilities,
            defaultQueryCardinality(kind), typeContract);
    }

    private static Optional<QueryOperationCardinality> defaultQueryCardinality(ConnectorOperationKind kind) {
        return ConnectorOperationKind.QUERY.equals(kind)
            ? Optional.of(QueryOperationCardinality.ONE_TO_ONE)
            : Optional.empty();
    }
}
