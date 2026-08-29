package org.pipelineframework.connector;

import java.util.Optional;
import java.util.Objects;

/**
 * Framework-owned projection from executable connector declarations to static metadata.
 */
final class ConnectorDescriptors {
    private ConnectorDescriptors() {
    }

    static ConnectorProviderDescriptor provider(ConnectorProvider<?> provider) {
        return new ConnectorProviderDescriptor(
            provider.id(),
            provider.version(),
            provider.configurationSchema().map(ConnectorConfigSchema::descriptor));
    }

    static ConnectorOperationDescriptor operation(ConnectorOperation operation) {
        Optional<ConnectorConfigSchemaDescriptor> configurationSchema = Optional.empty();
        Optional<CommandCapabilities> commandCapabilities = Optional.empty();
        Optional<QueryCapabilities> queryCapabilities = Optional.empty();
        if (operation instanceof CommandOperation<?, ?, ?> command) {
            configurationSchema = command.configurationSchema().map(ConnectorConfigSchema::descriptor);
            commandCapabilities = optional(
                command.capabilities(), CommandCapabilities.conservative(), "command operation capabilities");
        } else if (operation instanceof QueryOperation<?, ?, ?> query) {
            configurationSchema = query.configurationSchema().map(ConnectorConfigSchema::descriptor);
            queryCapabilities = optional(
                query.capabilities(), QueryCapabilities.conservative(), "query operation capabilities");
        }
        return new ConnectorOperationDescriptor(
            operation.id(), kind(operation), operation.majorVersion(), configurationSchema,
            commandCapabilities, queryCapabilities, ConnectorOperationTypes.contract(operation));
    }

    static ConnectorOperationKind kind(ConnectorOperation operation) {
        if (operation instanceof CommandOperation<?, ?, ?>) {
            return ConnectorOperationKind.COMMAND;
        }
        if (operation instanceof QueryOperation<?, ?, ?>) {
            return ConnectorOperationKind.QUERY;
        }
        if (operation instanceof ObjectSourceOperation) {
            return ConnectorOperationKind.OBJECT_SOURCE;
        }
        if (operation instanceof ObjectTargetOperation) {
            return ConnectorOperationKind.OBJECT_TARGET;
        }
        throw new IllegalArgumentException(
            "connector operation must implement a supported semantic family: " + operation.getClass().getName());
    }

    private static <T> Optional<T> optional(T value, T conservative, String subject) {
        Objects.requireNonNull(value, subject + " must not be null");
        return value.equals(conservative) ? Optional.empty() : Optional.of(value);
    }
}
