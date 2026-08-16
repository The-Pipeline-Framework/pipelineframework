package org.pipelineframework.connector;

import java.util.Optional;

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
            provider.configurationSchema().map(ConnectorConfigSchema::descriptor),
            optional(provider.executionCapabilities(), ConnectorExecutionCapabilities.conservative()));
    }

    static ConnectorOperationDescriptor operation(ConnectorOperation operation) {
        Optional<ConnectorConfigSchemaDescriptor> configurationSchema = Optional.empty();
        Optional<CommandCapabilities> commandCapabilities = Optional.empty();
        if (operation instanceof CommandOperation<?, ?, ?> command) {
            configurationSchema = command.configurationSchema().map(ConnectorConfigSchema::descriptor);
            commandCapabilities = optional(command.capabilities(), CommandCapabilities.conservative());
        } else if (operation instanceof QueryOperation<?, ?, ?> query) {
            configurationSchema = query.configurationSchema().map(ConnectorConfigSchema::descriptor);
        }
        return new ConnectorOperationDescriptor(
            operation.id(), kind(operation), operation.majorVersion(), configurationSchema, commandCapabilities);
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

    private static <T> Optional<T> optional(T value, T conservative) {
        return value.equals(conservative) ? Optional.empty() : Optional.of(value);
    }
}
