package org.pipelineframework.connector;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.pipelineframework.protocol.ProtocolTypeDescriptor;

/**
 * Static descriptor published by a provider artifact without constructing the provider.
 */
public record ConnectorProviderArtifactDescriptor(
    ConnectorProviderDescriptor provider,
    List<ConnectorOperationDescriptor> operations,
    List<ProtocolTypeDescriptor> protocolTypes
) {
    public ConnectorProviderArtifactDescriptor {
        provider = Objects.requireNonNull(provider, "provider descriptor must not be null");
        operations = List.copyOf(Objects.requireNonNull(operations, "operations must not be null"));
        protocolTypes = List.copyOf(Objects.requireNonNull(protocolTypes, "protocol types must not be null"));
        validateOperations(operations);
        validateProtocolTypes(provider, protocolTypes);
    }

    public ConnectorProviderArtifactDescriptor(
        ConnectorProviderDescriptor provider,
        List<ConnectorOperationDescriptor> operations
    ) {
        this(provider, operations, List.of());
    }

    private static void validateOperations(Collection<ConnectorOperationDescriptor> operations) {
        for (ConnectorOperationDescriptor operation : operations) {
            Objects.requireNonNull(operation, "operation descriptor must not be null");
        }
    }

    private static void validateProtocolTypes(
        ConnectorProviderDescriptor provider,
        Collection<ProtocolTypeDescriptor> protocolTypes
    ) {
        for (ProtocolTypeDescriptor protocolType : protocolTypes) {
            ProtocolTypeDescriptor checked = Objects.requireNonNull(protocolType, "protocol type descriptor must not be null");
            if (!provider.id().equals(checked.identity().namespace())) {
                throw new IllegalArgumentException("connector protocol type '" + checked.identity()
                    + "' must use provider namespace '" + provider.id().value() + "'");
            }
        }
    }
}
