package org.pipelineframework.connector;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Static descriptor published by a provider artifact without constructing the provider.
 */
public record ConnectorProviderArtifactDescriptor(
    ConnectorProviderDescriptor provider,
    List<ConnectorOperationDescriptor> operations
) {
    public ConnectorProviderArtifactDescriptor {
        provider = Objects.requireNonNull(provider, "provider descriptor must not be null");
        operations = List.copyOf(Objects.requireNonNull(operations, "operations must not be null"));
        validateOperations(operations);
    }

    private static void validateOperations(Collection<ConnectorOperationDescriptor> operations) {
        for (ConnectorOperationDescriptor operation : operations) {
            Objects.requireNonNull(operation, "operation descriptor must not be null");
        }
    }
}
