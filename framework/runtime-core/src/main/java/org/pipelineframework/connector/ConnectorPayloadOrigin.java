package org.pipelineframework.connector;

import java.util.Objects;
import java.util.Optional;

/**
 * Binding-owned origin of a connector payload reference.
 *
 * <p>The configuration snapshot excludes resolved secrets, so credential rotation does not change
 * reference identity. Any provider configuration that identifies the remote resource does.</p>
 */
public record ConnectorPayloadOrigin(
    ConnectorBindingName bindingName,
    ConnectorOperationIdentity operation,
    int providerMajorVersion,
    Optional<ConnectorConfigurationSnapshot> configuration
) {
    public ConnectorPayloadOrigin {
        bindingName = Objects.requireNonNull(bindingName, "connector payload binding name must not be null");
        operation = Objects.requireNonNull(operation, "connector payload operation identity must not be null");
        if (!ConnectorOperationKind.OBJECT_SOURCE.equals(operation.kind())) {
            throw new IllegalArgumentException("connector payload origin operation must be an object source");
        }
        if (providerMajorVersion < 1) {
            throw new IllegalArgumentException("connector payload provider major version must be positive");
        }
        configuration = Objects.requireNonNull(configuration, "connector payload configuration must not be null");
    }
}
