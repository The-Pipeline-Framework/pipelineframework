package org.pipelineframework.connector;

import java.util.Objects;
import java.util.Optional;

/**
 * Static provider identity and optional typed configuration schema hook.
 */
public record ConnectorProviderDescriptor(
    ConnectorProviderId id,
    ConnectorProviderVersion version,
    Optional<ConnectorConfigSchemaDescriptor> configurationSchema,
    Optional<ConnectorExecutionCapabilities> executionCapabilities
) {
    public ConnectorProviderDescriptor {
        id = Objects.requireNonNull(id, "provider ID must not be null");
        version = Objects.requireNonNull(version, "provider version must not be null");
        configurationSchema = Objects.requireNonNull(configurationSchema, "configuration schema must not be null");
        executionCapabilities = Objects.requireNonNull(executionCapabilities, "execution capabilities must not be null");
    }

    public ConnectorProviderDescriptor(ConnectorProviderId id, ConnectorProviderVersion version) {
        this(id, version, Optional.empty(), Optional.empty());
    }

    public ConnectorProviderDescriptor(
        ConnectorProviderId id,
        ConnectorProviderVersion version,
        Optional<ConnectorConfigSchemaDescriptor> configurationSchema
    ) {
        this(id, version, configurationSchema, Optional.empty());
    }
}
