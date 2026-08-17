package org.pipelineframework.connector;

import java.util.Objects;

/**
 * Runtime-only definition of one named, configured provider instance.
 *
 * <p>The configuration document is binding input, not durable release metadata. Hosts must publish
 * only a sanitized {@link ConnectorConfigurationSnapshot} when describing a binding.</p>
 */
public record ConnectorBindingDefinition(
    ConnectorBindingName name,
    ConnectorProviderId providerId,
    int providerMajorVersion,
    ConnectorConfigurationDocument configuration
) {
    public ConnectorBindingDefinition {
        name = Objects.requireNonNull(name, "connector binding name must not be null");
        providerId = Objects.requireNonNull(providerId, "connector provider ID must not be null");
        if (providerMajorVersion < 1) {
            throw new IllegalArgumentException("connector provider major version must be positive");
        }
        configuration = Objects.requireNonNull(configuration, "connector provider configuration must not be null");
    }
}
