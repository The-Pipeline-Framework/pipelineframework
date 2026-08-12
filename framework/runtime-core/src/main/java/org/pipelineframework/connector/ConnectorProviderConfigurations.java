package org.pipelineframework.connector;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Explicit provider-level configuration documents keyed by provider identity.
 */
public record ConnectorProviderConfigurations(Map<ConnectorProviderId, ConnectorConfigurationDocument> values) {
    public ConnectorProviderConfigurations {
        Objects.requireNonNull(values, "provider configurations must not be null");
        Map<ConnectorProviderId, ConnectorConfigurationDocument> copy = new LinkedHashMap<>();
        for (Map.Entry<ConnectorProviderId, ConnectorConfigurationDocument> entry : values.entrySet()) {
            copy.put(
                Objects.requireNonNull(entry.getKey(), "provider ID must not be null"),
                Objects.requireNonNull(entry.getValue(), "provider configuration document must not be null"));
        }
        values = Map.copyOf(copy);
    }

    public static ConnectorProviderConfigurations empty() {
        return new ConnectorProviderConfigurations(Map.of());
    }
}
