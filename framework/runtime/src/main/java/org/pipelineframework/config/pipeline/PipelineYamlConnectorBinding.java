package org.pipelineframework.config.pipeline;

import java.util.Map;

import org.pipelineframework.connector.ConnectorBindingDefinition;
import org.pipelineframework.connector.ConnectorBindingName;
import org.pipelineframework.connector.ConnectorConfigurationDocument;
import org.pipelineframework.connector.ConnectorProviderId;

/**
 * One named configured Connector Provider binding from pipeline YAML.
 */
public record PipelineYamlConnectorBinding(
    String name,
    String provider,
    int version,
    Map<String, Object> config
) {
    public PipelineYamlConnectorBinding {
        name = new ConnectorBindingName(name).value();
        provider = ConnectorProviderId.of(provider).value();
        if (version < 1) {
            throw new IllegalArgumentException("connector binding '" + name + "' version must be positive");
        }
        config = config == null ? Map.of() : Map.copyOf(config);
    }

    public ConnectorBindingDefinition toDefinition() {
        return new ConnectorBindingDefinition(
            ConnectorBindingName.of(name),
            ConnectorProviderId.of(provider),
            version,
            new ConnectorConfigurationDocument(config));
    }
}
