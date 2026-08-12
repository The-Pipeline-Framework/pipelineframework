package org.pipelineframework.connector;

/**
 * Actionable configuration binding failure, scoped to a provider or operation descriptor.
 */
public final class ConnectorConfigurationException extends IllegalArgumentException {
    public ConnectorConfigurationException(String message) {
        super(message);
    }
}
