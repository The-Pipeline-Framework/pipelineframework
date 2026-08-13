package org.pipelineframework.connector;

/**
 * Declared execution ownership for a connector provider.
 */
public enum ConnectorExecutionStyle {
    UNSPECIFIED,
    NON_BLOCKING,
    BLOCKING,
    PROVIDER_MANAGED
}
