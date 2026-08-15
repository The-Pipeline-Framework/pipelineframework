package org.pipelineframework.connector;

/**
 * Declared concurrency ownership for a connector provider.
 */
public enum ConnectorConcurrencyScope {
    UNSPECIFIED,
    UNBOUNDED,
    PROVIDER_SCOPED,
    CONNECTION_SCOPED,
    OPERATION_SCOPED,
    PROVIDER_MANAGED
}
