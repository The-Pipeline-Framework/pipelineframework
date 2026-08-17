package org.pipelineframework.connector;

/**
 * Metadata/catalog contract for a provider operation. It deliberately has no universal execute method.
 */
public interface ConnectorOperation {
    String id();

    default int majorVersion() {
        return 1;
    }
}
