package org.pipelineframework.connector;

/**
 * Operation that permits only one in-flight provider invocation per configured binding.
 */
public interface SerializedOperation extends ConnectorOperation {
}
