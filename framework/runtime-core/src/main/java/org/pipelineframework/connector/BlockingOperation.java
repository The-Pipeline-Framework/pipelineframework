package org.pipelineframework.connector;

/**
 * Operation whose invocation may block and must therefore begin on a framework worker.
 */
public interface BlockingOperation extends ConnectorOperation {
}
