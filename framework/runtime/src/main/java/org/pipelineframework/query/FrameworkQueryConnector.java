package org.pipelineframework.query;

import java.util.concurrent.CompletionStage;

/**
 * Framework connector contract used by first-party captured query modules.
 */
public interface FrameworkQueryConnector {
    String connectorName();

    <O> CompletionStage<O> queryOne(QueryRequest<?> request, Class<O> outputType);
}
