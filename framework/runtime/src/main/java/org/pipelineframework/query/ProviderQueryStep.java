package org.pipelineframework.query;

/**
 * Marker for generated Query steps backed by a ConnectorProvider operation.
 */
public interface ProviderQueryStep {
    QueryCacheRequirements queryCacheRequirements();
}
