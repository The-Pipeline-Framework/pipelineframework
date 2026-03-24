package org.pipelineframework.config.boundary;

/**
 * Input boundary configuration for a pipeline.
 *
 * @param subscription reliable checkpoint subscription settings
 */
public record PipelineInputBoundaryConfig(
    PipelineSubscriptionConfig subscription
) {
}
