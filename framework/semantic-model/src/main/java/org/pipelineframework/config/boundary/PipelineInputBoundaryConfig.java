package org.pipelineframework.config.boundary;

/**
 * Input boundary configuration for a pipeline.
 *
 * @param subscription reliable checkpoint subscription settings
 * @param object object source admission settings
 */
public record PipelineInputBoundaryConfig(
    PipelineSubscriptionConfig subscription,
    PipelineObjectInputConfig object
) {
    public PipelineInputBoundaryConfig(PipelineSubscriptionConfig subscription) {
        this(subscription, null);
    }

    public PipelineInputBoundaryConfig {
        if (subscription != null && object != null) {
            throw new IllegalArgumentException("pipeline input boundary cannot declare both subscription and object");
        }
        if (subscription == null && object == null) {
            throw new IllegalArgumentException("pipeline input boundary must declare either subscription or object");
        }
    }
}
