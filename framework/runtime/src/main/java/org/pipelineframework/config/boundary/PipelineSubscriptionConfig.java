package org.pipelineframework.config.boundary;

/**
 * Subscription boundary for reliable checkpoint intake.
 *
 * @param publication compile-time logical publication name this pipeline subscribes to
 * @param mapper optional mapper bean class implementing {@code Mapper<PublishedCheckpoint, PipelineInput>}
 */
public record PipelineSubscriptionConfig(
    String publication,
    String mapper
) {
    public PipelineSubscriptionConfig {
        if (publication == null || publication.isBlank()) {
            throw new IllegalArgumentException("subscription publication must not be blank");
        }
    }
}
