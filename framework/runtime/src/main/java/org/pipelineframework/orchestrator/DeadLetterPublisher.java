package org.pipelineframework.orchestrator;

import io.smallrye.mutiny.Uni;

/**
 * Publishes terminal execution failures to a dead-letter destination.
 */
public interface DeadLetterPublisher {

    /**
     * The provider name used to select this publisher via configuration.
     *
     * @return the provider name
     */
    default String providerName() {
        return "log";
    }

    /**
     * Indicates the provider's selection priority when multiple publishers are available.
     *
     * @return the priority used for provider selection; higher values indicate higher precedence
     */
    default int priority() {
        return 0;
    }

    /**
 * Publish a dead-letter envelope to the configured dead-letter destination.
 *
 * @param envelope the dead-letter payload to publish
 * @return a Uni that completes when publishing is finished
 */
    Uni<Void> publish(DeadLetterEnvelope envelope);
}
