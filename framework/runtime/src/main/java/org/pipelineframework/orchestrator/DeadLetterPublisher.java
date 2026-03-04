package org.pipelineframework.orchestrator;

import io.smallrye.mutiny.Uni;

/**
 * Publishes terminal execution failures to a dead-letter destination.
 */
public interface DeadLetterPublisher {

    /**
     * Provider name used to select this DeadLetterPublisher implementation for configuration.
     *
     * @return the provider name used for configuration selection, defaults to "log"
     */
    default String providerName() {
        return "log";
    }

    /**
     * The provider's precedence value used to choose between multiple publishers.
     *
     * Higher numeric values have higher precedence and are selected over lower values.
     * The default implementation returns {@code 0}, which takes precedence over built-ins
     * such as {@code LoggingDeadLetterPublisher} ({@code -100}).
     *
     * @return the precedence value for this provider; higher values indicate higher precedence
     */
    default int priority() {
        return 0;
    }

    /**
 * Publishes the given dead-letter envelope to the configured dead-letter destination.
 *
 * @param envelope the dead-letter payload to publish
 * @return completion signal for the publish operation
 */
    Uni<Void> publish(DeadLetterEnvelope envelope);
}
