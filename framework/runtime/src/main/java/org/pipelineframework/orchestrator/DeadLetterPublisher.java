package org.pipelineframework.orchestrator;

import io.smallrye.mutiny.Uni;

/**
 * Publishes terminal execution failures to a dead-letter destination.
 */
public interface DeadLetterPublisher {

    /**
     * Provider name used for configuration-based selection.
     *
     * @return provider name
     */
    default String providerName() {
        return "log";
    }

    /**
     * Provider priority used when multiple publishers are available.
     * Higher numeric values have higher precedence and are selected over lower values.
     * The default implementation returns {@code 0}, which takes precedence over
     * built-ins such as {@code LoggingDeadLetterPublisher} ({@code -100}).
     *
     * @return provider priority
     */
    default int priority() {
        return 0;
    }

    /**
     * Publishes one dead-letter envelope.
     *
     * @param envelope dead-letter payload
     * @return completion signal
     */
    Uni<Void> publish(DeadLetterEnvelope envelope);
}
