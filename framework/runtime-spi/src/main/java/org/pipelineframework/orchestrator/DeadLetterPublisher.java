package org.pipelineframework.orchestrator;

import java.util.Optional;

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
     * Validates provider readiness for startup.
     *
     * <p>Return a non-empty value when the provider is selected but cannot safely operate
     * with the configuration supplied when the provider was constructed.</p>
     *
     * @return optional startup validation error
     */
    default Optional<String> startupValidationError() {
        throw new IllegalStateException("Dead-letter publisher '" + providerName()
            + "' must implement startupValidationError() and be rebuilt against the current runtime SPI");
    }

    /**
     * Publishes one dead-letter envelope.
     *
     * @param envelope dead-letter payload
     * @return completion signal
     */
    Uni<Void> publish(DeadLetterEnvelope envelope);
}
