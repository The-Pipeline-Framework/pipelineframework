package org.pipelineframework.orchestrator;

import jakarta.enterprise.context.ApplicationScoped;

import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;

/**
 * Default dead-letter publisher that logs terminal failures.
 */
@ApplicationScoped
public class LoggingDeadLetterPublisher implements DeadLetterPublisher {

    private static final Logger LOG = Logger.getLogger(LoggingDeadLetterPublisher.class);

    /**
     * Provides the provider name for this dead-letter publisher implementation.
     *
     * @return the provider name "log"
     */
    @Override
    public String providerName() {
        return "log";
    }

    /**
     * Provides the priority ordering for this dead-letter publisher.
     *
     * @return the priority value; higher numbers indicate higher priority (this publisher returns -100)
     */
    @Override
    public int priority() {
        return -100;
    }

    /**
     * Logs the provided dead-letter envelope's terminal failure information to the error log.
     *
     * @param envelope the dead-letter envelope whose tenantId, executionId, transitionKey and errorMessage will be logged
     * @return a Uni that completes with no value; subscribing to it triggers the error-level log entry for the envelope
     */
    @Override
    public Uni<Void> publish(DeadLetterEnvelope envelope) {
        return Uni.createFrom().voidItem().invoke(() ->
            LOG.errorf("Execution moved to DLQ: tenant=%s execution=%s transition=%s error=%s",
                envelope.tenantId(),
                envelope.executionId(),
                envelope.transitionKey(),
                envelope.errorMessage()));
    }
}
