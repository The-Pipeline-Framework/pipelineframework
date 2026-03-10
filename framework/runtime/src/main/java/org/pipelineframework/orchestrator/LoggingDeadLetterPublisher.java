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

    @Override
    public String providerName() {
        return "log";
    }

    @Override
    public int priority() {
        return -100;
    }

    @Override
    public Uni<Void> publish(DeadLetterEnvelope envelope) {
        return Uni.createFrom().voidItem().invoke(() ->
            {
                DeadLetterMetrics.record(providerName(), envelope);
                LOG.errorf(
                    "Execution moved to DLQ: tenant=%s execution=%s transition=%s status=%s reason=%s error=%s retryable=%s retriesObserved=%d transport=%s platform=%s",
                envelope.tenantId(),
                envelope.executionId(),
                envelope.transitionKey(),
                envelope.terminalStatus(),
                envelope.terminalReason(),
                envelope.errorMessage(),
                envelope.retryable(),
                envelope.retriesObserved(),
                envelope.transport(),
                envelope.platform());
            });
    }
}
