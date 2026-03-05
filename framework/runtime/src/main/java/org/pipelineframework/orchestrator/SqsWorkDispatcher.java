package org.pipelineframework.orchestrator;

import java.time.Duration;
import jakarta.enterprise.context.ApplicationScoped;

import io.smallrye.mutiny.Uni;

/**
 * Placeholder provider for SQS-backed work dispatch.
 *
 * <p>The core runtime ships the SPI and an event-based dispatcher. A production SQS
 * provider is expected to be supplied by deployment-specific modules.</p>
 */
@ApplicationScoped
public class SqsWorkDispatcher implements WorkDispatcher {

    private static final String ERROR =
        "SqsWorkDispatcher is selected but not implemented in core runtime. " +
            "Provide a deployment-specific provider module.";

    @Override
    public String providerName() {
        return "sqs";
    }

    @Override
    public int priority() {
        return -1000;
    }

    @Override
    public Uni<Void> enqueueNow(ExecutionWorkItem item) {
        return Uni.createFrom().failure(new UnsupportedOperationException(ERROR));
    }

    @Override
    public Uni<Void> enqueueDelayed(ExecutionWorkItem item, Duration delay) {
        return Uni.createFrom().failure(new UnsupportedOperationException(ERROR));
    }
}
