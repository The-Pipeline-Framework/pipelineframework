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

    /**
     * Identifies this dispatcher as the SQS provider.
     *
     * @return the provider name "sqs"
     */
    @Override
    public String providerName() {
        return "sqs";
    }

    /**
     * Provides the selection priority for this work dispatcher.
     *
     * @return the dispatcher selection priority; lower values indicate lower priority (this implementation returns -1000)
     */
    @Override
    public int priority() {
        return -1000;
    }

    /**
     * Attempts to enqueue the given work item for immediate dispatch using the SQS provider, but signals that the SQS provider is not implemented in the core runtime.
     *
     * @param item the execution work item to enqueue
     * @return a `Uni<Void>` that fails with an `UnsupportedOperationException` containing the provider-not-implemented error message
     */
    @Override
    public Uni<Void> enqueueNow(ExecutionWorkItem item) {
        return Uni.createFrom().failure(new UnsupportedOperationException(ERROR));
    }

    /**
     * Indicates that scheduling a work item with a delay is unavailable because the SQS provider is not implemented in the core runtime.
     *
     * @param item  the execution work item intended for delayed enqueue
     * @param delay the requested delay before the item should be enqueued
     * @return a Uni that fails with an UnsupportedOperationException containing the message "SqsWorkDispatcher is selected but not implemented in core runtime. Provide a deployment-specific provider module."
     */
    @Override
    public Uni<Void> enqueueDelayed(ExecutionWorkItem item, Duration delay) {
        return Uni.createFrom().failure(new UnsupportedOperationException(ERROR));
    }
}
