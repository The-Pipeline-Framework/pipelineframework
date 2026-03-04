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
     * Identifies this dispatcher as the SQS-backed provider.
     *
     * @return the provider name "sqs"
     */
    @Override
    public String providerName() {
        return "sqs";
    }

    /**
     * Specifies this dispatcher's selection priority among WorkDispatcher implementations.
     *
     * Lower values indicate lower selection precedence when multiple providers are available.
     *
     * @return the priority value; lower values represent lower precedence
     */
    @Override
    public int priority() {
        return -1000;
    }

    /**
     * Indicates that enqueuing work to SQS is not implemented in the core runtime and always fails.
     *
     * @param item the work item that would be enqueued
     * @return a Uni that fails with an {@link UnsupportedOperationException} containing the provider-not-implemented message
     */
    @Override
    public Uni<Void> enqueueNow(ExecutionWorkItem item) {
        return Uni.createFrom().failure(new UnsupportedOperationException(ERROR));
    }

    /**
     * Signals that enqueueing a work item with a delay is not supported by the core SQS placeholder dispatcher.
     *
     * @param item  the work item that would be enqueued
     * @param delay the delay after which the work item would be enqueued
     * @return a failed {@code Uni<Void>} with an {@link UnsupportedOperationException} indicating the SQS provider is not implemented in the core runtime
     */
    @Override
    public Uni<Void> enqueueDelayed(ExecutionWorkItem item, Duration delay) {
        return Uni.createFrom().failure(new UnsupportedOperationException(ERROR));
    }
}
