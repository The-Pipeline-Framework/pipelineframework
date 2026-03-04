package org.pipelineframework.orchestrator;

import java.time.Duration;

import io.smallrye.mutiny.Uni;

/**
 * Dispatches execution work items to workers.
 */
public interface WorkDispatcher {

    /**
     * Identifies the provider name used to select this dispatcher via configuration.
     *
     * Implementations may override this; the default provider name is "event".
     *
     * @return the provider name used for configuration-based selection
     */
    default String providerName() {
        return "event";
    }

    /**
     * Provider priority used to select between multiple dispatchers.
     * Higher numeric values indicate higher precedence. The default value is {@code 0},
     * which is chosen to take precedence over built-in dispatchers such as
     * {@code EventWorkDispatcher} ({@code -100}) and {@code SqsWorkDispatcher} ({@code -1000}).
     *
     * @return the provider priority value; higher values indicate higher precedence
     */
    default int priority() {
        return 0;
    }

    /**
 * Enqueues the given work item for immediate dispatch.
 *
 * @param item the work item to enqueue for immediate dispatch
 * @return a Uni that completes when the item has been enqueued
 */
    Uni<Void> enqueueNow(ExecutionWorkItem item);

    /**
 * Enqueues a work item for delayed dispatch.
 *
 * <p>Implementations must treat a {@code null} {@code delay} as zero delay
 * (equivalent to {@code Duration.ofMillis(0)}).</p>
 *
 * @param item  the work item to enqueue
 * @param delay the delay before dispatch; {@code null} means no delay
 * @return      a completion signal indicating the enqueue operation has finished
 */
    Uni<Void> enqueueDelayed(ExecutionWorkItem item, Duration delay);
}
