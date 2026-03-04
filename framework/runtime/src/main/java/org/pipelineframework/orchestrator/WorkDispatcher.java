package org.pipelineframework.orchestrator;

import java.time.Duration;

import io.smallrye.mutiny.Uni;

/**
 * Dispatches execution work items to workers.
 */
public interface WorkDispatcher {

    /**
     * Provider name used to select this dispatcher via configuration.
     *
     * @return the provider name (default: "event")
     */
    default String providerName() {
        return "event";
    }

    /**
     * Provider priority used when multiple dispatchers are available.
     *
     * @return the priority value used to order dispatchers when more than one is available (default: 0)
     */
    default int priority() {
        return 0;
    }

    /**
 * Enqueues the given work item for immediate dispatch.
 *
 * @param item the work item to enqueue for immediate execution
 * @return a completion signal that completes when the item has been accepted for dispatch
 */
    Uni<Void> enqueueNow(ExecutionWorkItem item);

    /**
 * Schedules the given work item to be dispatched after the specified delay.
 *
 * @param item  the work item to dispatch
 * @param delay the time to wait before dispatching the item
 * @return a `Uni<Void>` that completes when the item has been enqueued for future dispatch
 */
    Uni<Void> enqueueDelayed(ExecutionWorkItem item, Duration delay);
}
