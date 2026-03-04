package org.pipelineframework.orchestrator;

import java.time.Duration;

import io.smallrye.mutiny.Uni;

/**
 * Dispatches execution work items to workers.
 */
public interface WorkDispatcher {

    /**
     * Provider name used for configuration-based selection.
     *
     * @return provider name
     */
    default String providerName() {
        return "event";
    }

    /**
     * Provider priority used when multiple dispatchers are available.
     *
     * @return provider priority
     */
    default int priority() {
        return 0;
    }

    /**
     * Enqueues work for immediate dispatch.
     *
     * @param item work item
     * @return completion signal
     */
    Uni<Void> enqueueNow(ExecutionWorkItem item);

    /**
     * Enqueues work for delayed dispatch.
     *
     * @param item work item
     * @param delay delay before dispatch
     * @return completion signal
     */
    Uni<Void> enqueueDelayed(ExecutionWorkItem item, Duration delay);
}
