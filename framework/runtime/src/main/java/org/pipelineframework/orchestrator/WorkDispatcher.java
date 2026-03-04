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
     * Higher numeric values have higher precedence and are selected over lower values.
     * The default implementation returns {@code 0}, which intentionally takes
     * precedence over built-ins such as {@code EventWorkDispatcher} ({@code -100})
     * and {@code SqsWorkDispatcher} ({@code -1000}).
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
     * <p>Implementations must treat a {@code null} delay as zero delay
     * (equivalent to {@code Duration.ofMillis(0)}).</p>
     *
     * @param item work item to enqueue
     * @param delay delay before dispatch; {@code null} means no delay
     * @return completion signal
     */
    Uni<Void> enqueueDelayed(ExecutionWorkItem item, Duration delay);
}
