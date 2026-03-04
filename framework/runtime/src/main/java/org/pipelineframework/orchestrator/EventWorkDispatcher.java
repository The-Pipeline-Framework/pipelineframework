package org.pipelineframework.orchestrator;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;

/**
 * Dispatcher that routes work through CDI async events.
 */
@ApplicationScoped
public class EventWorkDispatcher implements WorkDispatcher {

    @Inject
    Event<ExecutionWorkItem> executionWorkEvent;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "tpf-work-dispatcher");
        thread.setDaemon(true);
        return thread;
    });

    /**
     * Provider name used to identify this dispatcher implementation.
     *
     * @return the provider name "event"
     */
    @Override
    public String providerName() {
        return "event";
    }

    /**
     * Provides this dispatcher's priority for ordering among dispatchers.
     *
     * @return the priority value for this dispatcher (-100)
     */
    @Override
    public int priority() {
        return -100;
    }

    /**
     * Dispatches the given work item through the configured CDI execution event.
     *
     * @param item the execution work item to dispatch
     * @return a Uni that completes when the dispatch request has been initiated; it does not indicate completion of the work item's processing
     */
    @Override
    public Uni<Void> enqueueNow(ExecutionWorkItem item) {
        return Uni.createFrom().voidItem()
            .invoke(() -> executionWorkEvent.fireAsync(item));
    }

    /**
     * Schedules an ExecutionWorkItem to be dispatched after the specified delay via the CDI async event.
     *
     * @param item  the work item to dispatch
     * @param delay the delay before dispatch; if null or negative, zero is used
     * @return a Uni<Void> that completes once the dispatch task has been scheduled; the item will be fired asynchronously after the delay
     */
    @Override
    public Uni<Void> enqueueDelayed(ExecutionWorkItem item, Duration delay) {
        long delayMs = Math.max(0L, delay == null ? 0L : delay.toMillis());
        return Uni.createFrom().voidItem()
            .invoke(() -> scheduler.schedule(() -> executionWorkEvent.fireAsync(item), delayMs, TimeUnit.MILLISECONDS));
    }

    /**
     * Shuts down the dispatcher's scheduler immediately when the CDI bean is destroyed.
     *
     * This halts currently executing tasks and cancels pending scheduled tasks.
     */
    @PreDestroy
    void shutdown() {
        scheduler.shutdownNow();
    }
}
