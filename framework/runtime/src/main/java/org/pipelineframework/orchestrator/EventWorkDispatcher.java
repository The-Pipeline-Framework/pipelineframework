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
     * The provider name for this dispatcher implementation.
     *
     * @return the provider name {@code "event"}
     */
    @Override
    public String providerName() {
        return "event";
    }

    /**
     * Indicates this dispatcher's priority for ordering among dispatchers.
     *
     * @return the priority value (-100) used to order dispatchers; lower values indicate lower priority
     */
    @Override
    public int priority() {
        return -100;
    }

    /**
     * Enqueues the given work item for immediate dispatch via the CDI execution event.
     *
     * @param item the work item to dispatch
     * @return a Uni that completes when the work item has been submitted for dispatch
     */
    @Override
    public Uni<Void> enqueueNow(ExecutionWorkItem item) {
        return Uni.createFrom().voidItem()
            .invoke(() -> executionWorkEvent.fireAsync(item));
    }

    /**
     * Schedules the given work item to be dispatched via CDI events after the specified delay.
     *
     * @param item  the work item to dispatch
     * @param delay the delay before dispatch; null or a negative duration is treated as zero
     * @return a Uni that, when subscribed, schedules the work item to be fired asynchronously after the normalized delay
     */
    @Override
    public Uni<Void> enqueueDelayed(ExecutionWorkItem item, Duration delay) {
        long delayMs = Math.max(0L, delay == null ? 0L : delay.toMillis());
        return Uni.createFrom().voidItem()
            .invoke(() -> scheduler.schedule(() -> executionWorkEvent.fireAsync(item), delayMs, TimeUnit.MILLISECONDS));
    }

    /**
     * Stops the internal scheduler when the bean is destroyed.
     *
     * Invokes {@code shutdownNow()} on the scheduler to attempt to halt executing tasks and prevent submission of new tasks.
     */
    @PreDestroy
    void shutdown() {
        scheduler.shutdownNow();
    }
}
