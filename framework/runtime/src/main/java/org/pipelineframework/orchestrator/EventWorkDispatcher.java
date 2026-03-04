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

    void setExecutionWorkEvent(Event<ExecutionWorkItem> executionWorkEvent) {
        this.executionWorkEvent = executionWorkEvent;
    }

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "tpf-work-dispatcher");
        thread.setDaemon(true);
        return thread;
    });

    @Override
    public String providerName() {
        return "event";
    }

    @Override
    public int priority() {
        return -100;
    }

    @Override
    public Uni<Void> enqueueNow(ExecutionWorkItem item) {
        return Uni.createFrom().voidItem()
            .invoke(() -> executionWorkEvent.fireAsync(item));
    }

    @Override
    public Uni<Void> enqueueDelayed(ExecutionWorkItem item, Duration delay) {
        long delayMs = Math.max(0L, delay == null ? 0L : delay.toMillis());
        return Uni.createFrom().voidItem()
            .invoke(() -> scheduler.schedule(() -> executionWorkEvent.fireAsync(item), delayMs, TimeUnit.MILLISECONDS));
    }

    @PreDestroy
    void shutdown() {
        scheduler.shutdownNow();
    }
}
