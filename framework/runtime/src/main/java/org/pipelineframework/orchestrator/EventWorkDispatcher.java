package org.pipelineframework.orchestrator;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.enterprise.event.NotificationOptions;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;

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
        return Uni.createFrom().item(() -> {
            executionWorkEvent.fireAsync(item, NotificationOptions.ofExecutor(Infrastructure.getDefaultExecutor()));
            return null;
        }).replaceWithVoid();
    }

    @Override
    public Uni<Void> enqueueDelayed(ExecutionWorkItem item, Duration delay) {
        long delayMs = Math.max(0L, delay == null ? 0L : delay.toMillis());
        CompletableFuture<Void> completion = new CompletableFuture<>();
        try {
            scheduler.schedule(() -> {
                try {
                    executionWorkEvent.fireAsync(item,
                        NotificationOptions.ofExecutor(Infrastructure.getDefaultExecutor()));
                    completion.complete(null);
                } catch (Throwable failure) {
                    completion.completeExceptionally(failure);
                }
            }, delayMs, TimeUnit.MILLISECONDS);
        } catch (RuntimeException failure) {
            completion.completeExceptionally(failure);
        }
        return Uni.createFrom().completionStage(() -> completion);
    }

    @PreDestroy
    void shutdown() {
        scheduler.shutdownNow();
    }
}
