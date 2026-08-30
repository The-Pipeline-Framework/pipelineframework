package org.pipelineframework.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import io.smallrye.common.vertx.VertxContext;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.runtime.core.RuntimeAdapters;

class RuntimeAdapterBootstrapTest {
    private RuntimeAdapterBootstrap bootstrap;

    @AfterEach
    void stopBootstrap() {
        if (bootstrap != null) {
            bootstrap.onStop();
        } else {
            RuntimeAdapters.resetForTests();
        }
    }

    @Test
    void quarkusBlockingBoundaryUsesTheWorkerPoolWhenVirtualThreadsAreNotRequested() throws Exception {
        bootstrap = new RuntimeAdapterBootstrap();
        bootstrap.onStart(null);
        String caller = Thread.currentThread().getName();
        RuntimeAdapters.setExecutionContext("tenant", "tenant-1");

        String worker = RuntimeAdapters.executeBlocking(
            () -> Thread.currentThread().getName(), false).toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertNotEquals(caller, worker);
        org.junit.jupiter.api.Assertions.assertEquals("tenant-1", RuntimeAdapters.executeBlocking(
            () -> RuntimeAdapters.executionContext("tenant", String.class), false)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS));
    }

    @Test
    void clearingOneVertxContextDoesNotStopAnotherContextFromPropagatingTheSameKey() throws Exception {
        bootstrap = new RuntimeAdapterBootstrap();
        bootstrap.onStart(null);
        Vertx vertx = Vertx.vertx();
        try {
            Context root = vertx.getOrCreateContext();
            Context first = VertxContext.createNewDuplicatedContext(root);
            Context second = VertxContext.createNewDuplicatedContext(root);

            onContext(first, () -> {
                RuntimeAdapters.setExecutionContext("tenant", "tenant-1");
                return CompletableFuture.completedFuture(null);
            });
            onContext(second, () -> {
                RuntimeAdapters.setExecutionContext("tenant", "tenant-2");
                return CompletableFuture.completedFuture(null);
            });
            onContext(first, () -> {
                RuntimeAdapters.clearExecutionContext("tenant");
                return CompletableFuture.completedFuture(null);
            });

            String propagated = onContext(second, () -> RuntimeAdapters.executeBlocking(
                () -> RuntimeAdapters.executionContext("tenant", String.class), false));

            assertEquals("tenant-2", propagated);
        } finally {
            vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    private static <T> T onContext(Context context, Supplier<CompletionStage<T>> action) throws Exception {
        CompletableFuture<T> result = new CompletableFuture<>();
        context.runOnContext(ignored -> {
            try {
                action.get().whenComplete((value, failure) -> {
                    if (failure == null) {
                        result.complete(value);
                    } else {
                        result.completeExceptionally(failure);
                    }
                });
            } catch (Throwable failure) {
                result.completeExceptionally(failure);
            }
        });
        return result.get(5, TimeUnit.SECONDS);
    }
}
