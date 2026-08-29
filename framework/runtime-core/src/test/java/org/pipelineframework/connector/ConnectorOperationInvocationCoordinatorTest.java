package org.pipelineframework.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.runtime.core.RuntimeAdapters;

class ConnectorOperationInvocationCoordinatorTest {
    private static final ConnectorBindingName BINDING = ConnectorBindingName.of("test.binding");
    private final ConnectorOperationInvocationCoordinator coordinator =
        new ConnectorOperationInvocationCoordinator();

    @AfterEach
    void resetRuntimeAdapters() {
        RuntimeAdapters.resetForTests();
    }

    @Test
    void offloadsBlockingQueryAndFlattensItsStage() throws Exception {
        String caller = Thread.currentThread().getName();
        AtomicReference<String> invokedOn = new AtomicReference<>();
        BlockingQueryOperation<Object, Object, Object> operation = blockingQuery();

        CompletionStage<String> result = coordinator.invoke(BINDING, operation, () -> {
            invokedOn.set(Thread.currentThread().getName());
            return CompletableFuture.completedFuture("done");
        });

        assertEquals("done", result.toCompletableFuture().get(5, TimeUnit.SECONDS));
        assertNotEquals(caller, invokedOn.get());
    }

    @Test
    void offloadsBlockingCommandAndPreservesSynchronousFailure() {
        String caller = Thread.currentThread().getName();
        AtomicReference<String> invokedOn = new AtomicReference<>();
        BlockingCommandOperation<Object, Object, Object> operation = blockingCommand();

        CompletionStage<String> result = coordinator.invoke(BINDING, operation, () -> {
            invokedOn.set(Thread.currentThread().getName());
            throw new IllegalArgumentException("broken");
        });

        CompletionException failure = assertThrows(
            CompletionException.class, () -> result.toCompletableFuture().join());
        assertTrue(failure.getCause() instanceof IllegalArgumentException);
        assertNotEquals(caller, invokedOn.get());
    }

    @Test
    void ordinaryOperationReturnsIncompleteProviderStageWithoutOffload() {
        QueryOperation<Object, Object, Object> operation = ordinaryQuery();
        CompletableFuture<String> provider = new CompletableFuture<>();
        AtomicReference<String> invokedOn = new AtomicReference<>();

        CompletionStage<String> result = coordinator.invoke(BINDING, operation, () -> {
            invokedOn.set(Thread.currentThread().getName());
            return provider;
        });

        assertEquals(Thread.currentThread().getName(), invokedOn.get());
        assertFalse(result.toCompletableFuture().isDone());
        provider.complete("done");
        assertEquals("done", result.toCompletableFuture().join());
    }

    @Test
    void asynchronousProviderFailureIsPreserved() {
        QueryOperation<Object, Object, Object> operation = ordinaryQuery();
        IllegalStateException providerFailure = new IllegalStateException("async-provider-failure");
        CompletableFuture<String> provider = new CompletableFuture<>();
        CompletionStage<String> result = coordinator.invoke(BINDING, operation, () -> provider);

        provider.completeExceptionally(providerFailure);

        CompletionException failure = assertThrows(
            CompletionException.class, () -> result.toCompletableFuture().join());
        assertEquals(providerFailure, failure.getCause());
    }

    @Test
    void providerManagedOperationCompletesOnItsOwnExecutor() throws Exception {
        QueryOperation<Object, Object, Object> operation = ordinaryQuery();
        try (var executor = Executors.newSingleThreadExecutor(r -> new Thread(r, "provider-owned"))) {
            AtomicReference<String> completedOn = new AtomicReference<>();
            CompletionStage<String> result = coordinator.invoke(BINDING, operation, () ->
                CompletableFuture.supplyAsync(() -> {
                    completedOn.set(Thread.currentThread().getName());
                    return "done";
                }, executor));

            assertEquals("done", result.toCompletableFuture().get(5, TimeUnit.SECONDS));
            assertEquals("provider-owned", completedOn.get());
        }
    }

    @Test
    void serializesUntilProviderTerminationAndReleasesAfterFailure() {
        SerializedQuery operation = new SerializedQuery();
        CompletableFuture<String> first = new CompletableFuture<>();
        CompletableFuture<String> second = new CompletableFuture<>();
        AtomicInteger invocations = new AtomicInteger();

        CompletionStage<String> firstResult = coordinator.invoke(BINDING, operation, () -> {
            invocations.incrementAndGet();
            return first;
        });
        CompletionStage<String> secondResult = coordinator.invoke(BINDING, operation, () -> {
            invocations.incrementAndGet();
            return second;
        });

        assertEquals(1, invocations.get());
        first.completeExceptionally(new IllegalStateException("failed"));
        assertThrows(CompletionException.class, () -> firstResult.toCompletableFuture().join());
        assertEquals(2, invocations.get());
        second.complete("done");
        assertEquals("done", secondResult.toCompletableFuture().join());
    }

    @Test
    void queuedCancellationSkipsInvocation() {
        SerializedQuery operation = new SerializedQuery();
        CompletableFuture<String> first = new CompletableFuture<>();
        AtomicInteger secondInvocations = new AtomicInteger();

        coordinator.invoke(BINDING, operation, () -> first);
        CompletableFuture<String> second = coordinator.invoke(BINDING, operation, () -> {
            secondInvocations.incrementAndGet();
            return CompletableFuture.completedFuture("second");
        }).toCompletableFuture();

        assertTrue(second.cancel(true));
        first.complete("first");
        assertEquals(0, secondInvocations.get());
    }

    @Test
    void startedCancellationRetainsGateUntilProviderTerminates() throws Exception {
        SerializedQuery operation = new SerializedQuery();
        CompletableFuture<String> provider = new CompletableFuture<>();
        AtomicInteger nextInvocations = new AtomicInteger();
        CompletionStage<String> first = coordinator.invoke(BINDING, operation, provider::minimalCompletionStage);
        CountDownLatch nextStarted = new CountDownLatch(1);
        CompletionStage<String> next = coordinator.invoke(BINDING, operation, () -> {
            nextInvocations.incrementAndGet();
            nextStarted.countDown();
            return CompletableFuture.completedFuture("next");
        });

        assertTrue(first.toCompletableFuture().cancel(true));
        assertFalse(nextStarted.await(100, TimeUnit.MILLISECONDS));
        provider.complete("provider-terminated");
        assertTrue(nextStarted.await(5, TimeUnit.SECONDS));
        assertEquals(1, nextInvocations.get());
        assertEquals("next", next.toCompletableFuture().join());
    }

    @Test
    void separateBindingsRemainIndependentForTheSameOperationInstance() {
        SerializedQuery operation = new SerializedQuery();
        CompletableFuture<String> held = new CompletableFuture<>();

        coordinator.invoke(ConnectorBindingName.of("first.binding"), operation, () -> held);
        CompletionStage<String> independent = coordinator.invoke(
            ConnectorBindingName.of("second.binding"), operation,
            () -> CompletableFuture.completedFuture("independent"));

        assertEquals("independent", independent.toCompletableFuture().join());
    }

    @Test
    void sameLogicalOperationWithinOneBindingSharesTheGateAcrossInstances() {
        SerializedQuery firstInstance = new SerializedQuery();
        SerializedQuery secondInstance = new SerializedQuery();
        CompletableFuture<String> held = new CompletableFuture<>();
        AtomicInteger secondInvocations = new AtomicInteger();

        coordinator.invoke(BINDING, firstInstance, () -> held);
        CompletionStage<String> second = coordinator.invoke(BINDING, secondInstance, () -> {
            secondInvocations.incrementAndGet();
            return CompletableFuture.completedFuture("second");
        });

        assertEquals(0, secondInvocations.get());
        held.complete("first");
        assertEquals("second", second.toCompletableFuture().join());
        assertEquals(1, secondInvocations.get());
    }

    @Test
    void blockingAndSerializedMarkersCompose() throws Exception {
        BlockingSerializedQuery operation = new BlockingSerializedQuery();
        String caller = Thread.currentThread().getName();
        AtomicReference<String> invokedOn = new AtomicReference<>();

        CompletionStage<String> result = coordinator.invoke(BINDING, operation, () -> {
            invokedOn.set(Thread.currentThread().getName());
            return CompletableFuture.completedFuture("done");
        });

        assertEquals("done", result.toCompletableFuture().get(5, TimeUnit.SECONDS));
        assertNotEquals(caller, invokedOn.get());
    }

    @Test
    void serializedGateReleasesWhenBlockingAdmissionFailsSynchronously() {
        IllegalStateException admissionFailure = new IllegalStateException("worker-admission-failed");
        RuntimeAdapters.registerReactiveRuntime(new org.pipelineframework.runtime.core.ReactiveRuntime() {
            @Override
            public <T> CompletionStage<T> executeBlocking(
                java.util.function.Supplier<T> supplier,
                boolean virtualThread
            ) {
                throw admissionFailure;
            }
        });
        BlockingSerializedQuery blocking = new BlockingSerializedQuery();

        CompletionStage<String> failed = coordinator.invoke(BINDING, blocking, () ->
            CompletableFuture.completedFuture("unreachable"));
        AtomicInteger nextInvocations = new AtomicInteger();
        CompletionStage<String> next = coordinator.invoke(BINDING, new SerializedQuery(), () -> {
            nextInvocations.incrementAndGet();
            return CompletableFuture.completedFuture("next");
        });

        CompletionException failure = assertThrows(
            CompletionException.class, () -> failed.toCompletableFuture().join());
        assertEquals(admissionFailure, failure.getCause());
        assertEquals("next", next.toCompletableFuture().join());
        assertEquals(1, nextInvocations.get());
    }

    @Test
    void blockingWorkerLifetimeIsShorterThanSerializedProviderStageLifetime() throws Exception {
        try (var worker = Executors.newSingleThreadExecutor(r -> new Thread(r, "single-blocking-worker"))) {
            RuntimeAdapters.registerReactiveRuntime(new org.pipelineframework.runtime.core.ReactiveRuntime() {
                @Override
                public <T> CompletionStage<T> executeBlocking(
                    java.util.function.Supplier<T> supplier,
                    boolean virtualThread
                ) {
                    return CompletableFuture.supplyAsync(supplier, worker);
                }
            });
            BlockingSerializedQuery operation = new BlockingSerializedQuery();
            CompletableFuture<String> providerStage = new CompletableFuture<>();
            CountDownLatch firstInvoked = new CountDownLatch(1);
            CountDownLatch secondInvoked = new CountDownLatch(1);
            AtomicReference<String> firstWorker = new AtomicReference<>();
            AtomicReference<String> secondWorker = new AtomicReference<>();

            CompletionStage<String> first = coordinator.invoke(BINDING, operation, () -> {
                firstWorker.set(Thread.currentThread().getName());
                firstInvoked.countDown();
                return providerStage;
            });
            assertTrue(firstInvoked.await(5, TimeUnit.SECONDS));

            CompletionStage<String> second = coordinator.invoke(BINDING, operation, () -> {
                secondWorker.set(Thread.currentThread().getName());
                secondInvoked.countDown();
                return CompletableFuture.completedFuture("second");
            });

            String releasedWorker = RuntimeAdapters.executeBlocking(
                () -> Thread.currentThread().getName(), false).toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertEquals("single-blocking-worker", releasedWorker);
            assertFalse(secondInvoked.await(100, TimeUnit.MILLISECONDS));

            Thread.ofPlatform().name("unrelated-provider-completer").start(() -> providerStage.complete("first"));

            assertEquals("first", first.toCompletableFuture().get(5, TimeUnit.SECONDS));
            assertTrue(secondInvoked.await(5, TimeUnit.SECONDS));
            assertEquals("second", second.toCompletableFuture().get(5, TimeUnit.SECONDS));
            assertEquals("single-blocking-worker", firstWorker.get());
            assertEquals("single-blocking-worker", secondWorker.get());
        }
    }

    private static QueryOperation<Object, Object, Object> ordinaryQuery() {
        return new QueryOperation<>() {
            @Override
            public String id() {
                return "ordinary";
            }

            @Override
            public CompletionStage<QueryOutcome<Object>> query(QueryInvocation<Object, Object, Object> invocation) {
                return CompletableFuture.completedFuture(new QueryOutcome.NotFound<>("not-found"));
            }
        };
    }

    private static BlockingQueryOperation<Object, Object, Object> blockingQuery() {
        return new BlockingQueryOperation<>() {
            @Override
            public String id() {
                return "blocking-query";
            }

            @Override
            public CompletionStage<QueryOutcome<Object>> query(QueryInvocation<Object, Object, Object> invocation) {
                return CompletableFuture.completedFuture(new QueryOutcome.NotFound<>("not-found"));
            }
        };
    }

    private static BlockingCommandOperation<Object, Object, Object> blockingCommand() {
        return new BlockingCommandOperation<>() {
            @Override
            public String id() {
                return "blocking-command";
            }

            @Override
            public CompletionStage<CommandOutcome<Object>> dispatch(
                CommandInvocation<Object, Object> invocation
            ) {
                return CompletableFuture.completedFuture(new CommandOutcome.Succeeded<>(
                    new Object(), CommandConfirmation.none(), java.util.List.of()));
            }
        };
    }

    private static class SerializedQuery implements QueryOperation<Object, Object, Object>, SerializedOperation {
        @Override
        public String id() {
            return "serialized";
        }

        @Override
        public CompletionStage<QueryOutcome<Object>> query(QueryInvocation<Object, Object, Object> invocation) {
            return CompletableFuture.completedFuture(new QueryOutcome.NotFound<>("not-found"));
        }
    }

    private static final class BlockingSerializedQuery extends SerializedQuery
        implements BlockingQueryOperation<Object, Object, Object> {
    }
}
