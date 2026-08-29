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
import java.util.concurrent.Flow;
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
    void blockingCancellationRetainsGateUntilTheProviderStageTerminates() throws Exception {
        try (var worker = Executors.newSingleThreadExecutor(r -> new Thread(r, "blocking-cancellation-worker"))) {
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
            CompletableFuture<String> provider = new CompletableFuture<>();
            CountDownLatch providerStarted = new CountDownLatch(1);
            CompletionStage<String> first = coordinator.invoke(BINDING, operation, () -> {
                providerStarted.countDown();
                return provider.minimalCompletionStage();
            });
            assertTrue(providerStarted.await(5, TimeUnit.SECONDS));

            CountDownLatch nextStarted = new CountDownLatch(1);
            CompletionStage<String> next = coordinator.invoke(BINDING, operation, () -> {
                nextStarted.countDown();
                return CompletableFuture.completedFuture("next");
            });

            assertTrue(first.toCompletableFuture().cancel(true));
            assertFalse(nextStarted.await(100, TimeUnit.MILLISECONDS));
            provider.complete("provider-terminated");
            assertTrue(nextStarted.await(5, TimeUnit.SECONDS));
            assertEquals("next", next.toCompletableFuture().get(5, TimeUnit.SECONDS));
        }
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

    @Test
    void streamingInvocationStartsOnDemandAndForwardsDemand() throws Exception {
        StreamingQuery operation = new StreamingQuery();
        ControlledPublisher<String> rows = new ControlledPublisher<>();
        CompletableFuture<Void> termination = new CompletableFuture<>();
        AtomicInteger invocations = new AtomicInteger();
        RecordingSubscriber<String> subscriber = new RecordingSubscriber<>();

        coordinator.<String>invokeStream(BINDING, operation, () -> {
            invocations.incrementAndGet();
            return new QueryStream<>(rows, termination);
        }).subscribe(subscriber);

        assertEquals(0, invocations.get());
        subscriber.request(1);
        assertEquals(1, invocations.get());
        assertEquals(1L, rows.demand());
        rows.emit("row-0");
        assertEquals(java.util.List.of("row-0"), subscriber.items());
        assertEquals(0L, rows.demand());
        rows.complete();
        termination.complete(null);
        subscriber.awaitTerminal();
        assertTrue(subscriber.completed());
    }

    @Test
    void serializedStreamingGateCoversRowsAndProviderResourceLifetime() throws Exception {
        SerializedStreamingQuery operation = new SerializedStreamingQuery();
        ControlledPublisher<String> firstRows = new ControlledPublisher<>();
        CompletableFuture<Void> firstTermination = new CompletableFuture<>();
        AtomicInteger secondInvocations = new AtomicInteger();
        RecordingSubscriber<String> first = new RecordingSubscriber<>();
        RecordingSubscriber<String> second = new RecordingSubscriber<>();

        coordinator.invokeStream(BINDING, operation,
            () -> new QueryStream<>(firstRows, firstTermination)).subscribe(first);
        coordinator.invokeStream(BINDING, operation, () -> {
            secondInvocations.incrementAndGet();
            ControlledPublisher<String> rows = new ControlledPublisher<>();
            rows.completeOnSubscribe();
            return new QueryStream<>(rows, CompletableFuture.completedFuture(null));
        }).subscribe(second);
        first.request(Long.MAX_VALUE);
        second.request(Long.MAX_VALUE);

        firstRows.complete();
        assertEquals(0, secondInvocations.get());
        assertFalse(first.completed());
        firstTermination.complete(null);

        first.awaitTerminal();
        second.awaitTerminal();
        assertEquals(1, secondInvocations.get());
        assertTrue(first.completed());
        assertTrue(second.completed());
    }

    @Test
    void queuedStreamingCancellationSkipsProviderInvocation() throws Exception {
        SerializedStreamingQuery operation = new SerializedStreamingQuery();
        ControlledPublisher<String> firstRows = new ControlledPublisher<>();
        CompletableFuture<Void> firstTermination = new CompletableFuture<>();
        AtomicInteger queuedInvocations = new AtomicInteger();
        RecordingSubscriber<String> first = new RecordingSubscriber<>();
        RecordingSubscriber<String> queued = new RecordingSubscriber<>();

        coordinator.invokeStream(BINDING, operation,
            () -> new QueryStream<>(firstRows, firstTermination)).subscribe(first);
        coordinator.<String>invokeStream(BINDING, operation, () -> {
            queuedInvocations.incrementAndGet();
            return new QueryStream<>(new ControlledPublisher<>(), CompletableFuture.completedFuture(null));
        }).subscribe(queued);
        first.request(Long.MAX_VALUE);
        queued.request(Long.MAX_VALUE);
        queued.cancel();

        firstRows.complete();
        firstTermination.complete(null);
        first.awaitTerminal();
        assertEquals(0, queuedInvocations.get());
    }

    @Test
    void startedStreamingCancellationRetainsGateUntilProviderResourcesTerminate() throws Exception {
        SerializedStreamingQuery operation = new SerializedStreamingQuery();
        ControlledPublisher<String> firstRows = new ControlledPublisher<>();
        CompletableFuture<Void> firstTermination = new CompletableFuture<>();
        AtomicInteger nextInvocations = new AtomicInteger();
        RecordingSubscriber<String> first = new RecordingSubscriber<>();
        RecordingSubscriber<String> next = new RecordingSubscriber<>();

        coordinator.invokeStream(BINDING, operation,
            () -> new QueryStream<>(firstRows, firstTermination)).subscribe(first);
        coordinator.invokeStream(BINDING, operation, () -> {
            nextInvocations.incrementAndGet();
            ControlledPublisher<String> rows = new ControlledPublisher<>();
            rows.completeOnSubscribe();
            return new QueryStream<>(rows, CompletableFuture.completedFuture(null));
        }).subscribe(next);
        first.request(Long.MAX_VALUE);
        next.request(Long.MAX_VALUE);

        first.cancel();
        assertTrue(firstRows.cancelled());
        assertEquals(0, nextInvocations.get());
        firstTermination.complete(null);
        next.awaitTerminal();
        assertEquals(1, nextInvocations.get());
    }

    @Test
    void blockingSerializedStreamReleasesWorkerButRetainsGateUntilStreamTermination() throws Exception {
        try (var worker = Executors.newSingleThreadExecutor(r -> new Thread(r, "stream-blocking-worker"))) {
            RuntimeAdapters.registerReactiveRuntime(new org.pipelineframework.runtime.core.ReactiveRuntime() {
                @Override
                public <T> CompletionStage<T> executeBlocking(
                    java.util.function.Supplier<T> supplier,
                    boolean virtualThread
                ) {
                    return CompletableFuture.supplyAsync(supplier, worker);
                }
            });
            BlockingSerializedStreamingQuery operation = new BlockingSerializedStreamingQuery();
            ControlledPublisher<String> firstRows = new ControlledPublisher<>();
            CompletableFuture<Void> firstTermination = new CompletableFuture<>();
            AtomicReference<String> firstWorker = new AtomicReference<>();
            AtomicReference<String> secondWorker = new AtomicReference<>();
            CountDownLatch firstInvoked = new CountDownLatch(1);
            CountDownLatch secondInvoked = new CountDownLatch(1);
            RecordingSubscriber<String> first = new RecordingSubscriber<>();
            RecordingSubscriber<String> second = new RecordingSubscriber<>();

            coordinator.invokeStream(BINDING, operation, () -> {
                firstWorker.set(Thread.currentThread().getName());
                firstInvoked.countDown();
                return new QueryStream<>(firstRows, firstTermination);
            }).subscribe(first);
            first.request(Long.MAX_VALUE);
            assertTrue(firstInvoked.await(5, TimeUnit.SECONDS));

            coordinator.invokeStream(BINDING, operation, () -> {
                secondWorker.set(Thread.currentThread().getName());
                secondInvoked.countDown();
                ControlledPublisher<String> rows = new ControlledPublisher<>();
                rows.completeOnSubscribe();
                return new QueryStream<>(rows, CompletableFuture.completedFuture(null));
            }).subscribe(second);
            second.request(Long.MAX_VALUE);

            String availableWorker = RuntimeAdapters.executeBlocking(
                () -> Thread.currentThread().getName(), false).toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertEquals("stream-blocking-worker", availableWorker);
            assertFalse(secondInvoked.await(100, TimeUnit.MILLISECONDS));

            Thread.ofPlatform().name("unrelated-stream-terminator").start(() -> {
                firstRows.complete();
                firstTermination.complete(null);
            });
            first.awaitTerminal();
            assertTrue(secondInvoked.await(5, TimeUnit.SECONDS));
            second.awaitTerminal();
            assertEquals("stream-blocking-worker", firstWorker.get());
            assertEquals("stream-blocking-worker", secondWorker.get());
        }
    }

    @Test
    void blockingSerializedStreamReleasesGateWhenWorkerAdmissionFailsSynchronously() throws Exception {
        IllegalStateException admissionFailure = new IllegalStateException("stream-worker-admission-failed");
        RuntimeAdapters.registerReactiveRuntime(new org.pipelineframework.runtime.core.ReactiveRuntime() {
            @Override
            public <T> CompletionStage<T> executeBlocking(
                java.util.function.Supplier<T> supplier,
                boolean virtualThread
            ) {
                throw admissionFailure;
            }
        });
        RecordingSubscriber<String> failed = new RecordingSubscriber<>();
        coordinator.<String>invokeStream(BINDING, new BlockingSerializedStreamingQuery(), () -> {
            throw new AssertionError("provider invocation must not start when worker admission fails");
        }).subscribe(failed);

        failed.request(Long.MAX_VALUE);

        assertTrue(failed.terminal.await(5, TimeUnit.SECONDS));
        assertEquals(admissionFailure, failed.failure);

        RecordingSubscriber<String> next = new RecordingSubscriber<>();
        coordinator.invokeStream(BINDING, new SerializedStreamingQuery(), () -> {
            ControlledPublisher<String> rows = new ControlledPublisher<>();
            rows.completeOnSubscribe();
            return new QueryStream<>(rows, CompletableFuture.completedFuture(null));
        }).subscribe(next);
        next.request(Long.MAX_VALUE);
        next.awaitTerminal();
        assertTrue(next.completed());
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

    private static class StreamingQuery implements StreamingQueryOperation<Object, Object, String> {
        @Override
        public String id() {
            return "streaming";
        }

        @Override
        public QueryStream<String> query(QueryInvocation<Object, Object, String> invocation) {
            throw new UnsupportedOperationException("test invokes through the coordinator supplier");
        }
    }

    private static class SerializedStreamingQuery extends StreamingQuery implements SerializedOperation {
    }

    private static final class BlockingSerializedStreamingQuery extends SerializedStreamingQuery
        implements BlockingOperation {
    }

    private static final class RecordingSubscriber<T> implements Flow.Subscriber<T> {
        private final java.util.List<T> items = new java.util.concurrent.CopyOnWriteArrayList<>();
        private final CountDownLatch terminal = new CountDownLatch(1);
        private volatile Flow.Subscription subscription;
        private volatile Throwable failure;
        private volatile boolean completed;

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
        }

        @Override
        public void onNext(T item) {
            items.add(item);
        }

        @Override
        public void onError(Throwable failure) {
            this.failure = failure;
            terminal.countDown();
        }

        @Override
        public void onComplete() {
            completed = true;
            terminal.countDown();
        }

        void request(long count) {
            subscription.request(count);
        }

        void cancel() {
            subscription.cancel();
        }

        java.util.List<T> items() {
            return java.util.List.copyOf(items);
        }

        boolean completed() {
            return completed;
        }

        void awaitTerminal() throws InterruptedException {
            assertTrue(terminal.await(5, TimeUnit.SECONDS));
            if (failure != null) {
                throw new AssertionError("stream failed", failure);
            }
        }
    }

    private static final class ControlledPublisher<T> implements Flow.Publisher<T> {
        private final AtomicReference<Flow.Subscriber<? super T>> subscriber = new AtomicReference<>();
        private final java.util.concurrent.atomic.AtomicLong demand = new java.util.concurrent.atomic.AtomicLong();
        private volatile boolean completeOnSubscribe;
        private volatile boolean cancelled;

        @Override
        public void subscribe(Flow.Subscriber<? super T> subscriber) {
            if (!this.subscriber.compareAndSet(null, subscriber)) {
                throw new IllegalStateException("single subscription publisher");
            }
            subscriber.onSubscribe(new Flow.Subscription() {
                @Override
                public void request(long count) {
                    demand.accumulateAndGet(count, (left, right) -> {
                        long sum = left + right;
                        return sum < 0L ? Long.MAX_VALUE : sum;
                    });
                }

                @Override
                public void cancel() {
                    cancelled = true;
                }
            });
            if (completeOnSubscribe) {
                subscriber.onComplete();
            }
        }

        long demand() {
            return demand.get();
        }

        void emit(T item) {
            if (demand.getAndUpdate(current -> current == Long.MAX_VALUE ? current : current - 1L) <= 0L) {
                throw new IllegalStateException("emitted without demand");
            }
            subscriber.get().onNext(item);
        }

        void complete() {
            Flow.Subscriber<? super T> resolved = subscriber.get();
            if (resolved != null) {
                resolved.onComplete();
            } else {
                completeOnSubscribe = true;
            }
        }

        void completeOnSubscribe() {
            completeOnSubscribe = true;
        }

        boolean cancelled() {
            return cancelled;
        }
    }
}
