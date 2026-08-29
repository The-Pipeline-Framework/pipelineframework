package org.pipelineframework.connector;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.pipelineframework.runtime.core.RuntimeAdapters;

/**
 * Applies the execution and concurrency semantics declared by connector operation interfaces.
 *
 * <p>Serialization is keyed by binding name plus operation ID and major version. Separate bindings
 * remain independent without introducing provider-wide or connection-wide concurrency metadata.
 */
public final class ConnectorOperationInvocationCoordinator {
    private final Map<InvocationKey, CompletableFuture<Void>> serializedTails = new HashMap<>();

    public <T> CompletionStage<T> invoke(
        ConnectorBindingName binding,
        ConnectorOperation operation,
        Supplier<? extends CompletionStage<T>> invocation
    ) {
        Objects.requireNonNull(binding, "connector binding must not be null");
        Objects.requireNonNull(operation, "operation must not be null");
        Objects.requireNonNull(invocation, "invocation must not be null");
        if (!(operation instanceof SerializedOperation)) {
            return invokeOperation(operation, invocation);
        }
        return invokeSerialized(binding, operation, invocation);
    }

    /**
     * Coordinates a finite streaming operation without changing its demand protocol.
     *
     * <p>Provider invocation is deferred until first demand. Serialized ownership is retained until
     * both the row publisher and the provider resource-lifetime stage have terminated.</p>
     */
    public <T> Flow.Publisher<T> invokeStream(
        ConnectorBindingName binding,
        ConnectorOperation operation,
        Supplier<? extends QueryStream<T>> invocation
    ) {
        Objects.requireNonNull(binding, "connector binding must not be null");
        Objects.requireNonNull(operation, "operation must not be null");
        Objects.requireNonNull(invocation, "invocation must not be null");
        return downstream -> {
            Objects.requireNonNull(downstream, "stream subscriber must not be null");
            StreamInvocation<T> coordinated = new StreamInvocation<>(downstream, binding, operation, invocation);
            downstream.onSubscribe(coordinated);
        };
    }

    private <T> CompletionStage<T> invokeSerialized(
        ConnectorBindingName binding,
        ConnectorOperation operation,
        Supplier<? extends CompletionStage<T>> invocation
    ) {
        InvocationKey key = new InvocationKey(binding, operation.id(), operation.majorVersion());
        CompletableFuture<Void> turn = new CompletableFuture<>();
        CompletableFuture<Void> predecessor;
        synchronized (serializedTails) {
            predecessor = serializedTails.put(key, turn);
        }
        SerializedInvocationFuture<T> result = new SerializedInvocationFuture<>();
        CompletionStage<Void> ready = predecessor == null
            ? CompletableFuture.completedFuture(null)
            : predecessor;
        ready.whenComplete((ignored, predecessorFailure) -> {
            if (!result.start()) {
                release(key, turn);
                return;
            }
            try {
                if (operation instanceof BlockingOperation) {
                    CompletionStage<CompletionStage<T>> admission = RuntimeAdapters.executeBlocking(
                        () -> requireStage(invocation.get()), false);
                    admission.whenComplete((providerStage, failure) -> {
                        if (failure == null) {
                            observeProviderStage(key, turn, result, providerStage);
                        } else {
                            failAndRelease(key, turn, result, failure);
                        }
                    });
                } else {
                    observeProviderStage(key, turn, result, invokeOperation(operation, invocation));
                }
            } catch (Throwable failure) {
                failAndRelease(key, turn, result, failure);
            }
        });
        return result;
    }

    private <T> void observeProviderStage(
        InvocationKey key,
        CompletableFuture<Void> turn,
        SerializedInvocationFuture<T> result,
        CompletionStage<T> providerStage
    ) {
        try {
            result.providerStage(providerStage);
            providerStage.whenComplete((value, failure) -> {
                if (!result.isCancelled()) {
                    if (failure == null) {
                        result.complete(value);
                    } else {
                        result.completeExceptionally(failure);
                    }
                }
                release(key, turn);
            });
        } catch (Throwable failure) {
            failAndRelease(key, turn, result, failure);
        }
    }

    private <T> void failAndRelease(
        InvocationKey key,
        CompletableFuture<Void> turn,
        SerializedInvocationFuture<T> result,
        Throwable failure
    ) {
        if (!result.isCancelled()) {
            result.completeExceptionally(failure);
        }
        release(key, turn);
    }

    private void release(InvocationKey key, CompletableFuture<Void> turn) {
        synchronized (serializedTails) {
            serializedTails.remove(key, turn);
        }
        turn.complete(null);
    }

    private record InvocationKey(ConnectorBindingName binding, String operationId, int majorVersion) {
    }

    private static <T> CompletionStage<T> invokeOperation(
        ConnectorOperation operation,
        Supplier<? extends CompletionStage<T>> invocation
    ) {
        if (operation instanceof BlockingOperation) {
            return RuntimeAdapters.executeBlocking(invocation::get, false)
                .thenCompose(ConnectorOperationInvocationCoordinator::requireStage);
        }
        try {
            return requireStage(invocation.get());
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private static <T> CompletionStage<T> requireStage(CompletionStage<T> stage) {
        if (stage == null) {
            return CompletableFuture.failedFuture(
                new IllegalStateException("connector operation returned a null CompletionStage"));
        }
        return stage;
    }

    private static <T> QueryStream<T> requireStream(QueryStream<T> stream) {
        return Objects.requireNonNull(stream, "connector operation returned a null QueryStream");
    }

    private final class StreamInvocation<T> implements Flow.Subscription, Flow.Subscriber<T> {
        private final Flow.Subscriber<? super T> downstream;
        private final ConnectorBindingName binding;
        private final ConnectorOperation operation;
        private final Supplier<? extends QueryStream<T>> invocation;
        private final Object demandLock = new Object();
        private final AtomicBoolean admitted = new AtomicBoolean();
        private final AtomicBoolean providerStarted = new AtomicBoolean();
        private final AtomicBoolean released = new AtomicBoolean();
        private Flow.Subscription upstream;
        private long pendingDemand;
        private volatile boolean cancelled;
        private volatile boolean publisherTerminated;
        private volatile boolean resourcesTerminated;
        private volatile Throwable publisherFailure;
        private volatile Throwable resourceFailure;
        private volatile InvocationKey key;
        private volatile CompletableFuture<Void> turn;

        private StreamInvocation(
            Flow.Subscriber<? super T> downstream,
            ConnectorBindingName binding,
            ConnectorOperation operation,
            Supplier<? extends QueryStream<T>> invocation
        ) {
            this.downstream = downstream;
            this.binding = binding;
            this.operation = operation;
            this.invocation = invocation;
        }

        @Override
        public void request(long count) {
            if (count <= 0L) {
                terminatePublisher(new IllegalArgumentException("stream demand must be positive"), true);
                return;
            }
            Flow.Subscription resolved;
            synchronized (demandLock) {
                if (cancelled || publisherTerminated) {
                    return;
                }
                resolved = upstream;
                if (resolved == null) {
                    pendingDemand = addCap(pendingDemand, count);
                }
            }
            if (resolved != null) {
                resolved.request(count);
            }
            admit();
        }

        @Override
        public void cancel() {
            Flow.Subscription resolved;
            synchronized (demandLock) {
                if (cancelled) {
                    return;
                }
                cancelled = true;
                publisherTerminated = true;
                resolved = upstream;
            }
            if (resolved != null) {
                resolved.cancel();
            }
            if (!providerStarted.get()) {
                releaseStream();
            } else {
                completeIfTerminated();
            }
        }

        private void admit() {
            if (!admitted.compareAndSet(false, true)) {
                return;
            }
            if (cancelled) {
                releaseStream();
                return;
            }
            if (operation instanceof SerializedOperation) {
                key = new InvocationKey(binding, operation.id(), operation.majorVersion());
                turn = new CompletableFuture<>();
                CompletableFuture<Void> predecessor;
                synchronized (serializedTails) {
                    predecessor = serializedTails.put(key, turn);
                }
                CompletionStage<Void> ready = predecessor == null
                    ? CompletableFuture.completedFuture(null)
                    : predecessor;
                ready.whenComplete((ignored, failure) -> {
                    if (cancelled) {
                        releaseStream();
                    } else {
                        invokeProvider();
                    }
                });
                return;
            }
            invokeProvider();
        }

        private void invokeProvider() {
            providerStarted.set(true);
            if (cancelled) {
                releaseStream();
                return;
            }
            if (operation instanceof BlockingOperation) {
                try {
                    RuntimeAdapters.executeBlocking(() -> requireStream(invocation.get()), false)
                        .whenComplete((stream, failure) -> {
                            if (failure == null) {
                                subscribeProvider(stream);
                            } else {
                                failBeforeStream(failure);
                            }
                        });
                } catch (Throwable failure) {
                    failBeforeStream(failure);
                }
                return;
            }
            try {
                subscribeProvider(requireStream(invocation.get()));
            } catch (Throwable failure) {
                failBeforeStream(failure);
            }
        }

        private void subscribeProvider(QueryStream<T> stream) {
            try {
                stream.termination().whenComplete((ignored, terminationFailure) -> {
                    resourceFailure = terminationFailure;
                    resourcesTerminated = true;
                    completeIfTerminated();
                });
                stream.rows().subscribe(this);
            } catch (Throwable subscriptionFailure) {
                publisherFailure = subscriptionFailure;
                publisherTerminated = true;
                completeIfTerminated();
            }
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            Objects.requireNonNull(subscription, "provider subscription must not be null");
            long demand;
            synchronized (demandLock) {
                if (upstream != null) {
                    subscription.cancel();
                    return;
                }
                upstream = subscription;
                demand = pendingDemand;
                pendingDemand = 0L;
                if (cancelled) {
                    subscription.cancel();
                    return;
                }
            }
            if (demand > 0L) {
                subscription.request(demand);
            }
        }

        @Override
        public void onNext(T item) {
            if (cancelled || publisherTerminated) {
                return;
            }
            if (item == null) {
                terminatePublisher(new NullPointerException("streaming Query emitted a null row"), true);
                return;
            }
            downstream.onNext(item);
        }

        @Override
        public void onError(Throwable failure) {
            terminatePublisher(
                Objects.requireNonNull(failure, "streaming Query failure must not be null"), false);
        }

        @Override
        public void onComplete() {
            synchronized (demandLock) {
                if (publisherTerminated) {
                    return;
                }
                publisherTerminated = true;
            }
            completeIfTerminated();
        }

        private void terminatePublisher(Throwable failure, boolean cancelUpstream) {
            Flow.Subscription resolved;
            synchronized (demandLock) {
                if (publisherTerminated) {
                    return;
                }
                publisherFailure = failure;
                publisherTerminated = true;
                resolved = upstream;
            }
            if (cancelUpstream && resolved != null) {
                resolved.cancel();
            }
            if (!providerStarted.get()) {
                failBeforeStream(failure);
            } else {
                completeIfTerminated();
            }
        }

        private void completeIfTerminated() {
            if (!publisherTerminated || !resourcesTerminated || !released.compareAndSet(false, true)) {
                return;
            }
            releaseTurn();
            if (cancelled) {
                return;
            }
            Throwable failure = publisherFailure == null ? resourceFailure : publisherFailure;
            if (failure == null) {
                downstream.onComplete();
            } else {
                downstream.onError(failure);
            }
        }

        private void failBeforeStream(Throwable failure) {
            if (!released.compareAndSet(false, true)) {
                return;
            }
            publisherFailure = failure;
            publisherTerminated = true;
            releaseTurn();
            if (!cancelled) {
                downstream.onError(failure);
            }
        }

        private void releaseStream() {
            released.compareAndSet(false, true);
            releaseTurn();
        }

        private void releaseTurn() {
            if (key != null && turn != null) {
                release(key, turn);
            }
        }

        private long addCap(long left, long right) {
            long sum = left + right;
            return sum < 0L ? Long.MAX_VALUE : sum;
        }
    }

    private static final class SerializedInvocationFuture<T> extends CompletableFuture<T> {
        private final AtomicBoolean started = new AtomicBoolean();
        private final AtomicReference<CompletionStage<T>> providerStage = new AtomicReference<>();

        private boolean start() {
            return !isCancelled() && started.compareAndSet(false, true);
        }

        private void providerStage(CompletionStage<T> stage) {
            providerStage.set(stage);
            if (isCancelled()) {
                cancelProvider(stage, true);
            }
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            boolean cancelled = super.cancel(mayInterruptIfRunning);
            CompletionStage<T> stage = providerStage.get();
            if (cancelled && started.get() && stage != null) {
                cancelProvider(stage, mayInterruptIfRunning);
            }
            return cancelled;
        }

        private static void cancelProvider(CompletionStage<?> stage, boolean mayInterruptIfRunning) {
            try {
                stage.toCompletableFuture().cancel(mayInterruptIfRunning);
            } catch (RuntimeException ignored) {
                // CompletionStage cancellation is optional; forwarding is best-effort.
            }
        }
    }
}
