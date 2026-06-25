package org.pipelineframework.awaitable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

import io.smallrye.mutiny.Uni;

final class LiveAwaitSession<O> implements Flow.Publisher<O> {

    private final String unitId;
    private final Class<?> outputType;
    private final Runnable closeHook;
    private final Object lock = new Object();
    private final Map<String, CompletableFuture<Void>> acceptanceFutures = new HashMap<>();
    private final Map<String, CompletableFuture<Void>> acceptedWaiters = new HashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private LiveAwaitSessionState<O> state = LiveAwaitSessionState.initial();
    private Flow.Subscriber<? super O> subscriber;
    private boolean draining;
    private boolean drainAgain;

    LiveAwaitSession(String unitId, Class<?> outputType, Runnable closeHook) {
        this.unitId = Objects.requireNonNull(unitId, "unitId must not be null");
        this.outputType = Objects.requireNonNull(outputType, "outputType must not be null");
        this.closeHook = Objects.requireNonNull(closeHook, "closeHook must not be null");
    }

    @Override
    public void subscribe(Flow.Subscriber<? super O> subscriber) {
        Objects.requireNonNull(subscriber, "subscriber must not be null");
        boolean reject;
        List<LiveAwaitSessionEffect<O>> effects = List.of();
        synchronized (lock) {
            reject = state.subscriberAttached();
            if (!reject) {
                effects = apply(state.attachSubscriber());
                this.subscriber = subscriber;
            }
        }
        interpret(effects);
        if (reject) {
            subscriber.onSubscribe(NoopSubscription.INSTANCE);
            subscriber.onError(new IllegalStateException("Live await streams support a single subscriber"));
            return;
        }
        subscriber.onSubscribe(new LiveSubscription());
        List<LiveAwaitSessionEffect<O>> readyEffects;
        synchronized (lock) {
            readyEffects = apply(state.markSubscriberReady());
        }
        interpret(readyEffects);
        drain();
    }

    public Uni<Void> accept(AwaitInteractionRecord record) {
        Objects.requireNonNull(record, "record must not be null");
        if (record.status() != AwaitInteractionStatus.COMPLETED) {
            IllegalStateException failure = new IllegalStateException(
                "Live await stream received terminal interaction " + record.interactionId()
                    + " with status " + record.status());
            fail(failure);
            return Uni.createFrom().failure(failure);
        }
        String completionKey = completionKey(record);
        O payload;
        try {
            @SuppressWarnings("unchecked")
            O coerced = (O) AwaitPayloadSupport.coercePayload(record.responsePayload(), outputType);
            payload = coerced;
        } catch (Throwable failure) {
            fail(failure);
            return Uni.createFrom().failure(failure);
        }

        CompletableFuture<Void> accepted = new CompletableFuture<>();
        List<LiveAwaitSessionEffect<O>> effects;
        synchronized (lock) {
            if (!state.canAcceptCompletion()) {
                accepted.completeExceptionally(new IllegalStateException(
                    "Live await stream is no longer accepting completions for unit " + unitId));
                return Uni.createFrom().completionStage(accepted);
            }
            if (state.duplicate(completionKey)) {
                accepted.complete(null);
                return Uni.createFrom().completionStage(accepted);
            }
            acceptanceFutures.put(completionKey, accepted);
            effects = apply(state.admitCompletion(completionKey, payload));
        }
        interpret(effects);
        drain();
        return Uni.createFrom().completionStage(accepted);
    }

    public Uni<Void> awaitAccepted(AwaitInteractionRecord record) {
        String completionKey = completionKey(record);
        synchronized (lock) {
            if (state.accepted(completionKey)) {
                return Uni.createFrom().voidItem();
            }
            if (!state.canAcceptCompletion()) {
                return Uni.createFrom().failure(new IllegalStateException(
                    "Live await stream is no longer accepting completions for unit " + unitId));
            }
            return Uni.createFrom().completionStage(
                acceptedWaiters.computeIfAbsent(completionKey, ignored -> new CompletableFuture<>()));
        }
    }

    public void markDispatchComplete(int expectedItemCount) {
        List<LiveAwaitSessionEffect<O>> effects;
        synchronized (lock) {
            effects = apply(state.markDispatchComplete(expectedItemCount));
        }
        interpret(effects);
        drain();
    }

    public void fail(Throwable failure) {
        Objects.requireNonNull(failure, "failure must not be null");
        List<LiveAwaitSessionEffect<O>> effects;
        synchronized (lock) {
            effects = apply(state.fail(failure));
        }
        interpret(effects);
        drain();
    }

    private void request(long n) {
        List<LiveAwaitSessionEffect<O>> effects;
        synchronized (lock) {
            effects = apply(state.request(n));
        }
        interpret(effects);
        drain();
    }

    private void cancel() {
        List<LiveAwaitSessionEffect<O>> effects;
        synchronized (lock) {
            effects = apply(state.cancel(unitId));
        }
        interpret(effects);
        drain();
    }

    private void drain() {
        synchronized (lock) {
            if (draining) {
                drainAgain = true;
                return;
            }
            draining = true;
        }
        while (true) {
            LiveAwaitSessionDecision<O> decision;
            synchronized (lock) {
                decision = state.drain();
                state = decision.state();
                if (decision.effects().isEmpty() && !drainAgain) {
                    draining = false;
                    return;
                }
                drainAgain = false;
            }
            interpret(decision.effects());
            synchronized (lock) {
                if (!drainAgain) {
                    draining = false;
                    return;
                }
            }
        }
    }

    private List<LiveAwaitSessionEffect<O>> apply(LiveAwaitSessionDecision<O> decision) {
        state = decision.state();
        return decision.effects();
    }

    private void interpret(Iterable<LiveAwaitSessionEffect<O>> effects) {
        for (LiveAwaitSessionEffect<O> effect : effects) {
            switch (effect) {
                case LiveAwaitSessionEffect.EmitItem<O> emit -> {
                    if (!emit(emit)) {
                        return;
                    }
                }
                case LiveAwaitSessionEffect.CompleteStream<O> ignored -> {
                    if (subscriber != null) {
                        subscriber.onComplete();
                    }
                }
                case LiveAwaitSessionEffect.FailStream<O> fail -> {
                    if (subscriber != null) {
                        subscriber.onError(fail.failure());
                    }
                }
                case LiveAwaitSessionEffect.FailAcceptance<O> fail -> failAcceptance(
                    fail.completionKey(),
                    fail.failure());
                case LiveAwaitSessionEffect.FailAcceptedWaiters<O> fail -> failAcceptedWaiters(fail.failure());
                case LiveAwaitSessionEffect.CloseSession<O> ignored -> close();
            }
        }
    }

    private boolean emit(LiveAwaitSessionEffect.EmitItem<O> emit) {
        try {
            subscriber.onNext(emit.item());
            completeAcceptance(emit.completionKey());
            return true;
        } catch (Throwable failure) {
            failAcceptance(emit.completionKey(), failure);
            fail(failure);
            return false;
        }
    }

    private void completeAcceptance(String completionKey) {
        CompletableFuture<Void> accepted;
        CompletableFuture<Void> waiter;
        synchronized (lock) {
            accepted = acceptanceFutures.remove(completionKey);
            waiter = acceptedWaiters.remove(completionKey);
            state = state.acceptanceCompleted(completionKey).state();
        }
        if (accepted != null) {
            accepted.complete(null);
        }
        if (waiter != null) {
            waiter.complete(null);
        }
    }

    private void failAcceptance(String completionKey, Throwable failure) {
        CompletableFuture<Void> accepted;
        CompletableFuture<Void> waiter;
        synchronized (lock) {
            accepted = acceptanceFutures.remove(completionKey);
            waiter = acceptedWaiters.remove(completionKey);
        }
        if (accepted != null) {
            accepted.completeExceptionally(failure);
        }
        if (waiter != null) {
            waiter.completeExceptionally(failure);
        }
    }

    private void failAcceptedWaiters(Throwable failure) {
        Map<String, CompletableFuture<Void>> waiters;
        synchronized (lock) {
            waiters = new HashMap<>(acceptedWaiters);
            acceptedWaiters.clear();
        }
        waiters.values().forEach(waiter -> waiter.completeExceptionally(failure));
    }

    private void close() {
        if (closed.compareAndSet(false, true)) {
            closeHook.run();
        }
    }

    private static String completionKey(AwaitInteractionRecord record) {
        return record.itemIndex() == null ? record.interactionId() : "item:" + record.itemIndex();
    }

    private final class LiveSubscription implements Flow.Subscription {
        @Override
        public void request(long n) {
            LiveAwaitSession.this.request(n);
        }

        @Override
        public void cancel() {
            LiveAwaitSession.this.cancel();
        }
    }

    private enum NoopSubscription implements Flow.Subscription {
        INSTANCE;

        @Override
        public void request(long n) {
        }

        @Override
        public void cancel() {
        }
    }
}
