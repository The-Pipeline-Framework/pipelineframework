/*
 * Copyright (c) 2023-2026 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework.awaitable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import jakarta.enterprise.context.ApplicationScoped;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;

/**
 * In-memory bridge from durable await completion admission to a live brokered await stream.
 */
@ApplicationScoped
public class AwaitLiveCompletionRegistry {

    private final ConcurrentMap<Key, LiveAwaitSession<?>> sessions = new ConcurrentHashMap<>();

    public <O> LiveAwaitSession<O> open(AwaitStepDescriptor descriptor, String tenantId, String unitId) {
        Objects.requireNonNull(descriptor, "descriptor must not be null");
        Objects.requireNonNull(tenantId, "tenantId must not be null");
        Objects.requireNonNull(unitId, "unitId must not be null");
        Class<?> outputType = resolveOutputType(descriptor);
        Key key = new Key(tenantId, unitId);
        LiveAwaitSession<O> session = new LiveAwaitSession<>(
            key,
            outputType,
            () -> sessions.remove(key));
        LiveAwaitSession<?> existing = sessions.putIfAbsent(key, session);
        if (existing != null) {
            throw new IllegalStateException("A live await stream already owns await unit " + unitId);
        }
        return session;
    }

    public Uni<Boolean> signal(AwaitInteractionRecord record, AwaitUnitRecord unit) {
        if (record == null || unit == null) {
            return Uni.createFrom().item(false);
        }
        LiveAwaitSession<?> session = sessions.get(new Key(record.tenantId(), record.unitId()));
        if (session == null) {
            return Uni.createFrom().item(false);
        }
        return session.enqueue(record).replaceWith(Boolean.TRUE);
    }

    public void close(String tenantId, String unitId) {
        if (tenantId != null && unitId != null) {
            sessions.remove(new Key(tenantId, unitId));
        }
    }

    private static Class<?> resolveOutputType(AwaitStepDescriptor descriptor) {
        try {
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            if (loader == null) {
                loader = AwaitPayloadSupport.class.getClassLoader();
            }
            return AwaitPayloadSupport.resolvePayloadClass(descriptor.outputType(), loader);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(
                "Failed resolving await output type " + descriptor.outputType()
                    + " for live await stream " + descriptor.stepId(),
                e);
        }
    }

    private record Key(String tenantId, String unitId) {
    }

    public static final class LiveAwaitSession<O> implements Flow.Publisher<O> {
        private final Key key;
        private final Class<?> outputType;
        private final Runnable closeHook;
        private final Object lock = new Object();
        private final ArrayDeque<Pending<O>> pending = new ArrayDeque<>();
        private final Set<String> seenCompletions = new HashSet<>();
        private final Set<String> acceptedCompletions = new HashSet<>();
        private final Map<String, CompletableFuture<Void>> acceptedWaiters = new HashMap<>();
        private final Set<String> activePermits = new HashSet<>();
        private final Map<String, CompletableFuture<Void>> permitWaiters = new LinkedHashMap<>();
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private Flow.Subscriber<? super O> subscriber;
        private long requested;
        private boolean dispatchComplete;
        private int expectedItemCount;
        private int emittedItemCount;
        private boolean subscriberReady;
        private boolean cancelled;
        private boolean terminated;
        private boolean terminalSignalDelivered;
        private Throwable terminalFailure;
        private boolean draining;
        private boolean drainAgain;

        private LiveAwaitSession(Key key, Class<?> outputType, Runnable closeHook) {
            this.key = key;
            this.outputType = outputType;
            this.closeHook = closeHook;
        }

        @Override
        public void subscribe(Flow.Subscriber<? super O> subscriber) {
            Objects.requireNonNull(subscriber, "subscriber must not be null");
            boolean reject = false;
            synchronized (lock) {
                if (this.subscriber == null) {
                    this.subscriber = subscriber;
                } else {
                    reject = true;
                }
            }
            if (reject) {
                subscriber.onSubscribe(NoopSubscription.INSTANCE);
                subscriber.onError(new IllegalStateException("Live await streams support a single subscriber"));
                return;
            }
            subscriber.onSubscribe(new LiveSubscription());
            synchronized (lock) {
                subscriberReady = true;
            }
            drain();
        }

        /**
         * Adds a durably admitted completion to this session's bounded handoff queue.
         * A successful result means the session owns the completion; it does not mean
         * downstream has consumed it yet.
         */
        public Uni<Void> enqueue(AwaitInteractionRecord record) {
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
            synchronized (lock) {
                if (cancelled || terminated) {
                    return Uni.createFrom().failure(new IllegalStateException(
                        "Live await stream is no longer accepting completions for unit " + key.unitId()));
                }
                if (!seenCompletions.add(completionKey)) {
                    return Uni.createFrom().voidItem();
                }
                pending.addLast(new Pending<>(completionKey, payload));
            }
            scheduleDrain();
            return Uni.createFrom().voidItem();
        }

        /**
         * Compatibility delivery acknowledgement. The result completes only after
         * downstream receives the item and the local dispatch permit is released.
         */
        public Uni<Void> accept(AwaitInteractionRecord record) {
            return enqueue(record).chain(() -> awaitAccepted(record));
        }

        public Uni<Void> awaitAccepted(AwaitInteractionRecord record) {
            String completionKey = completionKey(record);
            synchronized (lock) {
                if (acceptedCompletions.contains(completionKey)) {
                    return Uni.createFrom().voidItem();
                }
                if (cancelled || terminated) {
                    return Uni.createFrom().failure(new IllegalStateException(
                        "Live await stream is no longer accepting completions for unit " + key.unitId()));
                }
                return Uni.createFrom().completionStage(
                    acceptedWaiters.computeIfAbsent(completionKey, ignored -> new CompletableFuture<>()));
            }
        }

        /**
         * Acquires one unresolved-interaction slot before dispatching an item.
         * The slot remains held until the completion is accepted by the live
         * subscriber or the session terminates.
         */
        public Uni<Void> acquirePermit(String completionKey, int maxPendingInteractions) {
            if (completionKey == null || completionKey.isBlank()) {
                return Uni.createFrom().failure(new IllegalArgumentException("completionKey must not be blank"));
            }
            if (maxPendingInteractions < 1) {
                return Uni.createFrom().failure(new IllegalArgumentException(
                    "maxPendingInteractions must be positive"));
            }
            synchronized (lock) {
                if (cancelled || terminated) {
                    return Uni.createFrom().failure(new IllegalStateException(
                        "Live await stream is no longer accepting dispatches for unit " + key.unitId()));
                }
                if (activePermits.contains(completionKey)) {
                    return Uni.createFrom().voidItem();
                }
                CompletableFuture<Void> waiting = permitWaiters.get(completionKey);
                if (waiting != null) {
                    return Uni.createFrom().completionStage(waiting);
                }
                if (activePermits.size() < maxPendingInteractions) {
                    activePermits.add(completionKey);
                    return Uni.createFrom().voidItem();
                }
                CompletableFuture<Void> waiter = new CompletableFuture<>();
                permitWaiters.put(completionKey, waiter);
                return Uni.createFrom().completionStage(waiter);
            }
        }

        public void markDispatchComplete(int expectedItemCount) {
            synchronized (lock) {
                this.dispatchComplete = true;
                this.expectedItemCount = Math.max(0, expectedItemCount);
            }
            drain();
        }

        public void fail(Throwable failure) {
            Objects.requireNonNull(failure, "failure must not be null");
            List<Pending<O>> rejected;
            Flow.Subscriber<? super O> current;
            synchronized (lock) {
                if (terminated) {
                    return;
                }
                terminated = true;
                terminalFailure = failure;
                rejected = new ArrayList<>(pending);
                pending.clear();
                if (subscriberReady && subscriber != null) {
                    terminalSignalDelivered = true;
                    current = subscriber;
                } else {
                    current = null;
                }
            }
            failAcceptedWaiters(failure);
            failPermitWaiters(failure);
            if (current != null) {
                current.onError(failure);
            }
            close();
        }

        private void request(long n) {
            if (n <= 0) {
                fail(new IllegalArgumentException("request amount must be positive"));
                return;
            }
            synchronized (lock) {
                requested = addCap(requested, n);
            }
            drain();
        }

        private void scheduleDrain() {
            Infrastructure.getDefaultExecutor().execute(this::drain);
        }

        private void cancel() {
            List<Pending<O>> rejected;
            synchronized (lock) {
                if (terminated) {
                    return;
                }
                cancelled = true;
                terminated = true;
                rejected = new ArrayList<>(pending);
                pending.clear();
            }
            IllegalStateException failure = new IllegalStateException(
                "Live await stream cancelled for unit " + key.unitId());
            failAcceptedWaiters(failure);
            failPermitWaiters(failure);
            close();
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
                List<Pending<O>> toEmit = new ArrayList<>();
                Flow.Subscriber<? super O> toComplete = null;
                Throwable toFail = null;
                synchronized (lock) {
                    while (!terminated && subscriberReady && subscriber != null && requested > 0 && !pending.isEmpty()) {
                        Pending<O> next = pending.removeFirst();
                        if (requested != Long.MAX_VALUE) {
                            requested--;
                        }
                        toEmit.add(next);
                    }
                }
                for (Pending<O> item : toEmit) {
                    try {
                        subscriber.onNext(item.item());
                        completeAcceptedWaiter(item.completionKey());
                    } catch (Throwable failure) {
                        failAcceptedWaiter(item.completionKey(), failure);
                        fail(failure);
                        finishDrain();
                        return;
                    }
                }
                synchronized (lock) {
                    emittedItemCount += toEmit.size();
                    if (!terminalSignalDelivered
                        && terminated
                        && terminalFailure != null
                        && subscriberReady
                        && subscriber != null) {
                        terminalSignalDelivered = true;
                        toFail = terminalFailure;
                    } else if (!terminated
                        && subscriber != null
                        && subscriberReady
                        && dispatchComplete
                        && pending.isEmpty()
                        && emittedItemCount >= expectedItemCount) {
                        terminated = true;
                        terminalSignalDelivered = true;
                        toComplete = subscriber;
                    }
                    if (toComplete != null || toFail != null) {
                        draining = false;
                        drainAgain = false;
                    } else if (!drainAgain) {
                        draining = false;
                    } else {
                        drainAgain = false;
                        continue;
                    }
                }
                if (toFail != null) {
                    subscriber.onError(toFail);
                    close();
                }
                if (toComplete != null) {
                    toComplete.onComplete();
                    close();
                }
                return;
            }
        }

        private void finishDrain() {
            synchronized (lock) {
                draining = false;
                drainAgain = false;
            }
        }

        private void close() {
            if (closed.compareAndSet(false, true)) {
                closeHook.run();
            }
        }

        private void completeAcceptedWaiter(String completionKey) {
            CompletableFuture<Void> waiter;
            synchronized (lock) {
                acceptedCompletions.add(completionKey);
                waiter = acceptedWaiters.remove(completionKey);
            }
            if (waiter != null) {
                waiter.complete(null);
            }
            releasePermit(completionKey);
        }

        private void failAcceptedWaiter(String completionKey, Throwable failure) {
            CompletableFuture<Void> waiter;
            synchronized (lock) {
                waiter = acceptedWaiters.remove(completionKey);
            }
            if (waiter != null) {
                waiter.completeExceptionally(failure);
            }
        }

        private void failAcceptedWaiters(Throwable failure) {
            List<CompletableFuture<Void>> waiters;
            synchronized (lock) {
                waiters = new ArrayList<>(acceptedWaiters.values());
                acceptedWaiters.clear();
            }
            for (CompletableFuture<Void> waiter : waiters) {
                waiter.completeExceptionally(failure);
            }
        }

        private void releasePermit(String completionKey) {
            CompletableFuture<Void> next = null;
            synchronized (lock) {
                if (!activePermits.remove(completionKey)) {
                    return;
                }
                var iterator = permitWaiters.entrySet().iterator();
                if (iterator.hasNext()) {
                    Map.Entry<String, CompletableFuture<Void>> entry = iterator.next();
                    iterator.remove();
                    activePermits.add(entry.getKey());
                    next = entry.getValue();
                }
            }
            if (next != null) {
                next.complete(null);
            }
        }

        private void failPermitWaiters(Throwable failure) {
            List<CompletableFuture<Void>> waiters;
            synchronized (lock) {
                waiters = new ArrayList<>(permitWaiters.values());
                permitWaiters.clear();
                activePermits.clear();
            }
            for (CompletableFuture<Void> waiter : waiters) {
                waiter.completeExceptionally(failure);
            }
        }

        private static long addCap(long current, long increment) {
            long updated = current + increment;
            return updated < 0 ? Long.MAX_VALUE : updated;
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

        private record Pending<O>(String completionKey, O item) {
        }
    }
}
