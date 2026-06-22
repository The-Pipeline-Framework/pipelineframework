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

package org.pipelineframework.blocking;

import java.util.List;
import java.util.concurrent.Flow;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;

import io.quarkus.arc.Unremovable;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;
import org.pipelineframework.context.TransportDispatchMetadata;
import org.pipelineframework.context.TransportDispatchMetadataHolder;

/**
 * Offloads blocking callbacks to a worker executor by default, with an opt-in virtual-thread path.
 */
@ApplicationScoped
@Unremovable
public class BlockingExecutionSupport {

    private final ExecutorService virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();

    public <T> Uni<T> supply(boolean useVirtualThreads, Supplier<T> supplier) {
        PipelineContext context = PipelineContextHolder.get();
        TransportDispatchMetadata transport = TransportDispatchMetadataHolder.get();
        return Uni.createFrom()
            .item(() -> withCapturedContext(context, transport, supplier))
            .runSubscriptionOn(selectExecutor(useVirtualThreads));
    }

    public <T> Multi<T> emitList(boolean useVirtualThreads, Supplier<List<T>> supplier) {
        return supply(useVirtualThreads, supplier)
            .onItem()
            .transformToMulti(items -> items == null
                ? Multi.createFrom().empty()
                : Multi.createFrom().iterable(items));
    }

    public <T> Multi<T> emitIterator(boolean useVirtualThreads, Supplier<? extends CloseableIterator<T>> supplier) {
        PipelineContext context = PipelineContextHolder.get();
        TransportDispatchMetadata transport = TransportDispatchMetadataHolder.get();
        Executor executor = selectExecutor(useVirtualThreads);
        return Multi.createFrom().publisher(subscriber ->
            subscriber.onSubscribe(new IteratorSubscription<>(subscriber, supplier, executor, context, transport)));
    }

    private Executor selectExecutor(boolean useVirtualThreads) {
        return useVirtualThreads ? virtualThreadExecutor : Infrastructure.getDefaultWorkerPool();
    }

    private static <T> T withCapturedContext(
        PipelineContext context,
        TransportDispatchMetadata transport,
        Supplier<T> supplier
    ) {
        PipelineContext previousContext = PipelineContextHolder.get();
        TransportDispatchMetadata previousTransport = TransportDispatchMetadataHolder.get();
        if (context != null) {
            PipelineContextHolder.set(context);
        } else {
            PipelineContextHolder.clear();
        }
        if (transport != null) {
            TransportDispatchMetadataHolder.set(transport);
        } else {
            TransportDispatchMetadataHolder.clear();
        }
        try {
            return supplier.get();
        } finally {
            if (previousContext != null) {
                PipelineContextHolder.set(previousContext);
            } else {
                PipelineContextHolder.clear();
            }
            if (previousTransport != null) {
                TransportDispatchMetadataHolder.set(previousTransport);
            } else {
                TransportDispatchMetadataHolder.clear();
            }
        }
    }

    @PreDestroy
    void close() {
        virtualThreadExecutor.shutdown();
    }

    private static final class IteratorSubscription<T> implements Flow.Subscription {
        private final Flow.Subscriber<? super T> subscriber;
        private final Supplier<? extends CloseableIterator<T>> supplier;
        private final Executor executor;
        private final PipelineContext context;
        private final TransportDispatchMetadata transport;
        private final AtomicLong requested = new AtomicLong();
        private final AtomicInteger workInProgress = new AtomicInteger();
        private CloseableIterator<T> iterator;
        private volatile boolean closed;
        private volatile boolean completed;

        private IteratorSubscription(
            Flow.Subscriber<? super T> subscriber,
            Supplier<? extends CloseableIterator<T>> supplier,
            Executor executor,
            PipelineContext context,
            TransportDispatchMetadata transport
        ) {
            this.subscriber = subscriber;
            this.supplier = supplier;
            this.executor = executor;
            this.context = context;
            this.transport = transport;
        }

        @Override
        public void request(long n) {
            if (n <= 0) {
                fail(new IllegalArgumentException("request amount must be positive"));
                return;
            }
            addRequested(n);
            scheduleDrain();
        }

        @Override
        public void cancel() {
            close();
        }

        private void scheduleDrain() {
            if (workInProgress.getAndIncrement() == 0) {
                executor.execute(this::drainWithContext);
            }
        }

        private void drainWithContext() {
            withCapturedContext(context, transport, () -> {
                drain();
                return null;
            });
        }

        private void drain() {
            int missed = 1;
            while (true) {
                while (requested.get() > 0 && !closed && !completed) {
                    T item;
                    try {
                        if (iterator == null) {
                            iterator = supplier.get();
                            if (iterator == null) {
                                completed = true;
                                subscriber.onComplete();
                                return;
                            }
                        }
                        if (!iterator.hasNext()) {
                            completed = true;
                            closeIterator();
                            subscriber.onComplete();
                            return;
                        }
                        item = iterator.next();
                    } catch (Throwable failure) {
                        fail(failure);
                        return;
                    }
                    if (item == null) {
                        fail(new NullPointerException("Blocking iterator emitted null item"));
                        return;
                    }
                    requested.decrementAndGet();
                    try {
                        subscriber.onNext(item);
                    } catch (Throwable failure) {
                        fail(failure);
                        return;
                    }
                }
                missed = workInProgress.addAndGet(-missed);
                if (missed == 0) {
                    return;
                }
            }
        }

        private void addRequested(long n) {
            requested.updateAndGet(current -> {
                long updated = current + n;
                return updated < 0 ? Long.MAX_VALUE : updated;
            });
        }

        private void fail(Throwable failure) {
            if (closed) {
                return;
            }
            closed = true;
            try {
                closeIterator();
            } catch (Throwable closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            subscriber.onError(failure);
        }

        private void close() {
            if (closed || completed) {
                return;
            }
            closed = true;
            executor.execute(() -> withCapturedContext(context, transport, () -> {
                closeIterator();
                return null;
            }));
        }

        private void closeIterator() {
            if (iterator == null) {
                return;
            }
            try {
                iterator.close();
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException("Failed to close blocking iterator", e);
            } finally {
                iterator = null;
            }
        }
    }
}
