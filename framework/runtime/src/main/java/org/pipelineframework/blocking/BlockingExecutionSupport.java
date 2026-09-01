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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
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
        return Multi.createFrom().publisher(openIterator(useVirtualThreads, supplier).rows());
    }

    public <T> BlockingIteratorPublisher<T> openIterator(
        boolean useVirtualThreads,
        Supplier<? extends CloseableIterator<T>> supplier
    ) {
        PipelineContext context = PipelineContextHolder.get();
        TransportDispatchMetadata transport = TransportDispatchMetadataHolder.get();
        Executor executor = selectExecutor(useVirtualThreads);
        CompletableFuture<Void> termination = new CompletableFuture<>();
        Flow.Publisher<T> rows = subscriber -> subscriber.onSubscribe(
            new IteratorSubscription<>(subscriber, supplier, executor, context, transport, termination));
        return new BlockingIteratorPublisher<>(rows, termination);
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

    private static void withCapturedContext(
        PipelineContext context,
        TransportDispatchMetadata transport,
        Runnable runnable
    ) {
        withCapturedContext(context, transport, () -> {
            runnable.run();
            return Boolean.TRUE;
        });
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
        private final CompletableFuture<Void> termination;
        private CloseableIterator<T> iterator;
        private volatile boolean closed;
        private volatile boolean completed;

        private IteratorSubscription(
            Flow.Subscriber<? super T> subscriber,
            Supplier<? extends CloseableIterator<T>> supplier,
            Executor executor,
            PipelineContext context,
            TransportDispatchMetadata transport,
            CompletableFuture<Void> termination
        ) {
            this.subscriber = subscriber;
            this.supplier = supplier;
            this.executor = executor;
            this.context = context;
            this.transport = transport;
            this.termination = termination;
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
            if (closed || completed) {
                return;
            }
            closed = true;
            scheduleDrain();
        }

        private void scheduleDrain() {
            if (workInProgress.getAndIncrement() == 0) {
                executor.execute(this::drainWithContext);
            }
        }

        private void drainWithContext() {
            withCapturedContext(context, transport, this::drain);
        }

        private void drain() {
            int missed = 1;
            while (true) {
                if (closed) {
                    closeAfterCancellation();
                    return;
                }
                while (requested.get() > 0 && !closed && !completed) {
                    T item;
                    try {
                        if (iterator == null) {
                            iterator = supplier.get();
                            if (iterator == null) {
                                complete();
                                return;
                            }
                        }
                        if (closed) {
                            closeAfterCancellation();
                            return;
                        }
                        if (!iterator.hasNext()) {
                            complete();
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
                    if (requested.get() == 0 && !closed && !completed) {
                        try {
                            if (!iterator.hasNext()) {
                                complete();
                                return;
                            }
                        } catch (Throwable failure) {
                            fail(failure);
                            return;
                        }
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
                closeAfterCancellation();
                return;
            }
            closed = true;
            try {
                closeIterator();
            } catch (Throwable closeFailure) {
                failure.addSuppressed(closeFailure);
                termination.completeExceptionally(closeFailure);
            }
            termination.complete(null);
            subscriber.onError(failure);
        }

        private void complete() {
            completed = true;
            try {
                closeIterator();
                termination.complete(null);
                subscriber.onComplete();
            } catch (Throwable closeFailure) {
                closed = true;
                termination.completeExceptionally(closeFailure);
                subscriber.onError(closeFailure);
            }
        }

        private void closeAfterCancellation() {
            try {
                closeIterator();
                termination.complete(null);
            } catch (Throwable closeFailure) {
                termination.completeExceptionally(closeFailure);
            }
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
