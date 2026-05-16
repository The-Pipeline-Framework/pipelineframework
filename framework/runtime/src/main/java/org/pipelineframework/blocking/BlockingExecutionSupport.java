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
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
        return supply(useVirtualThreads, supplier::get)
            .onItem()
            .transformToMulti(iterator -> iterator == null
                ? Multi.createFrom().empty()
                : emitIteratorItems(useVirtualThreads, iterator));
    }

    private <T> Multi<T> emitIteratorItems(boolean useVirtualThreads, CloseableIterator<T> iterator) {
        IteratorEmissionState<T> state = new IteratorEmissionState<>(iterator);
        return Multi.createBy()
            .repeating()
            .uni(() -> state, current -> supply(useVirtualThreads, current::nextResult))
            .until(IteratorResult::done)
            .onItem()
            .transform(result -> result.item())
            .onTermination()
            .call(() -> supply(useVirtualThreads, () -> {
                state.close();
                return Boolean.TRUE;
            }));
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

    private static final class IteratorEmissionState<T> {
        private final CloseableIterator<T> iterator;
        private boolean completed;
        private boolean closed;

        private IteratorEmissionState(CloseableIterator<T> iterator) {
            this.iterator = iterator;
        }

        private synchronized IteratorResult<T> nextResult() {
            if (closed) {
                return IteratorResult.completedResult();
            }
            if (completed) {
                return IteratorResult.completedResult();
            }
            if (!iterator.hasNext()) {
                completed = true;
                return IteratorResult.completedResult();
            }
            return IteratorResult.nextItem(iterator.next());
        }

        private synchronized void close() {
            if (closed) {
                return;
            }
            closed = true;
            try {
                iterator.close();
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException("Failed to close blocking iterator", e);
            }
        }
    }

    private record IteratorResult<T>(boolean done, T item) {
        private static <T> IteratorResult<T> completedResult() {
            return new IteratorResult<>(true, null);
        }

        private static <T> IteratorResult<T> nextItem(T item) {
            return new IteratorResult<>(false, item);
        }
    }
}
