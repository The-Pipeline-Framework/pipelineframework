package org.pipelineframework.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import jakarta.enterprise.context.ApplicationScoped;

import io.quarkus.arc.properties.IfBuildProperty;

/**
 * In-memory query capture store for tests, local development, and unmanaged defaults.
 */
@ApplicationScoped
@IfBuildProperty(
    name = "pipeline.query.capture-store.provider",
    stringValue = "memory",
    enableIfMissing = true)
public class InMemoryQueryCaptureStore implements QueryCaptureStore {
    private final ConcurrentMap<String, QueryCaptureRecord> records = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, List<StreamingQueryCaptureItem>> streamingRecords = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, StreamingState> streamingWrites = new ConcurrentHashMap<>();

    @Override
    public CompletionStage<Optional<QueryCaptureRecord>> get(String captureKey) {
        return CompletableFuture.completedFuture(Optional.ofNullable(records.get(captureKey)));
    }

    @Override
    public CompletionStage<QueryCaptureRecord> putIfAbsent(QueryCaptureRecord record) {
        QueryCaptureRecord existing = records.putIfAbsent(record.captureKey(), record);
        return CompletableFuture.completedFuture(existing == null ? record : existing);
    }

    @Override
    public synchronized CompletionStage<StreamingQueryCaptureOpen> openStreaming(
        StreamingQueryCaptureRequest request
    ) {
        List<StreamingQueryCaptureItem> committed = streamingRecords.get(request.captureKey());
        if (committed != null) {
            return CompletableFuture.completedFuture(new StreamingQueryCaptureOpen.Replay(new ItemsPublisher(committed)));
        }
        StreamingState active = streamingWrites.get(request.captureKey());
        if (active != null) {
            CompletableFuture<StreamingQueryCaptureOpen> waiter = new CompletableFuture<>();
            active.waiters.add(new StreamingWaiter(request, waiter));
            return waiter;
        }
        StreamingState state = new StreamingState(request);
        streamingWrites.put(request.captureKey(), state);
        return CompletableFuture.completedFuture(new StreamingQueryCaptureOpen.Write(new InMemoryWriter(state)));
    }

    @Override
    public synchronized CompletionStage<Boolean> remove(String captureKey) {
        boolean removed = records.remove(captureKey) != null;
        removed |= streamingRecords.remove(captureKey) != null;
        StreamingState active = streamingWrites.remove(captureKey);
        if (active != null) {
            active.closed = true;
            active.waiters.forEach(waiter -> waiter.result().completeExceptionally(
                new IllegalStateException("streaming Query capture was removed")));
            removed = true;
        }
        return CompletableFuture.completedFuture(removed);
    }

    @Override
    public synchronized CompletionStage<Void> clear() {
        records.clear();
        streamingRecords.clear();
        streamingWrites.values().forEach(state -> {
            state.closed = true;
            state.waiters.forEach(waiter -> waiter.result().completeExceptionally(
                new IllegalStateException("streaming Query capture store was cleared")));
        });
        streamingWrites.clear();
        return CompletableFuture.completedFuture(null);
    }

    private final class InMemoryWriter implements StreamingQueryCaptureWriter {
        private final StreamingState state;

        private InMemoryWriter(StreamingState state) {
            this.state = state;
        }

        @Override
        public CompletionStage<Void> append(StreamingQueryCaptureItem item) {
            synchronized (InMemoryQueryCaptureStore.this) {
                if (state.closed) {
                    return CompletableFuture.failedFuture(new IllegalStateException("streaming Query capture is closed"));
                }
                long expected = state.items.size();
                if (item.ordinal() != expected) {
                    return CompletableFuture.failedFuture(new IllegalArgumentException(
                        "streaming Query capture expected ordinal " + expected + " but received " + item.ordinal()));
                }
                if (!state.request.outputType().equals(item.outputType())) {
                    return CompletableFuture.failedFuture(new IllegalArgumentException(
                        "streaming Query capture item type does not match its observation"));
                }
                state.items.add(item);
            }
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> commit() {
            List<StreamingWaiter> waiters;
            List<StreamingQueryCaptureItem> committed;
            synchronized (InMemoryQueryCaptureStore.this) {
                if (state.closed) {
                    return CompletableFuture.failedFuture(new IllegalStateException("streaming Query capture is closed"));
                }
                state.closed = true;
                committed = List.copyOf(state.items);
                streamingRecords.put(state.request.captureKey(), committed);
                streamingWrites.remove(state.request.captureKey(), state);
                waiters = List.copyOf(state.waiters);
            }
            waiters.forEach(waiter -> waiter.result().complete(
                new StreamingQueryCaptureOpen.Replay(new ItemsPublisher(committed))));
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> abort() {
            List<StreamingWaiter> waiters;
            synchronized (InMemoryQueryCaptureStore.this) {
                if (state.closed) {
                    return CompletableFuture.completedFuture(null);
                }
                state.closed = true;
                streamingWrites.remove(state.request.captureKey(), state);
                waiters = List.copyOf(state.waiters);
            }
            for (StreamingWaiter waiter : waiters) {
                openStreaming(waiter.request()).whenComplete((opened, failure) -> {
                    if (failure == null) {
                        waiter.result().complete(opened);
                    } else {
                        waiter.result().completeExceptionally(failure);
                    }
                });
            }
            return CompletableFuture.completedFuture(null);
        }
    }

    private static final class StreamingState {
        private final StreamingQueryCaptureRequest request;
        private final List<StreamingQueryCaptureItem> items = new ArrayList<>();
        private final List<StreamingWaiter> waiters = new ArrayList<>();
        private boolean closed;

        private StreamingState(StreamingQueryCaptureRequest request) {
            this.request = request;
        }
    }

    private record StreamingWaiter(
        StreamingQueryCaptureRequest request,
        CompletableFuture<StreamingQueryCaptureOpen> result
    ) {
    }

    private record ItemsPublisher(List<StreamingQueryCaptureItem> items)
        implements Flow.Publisher<StreamingQueryCaptureItem> {
        private ItemsPublisher {
            items = List.copyOf(items);
        }

        @Override
        public void subscribe(Flow.Subscriber<? super StreamingQueryCaptureItem> subscriber) {
            subscriber.onSubscribe(new Flow.Subscription() {
                private final AtomicLong index = new AtomicLong();
                private final AtomicBoolean done = new AtomicBoolean();

                @Override
                public synchronized void request(long count) {
                    if (count <= 0L) {
                        if (done.compareAndSet(false, true)) {
                            subscriber.onError(new IllegalArgumentException("streaming capture replay demand must be positive"));
                        }
                        return;
                    }
                    long remaining = count;
                    while (remaining-- > 0L && !done.get()) {
                        long next = index.getAndIncrement();
                        if (next >= items.size()) {
                            if (done.compareAndSet(false, true)) {
                                subscriber.onComplete();
                            }
                            return;
                        }
                        subscriber.onNext(items.get((int) next));
                    }
                    if (index.get() >= items.size() && done.compareAndSet(false, true)) {
                        subscriber.onComplete();
                    }
                }

                @Override
                public synchronized void cancel() {
                    done.set(true);
                }
            });
        }
    }
}
