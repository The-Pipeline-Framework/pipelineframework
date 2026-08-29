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

package org.pipelineframework.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.connector.ConnectorBindingDefinition;
import org.pipelineframework.connector.ConnectorBindingName;
import org.pipelineframework.connector.ConnectorBindingRegistry;
import org.pipelineframework.connector.ConnectorConfigurationDocument;
import org.pipelineframework.connector.ConnectorOperation;
import org.pipelineframework.connector.ConnectorOperationIdentity;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.ConnectorProvider;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorProviderVersion;
import org.pipelineframework.connector.QueryInvocation;
import org.pipelineframework.connector.QueryStream;
import org.pipelineframework.connector.StreamingQueryOperation;
import org.pipelineframework.connector.TestConnectorBindingRegistries;
import org.pipelineframework.execution.PipelineExecutionContext;
import org.pipelineframework.execution.PipelineExecutionContextHolder;

class NativeStreamingQueryOperationTest {
    private final InMemoryQueryCaptureStore captureStore = new InMemoryQueryCaptureStore();
    private final StreamingRowsOperation operation = new StreamingRowsOperation();
    private final QueryStepSupport support = new QueryStepSupport(
        List.of(), List.of(captureStore), bindings(operation));

    @AfterEach
    void clearContext() {
        PipelineExecutionContextHolder.clear();
    }

    @Test
    void emitsOrderedRowsWithoutMaterializingTheProviderContract() {
        operation.next(List.of(new Row("A"), new Row("B"), new Row("C")), Optional.empty());

        List<Row> rows = support.queryOneToMany(descriptor(), new Lookup("group-1"), Row.class)
            .collect().asList().await().atMost(Duration.ofSeconds(2));

        assertEquals(List.of(new Row("A"), new Row("B"), new Row("C")), rows);
        assertEquals(1, operation.invocations.get());
    }

    @Test
    void commitsACompleteOrderedObservationAndReplaysItWithoutTheProvider() {
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant", "execution", 4));
        operation.next(List.of(new Row("A"), new Row("B"), new Row("C")), Optional.empty());

        List<Row> first = support.queryOneToMany(descriptor(), new Lookup("group-1"), Row.class)
            .collect().asList().await().atMost(Duration.ofSeconds(2));
        QueryStepSupport replayOnly = new QueryStepSupport(
            List.of(), List.of(captureStore), unavailableBindings());
        List<Row> replayed = replayOnly.queryOneToMany(descriptor(), new Lookup("group-1"), Row.class)
            .collect().asList().await().atMost(Duration.ofSeconds(2));

        assertEquals(first, replayed);
        assertEquals(List.of(new Row("A"), new Row("B"), new Row("C")), replayed);
        assertEquals(1, operation.invocations.get());
    }

    @Test
    void abortsAPartialObservationAndReevaluatesTheWholeOrderedStream() {
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant", "retry-execution", 4));
        operation.next(List.of(new Row("A"), new Row("B"), new Row("C")),
            Optional.of(new IllegalStateException("database stream failed")));

        assertThrows(IllegalStateException.class, () -> support
            .queryOneToMany(descriptor(), new Lookup("group-1"), Row.class)
            .collect().asList().await().atMost(Duration.ofSeconds(2)));

        operation.next(List.of(new Row("A"), new Row("B"), new Row("C"), new Row("D")), Optional.empty());
        List<Row> retry = support.queryOneToMany(descriptor(), new Lookup("group-1"), Row.class)
            .collect().asList().await().atMost(Duration.ofSeconds(2));

        assertEquals(List.of(new Row("A"), new Row("B"), new Row("C"), new Row("D")), retry);
        assertEquals(2, operation.invocations.get());
    }

    @Test
    void commitsAndReplaysAnEmptySuccessfulObservation() {
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant", "empty-execution", 4));
        operation.next(List.of(), Optional.empty());

        List<Row> first = support.queryOneToMany(descriptor(), new Lookup("empty"), Row.class)
            .collect().asList().await().atMost(Duration.ofSeconds(2));
        List<Row> replayed = new QueryStepSupport(List.of(), List.of(captureStore), unavailableBindings())
            .queryOneToMany(descriptor(), new Lookup("empty"), Row.class)
            .collect().asList().await().atMost(Duration.ofSeconds(2));

        assertEquals(List.of(), first);
        assertEquals(List.of(), replayed);
        assertEquals(1, operation.invocations.get());
    }

    @Test
    void cancellationAbortsThePartialObservationBeforeTheNextEvaluation() throws Exception {
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant", "cancel-execution", 4));
        TrackingCaptureStore trackingStore = new TrackingCaptureStore();
        QueryStepSupport trackingSupport = new QueryStepSupport(
            List.of(), List.of(trackingStore), bindings(operation));
        CompletableFuture<Void> termination = new CompletableFuture<>();
        CancellablePublisher publisher = new CancellablePublisher(termination);
        operation.nextStream(new QueryStream<>(publisher, termination));

        var subscription = trackingSupport.queryOneToMany(
                descriptor(), new Lookup("group-1"), Row.class)
            .subscribe().with(ignored -> { }, ignored -> { });
        subscription.cancel();

        assertTrue(publisher.cancelled.await(2, TimeUnit.SECONDS));
        assertTrue(trackingStore.aborted.await(2, TimeUnit.SECONDS));
        operation.next(List.of(new Row("A"), new Row("B")), Optional.empty());
        List<Row> retry = trackingSupport.queryOneToMany(
                descriptor(), new Lookup("group-1"), Row.class)
            .collect().asList().await().atMost(Duration.ofSeconds(2));
        assertEquals(List.of(new Row("A"), new Row("B")), retry);
        assertEquals(2, operation.invocations.get());
    }

    private QueryStepDescriptor descriptor() {
        ConnectorOperationIdentity identity = new ConnectorOperationIdentity(
            ConnectorProviderId.of("acme.rows"), "find.many", ConnectorOperationKind.QUERY, 1);
        return QueryStepDescriptor.nativeStreamingQuery(
            "FindRows",
            Lookup.class.getName(),
            Row.class.getName(),
            new NativeQuerySelector(ConnectorBindingName.of("rows"), identity, 1),
            Map.of(),
            List.of("group"));
    }

    private static ConnectorBindingRegistry bindings(StreamingRowsOperation operation) {
        return TestConnectorBindingRegistries.fromProviderSupplier(
            List.of(new ConnectorBindingDefinition(
                ConnectorBindingName.of("rows"),
                ConnectorProviderId.of("acme.rows"),
                1,
                ConnectorConfigurationDocument.empty())),
            new RowsProvider(operation),
            () -> new RowsProvider(operation));
    }

    private static ConnectorBindingRegistry unavailableBindings() {
        return ConnectorBindingRegistry.fromProvidersAllowingUnavailable(
            List.of(new ConnectorBindingDefinition(
                ConnectorBindingName.of("rows"),
                ConnectorProviderId.of("acme.rows"),
                1,
                ConnectorConfigurationDocument.empty())),
            List.of());
    }

    record Lookup(String group) {
    }

    record Row(String value) {
    }

    private static final class RowsProvider implements ConnectorProvider<Void> {
        private final StreamingRowsOperation operation;

        private RowsProvider(StreamingRowsOperation operation) {
            this.operation = operation;
        }

        @Override
        public ConnectorProviderId id() {
            return ConnectorProviderId.of("acme.rows");
        }

        @Override
        public ConnectorProviderVersion version() {
            return new ConnectorProviderVersion(1, 0);
        }

        @Override
        public Collection<? extends ConnectorOperation> operations() {
            return List.of(operation);
        }
    }

    private static final class StreamingRowsOperation
        implements StreamingQueryOperation<Lookup, ConnectorConfigurationDocument, Row> {
        private final AtomicInteger invocations = new AtomicInteger();
        private List<Row> rows = List.of();
        private Optional<RuntimeException> terminalFailure = Optional.empty();
        private Optional<QueryStream<Row>> streamOverride = Optional.empty();

        void next(List<Row> rows, Optional<RuntimeException> terminalFailure) {
            this.rows = List.copyOf(rows);
            this.terminalFailure = terminalFailure;
            streamOverride = Optional.empty();
        }

        void nextStream(QueryStream<Row> stream) {
            streamOverride = Optional.of(stream);
        }

        @Override
        public String id() {
            return "find.many";
        }

        @Override
        public QueryStream<Row> query(
            QueryInvocation<Lookup, ConnectorConfigurationDocument, Row> invocation
        ) {
            invocations.incrementAndGet();
            if (streamOverride.isPresent()) {
                QueryStream<Row> selected = streamOverride.orElseThrow();
                streamOverride = Optional.empty();
                return selected;
            }
            return new QueryStream<>(orderedPublisher(rows, terminalFailure), CompletableFuture.completedFuture(null));
        }
    }

    private static final class CancellablePublisher implements Flow.Publisher<Row> {
        private final CompletableFuture<Void> termination;
        private final CountDownLatch cancelled = new CountDownLatch(1);

        private CancellablePublisher(CompletableFuture<Void> termination) {
            this.termination = termination;
        }

        @Override
        public void subscribe(Flow.Subscriber<? super Row> subscriber) {
            subscriber.onSubscribe(new Flow.Subscription() {
                private final AtomicBoolean emitted = new AtomicBoolean();

                @Override
                public void request(long count) {
                    if (count > 0L && emitted.compareAndSet(false, true)) {
                        subscriber.onNext(new Row("partial"));
                    }
                }

                @Override
                public void cancel() {
                    cancelled.countDown();
                    termination.complete(null);
                }
            });
        }
    }

    private static final class TrackingCaptureStore implements QueryCaptureStore {
        private final InMemoryQueryCaptureStore delegate = new InMemoryQueryCaptureStore();
        private final CountDownLatch aborted = new CountDownLatch(1);

        @Override
        public CompletionStage<Optional<QueryCaptureRecord>> get(String captureKey) {
            return delegate.get(captureKey);
        }

        @Override
        public CompletionStage<QueryCaptureRecord> putIfAbsent(QueryCaptureRecord record) {
            return delegate.putIfAbsent(record);
        }

        @Override
        public CompletionStage<StreamingQueryCaptureOpen> openStreaming(StreamingQueryCaptureRequest request) {
            return delegate.openStreaming(request).thenApply(opened -> {
                if (!(opened instanceof StreamingQueryCaptureOpen.Write write)) {
                    return opened;
                }
                StreamingQueryCaptureWriter writer = write.writer();
                return new StreamingQueryCaptureOpen.Write(new StreamingQueryCaptureWriter() {
                    @Override
                    public CompletionStage<Void> append(StreamingQueryCaptureItem item) {
                        return writer.append(item);
                    }

                    @Override
                    public CompletionStage<Void> commit() {
                        return writer.commit();
                    }

                    @Override
                    public CompletionStage<Void> abort() {
                        aborted.countDown();
                        return writer.abort();
                    }
                });
            });
        }

        @Override
        public CompletionStage<Boolean> remove(String captureKey) {
            return delegate.remove(captureKey);
        }

        @Override
        public CompletionStage<Void> clear() {
            return delegate.clear();
        }
    }

    private static <T> Flow.Publisher<T> orderedPublisher(
        List<T> items,
        Optional<? extends RuntimeException> terminalFailure
    ) {
        List<T> snapshot = List.copyOf(items);
        return subscriber -> subscriber.onSubscribe(new Flow.Subscription() {
            private int index;
            private boolean terminated;

            @Override
            public synchronized void request(long count) {
                if (terminated) {
                    return;
                }
                if (count <= 0L) {
                    terminated = true;
                    subscriber.onError(new IllegalArgumentException("positive demand required"));
                    return;
                }
                long remaining = count;
                while (!terminated && remaining-- > 0L && index < snapshot.size()) {
                    subscriber.onNext(snapshot.get(index++));
                }
                if (!terminated && index == snapshot.size()) {
                    terminated = true;
                    terminalFailure.ifPresentOrElse(subscriber::onError, subscriber::onComplete);
                }
            }

            @Override
            public synchronized void cancel() {
                terminated = true;
            }
        });
    }
}
