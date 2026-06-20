package org.pipelineframework.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.execution.PipelineExecutionContext;
import org.pipelineframework.execution.PipelineExecutionContextHolder;

class QueryStepSupportTest {

    @AfterEach
    void cleanup() {
        PipelineExecutionContextHolder.clear();
    }

    @Test
    void unmanagedExecutionCallsConnectorEachTimeWithoutCapture() {
        CountingConnector connector = new CountingConnector();
        QueryStepSupport support = new QueryStepSupport(List.of(connector), List.of(new InMemoryQueryCaptureStore()));
        QueryStepDescriptor descriptor = descriptor();
        Lookup input = new Lookup("customer-1", "US");

        Snapshot first = support.queryOneToOne(descriptor, input, Snapshot.class)
            .await().atMost(Duration.ofSeconds(2));
        Snapshot second = support.queryOneToOne(descriptor, input, Snapshot.class)
            .await().atMost(Duration.ofSeconds(2));

        assertEquals(1, first.callNumber());
        assertEquals(2, second.callNumber());
        assertEquals(2, connector.calls.get());
    }

    @Test
    void managedExecutionReusesCapturedOutputForSameExecutionStepAndKey() {
        CountingConnector connector = new CountingConnector();
        QueryStepSupport support = new QueryStepSupport(List.of(connector), List.of(new InMemoryQueryCaptureStore()));
        QueryStepDescriptor descriptor = descriptor();
        Lookup input = new Lookup("customer-1", "US");
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant-1", "exec-1", 2));

        Snapshot first = support.queryOneToOne(descriptor, input, Snapshot.class)
            .await().atMost(Duration.ofSeconds(2));
        Snapshot second = support.queryOneToOne(descriptor, input, Snapshot.class)
            .await().atMost(Duration.ofSeconds(2));

        assertEquals(first, second);
        assertEquals(1, connector.calls.get());
    }

    @Test
    void keyFieldsLimitCaptureIdentityToSelectedInputFields() {
        CountingConnector connector = new CountingConnector();
        QueryStepSupport support = new QueryStepSupport(List.of(connector), List.of(new InMemoryQueryCaptureStore()));
        QueryStepDescriptor descriptor = descriptor();
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant-1", "exec-1", 2));

        Snapshot first = support.queryOneToOne(descriptor, new Lookup("customer-1", "US"), Snapshot.class)
            .await().atMost(Duration.ofSeconds(2));
        Snapshot second = support.queryOneToOne(descriptor, new Lookup("customer-1", "FR"), Snapshot.class)
            .await().atMost(Duration.ofSeconds(2));

        assertEquals(first, second);
        assertEquals(1, connector.calls.get());
    }

    @Test
    void inMemoryCaptureStoreSupportsExplicitCleanup() {
        InMemoryQueryCaptureStore store = new InMemoryQueryCaptureStore();
        QueryCaptureRecord record = new QueryCaptureRecord(
            "tenant-1",
            "exec-1",
            2,
            "customer-risk-by-id",
            "v1",
            "capture-key",
            "{}",
            "{}",
            Snapshot.class.getName(),
            java.time.Instant.now());

        store.putIfAbsent(record).await().atMost(Duration.ofSeconds(2));

        assertTrue(store.get("capture-key").await().atMost(Duration.ofSeconds(2)).isPresent());
        assertTrue(store.remove("capture-key").await().atMost(Duration.ofSeconds(2)));
        assertTrue(store.get("capture-key").await().atMost(Duration.ofSeconds(2)).isEmpty());

        store.putIfAbsent(record).await().atMost(Duration.ofSeconds(2));
        store.clear().await().atMost(Duration.ofSeconds(2));

        assertTrue(store.get("capture-key").await().atMost(Duration.ofSeconds(2)).isEmpty());
    }

    private QueryStepDescriptor descriptor() {
        return new QueryStepDescriptor(
            "LoadCustomerRisk",
            "customer-risk-by-id",
            "customer-risk",
            "v1",
            Lookup.class.getName(),
            Snapshot.class.getName(),
            "ONE_TO_ONE",
            List.of("customerId"),
            Map.of("source", "risk-db"));
    }

    private static final class CountingConnector implements QueryConnector<Lookup, Snapshot> {
        private final AtomicInteger calls = new AtomicInteger();

        @Override
        public String connectorName() {
            return "customer-risk";
        }

        @Override
        public Uni<Snapshot> execute(QueryRequest<Lookup> request) {
            int call = calls.incrementAndGet();
            return Uni.createFrom().item(new Snapshot(request.input().customerId(), "MEDIUM", call));
        }
    }

    record Lookup(String customerId, String locale) {
    }

    record Snapshot(String customerId, String riskBand, int callNumber) {
    }
}
