package org.pipelineframework.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.pipeline.PipelineYamlJpaQuery;
import org.pipelineframework.execution.PipelineExecutionContext;
import org.pipelineframework.execution.PipelineExecutionContextHolder;

class QueryStepSupportTest {

    @AfterEach
    void cleanup() {
        PipelineExecutionContextHolder.clear();
    }

    @Test
    void unmanagedExecutionCallsConnectorEachTimeWithoutCapture() {
        CountingFrameworkConnector connector = new CountingFrameworkConnector();
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
        CountingFrameworkConnector connector = new CountingFrameworkConnector();
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
        CountingFrameworkConnector connector = new CountingFrameworkConnector();
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

        store.putIfAbsent(record).toCompletableFuture().join();

        assertTrue(store.get("capture-key").toCompletableFuture().join().isPresent());
        assertTrue(store.remove("capture-key").toCompletableFuture().join());
        assertTrue(store.get("capture-key").toCompletableFuture().join().isEmpty());

        store.putIfAbsent(record).toCompletableFuture().join();
        store.clear().toCompletableFuture().join();

        assertTrue(store.get("capture-key").toCompletableFuture().join().isEmpty());
    }

    @Test
    void connectorNullCompletionStageFailsDeterministically() {
        QueryStepSupport support = new QueryStepSupport(List.of(new NullStageFrameworkConnector()), List.of(new InMemoryQueryCaptureStore()));

        IllegalStateException failure = assertThrows(IllegalStateException.class, () ->
            support.queryOneToOne(descriptor(), new Lookup("customer-1", "US"), Snapshot.class)
                .await().atMost(Duration.ofSeconds(2)));

        assertTrue(failure.getMessage().contains("returned null CompletionStage"));
    }

    @Test
    void connectorCompletedNullResultFailsDeterministically() {
        QueryStepSupport support = new QueryStepSupport(List.of(new NullResultFrameworkConnector()), List.of(new InMemoryQueryCaptureStore()));

        IllegalStateException failure = assertThrows(IllegalStateException.class, () ->
            support.queryOneToOne(descriptor(), new Lookup("customer-1", "US"), Snapshot.class)
                .await().atMost(Duration.ofSeconds(2)));

        assertTrue(failure.getMessage().contains("completed with null result"));
    }

    private QueryStepDescriptor descriptor() {
        return new QueryStepDescriptor(
            "LoadCustomerRisk",
            "customer-risk-by-id",
            "jpa",
            "v1",
            Lookup.class.getName(),
            Snapshot.class.getName(),
            "ONE_TO_ONE",
            List.of("customerId"),
            new PipelineYamlJpaQuery(
                CustomerRiskEntity.class.getName(),
                Map.of("customerId", "input.customerId"),
                Map.of("customerId", "customerId", "riskBand", "riskBand", "callNumber", "callNumber"),
                "single"));
    }

    private static final class CountingFrameworkConnector implements FrameworkQueryConnector {
        private final AtomicInteger calls = new AtomicInteger();

        @Override
        public String connectorName() {
            return "jpa";
        }

        @Override
        public <O> CompletionStage<O> queryOne(QueryRequest<?> request, Class<O> outputType) {
            int call = calls.incrementAndGet();
            Lookup input = (Lookup) request.input();
            Snapshot output = new Snapshot(input.customerId(), "MEDIUM", call);
            return CompletableFuture.completedFuture(outputType.cast(output));
        }
    }

    private static final class NullStageFrameworkConnector implements FrameworkQueryConnector {
        @Override
        public String connectorName() {
            return "jpa";
        }

        @Override
        public <O> CompletionStage<O> queryOne(QueryRequest<?> request, Class<O> outputType) {
            return null;
        }
    }

    private static final class NullResultFrameworkConnector implements FrameworkQueryConnector {
        @Override
        public String connectorName() {
            return "jpa";
        }

        @Override
        public <O> CompletionStage<O> queryOne(QueryRequest<?> request, Class<O> outputType) {
            return CompletableFuture.completedFuture(null);
        }
    }

    record Lookup(String customerId, String locale) {
    }

    record Snapshot(String customerId, String riskBand, int callNumber) {
    }

    private static final class CustomerRiskEntity {
    }
}
