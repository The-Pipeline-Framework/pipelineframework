package org.pipelineframework.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.connector.ConnectorBindingName;
import org.pipelineframework.connector.ConnectorCompletionStages;
import org.pipelineframework.connector.ConnectorOperationIdentity;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.config.pipeline.PipelineJson;
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
    void captureStoreFailurePreventsProviderInvocation() {
        CountingFrameworkConnector connector = new CountingFrameworkConnector();
        QueryStepSupport support = new QueryStepSupport(List.of(connector), List.of(new FailingCaptureStore()));
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant-1", "exec-1", 2));

        QueryCaptureStoreException failure = assertThrows(QueryCaptureStoreException.class, () ->
            support.queryOneToOne(descriptor(), new Lookup("customer-1", "US"), Snapshot.class)
                .await().atMost(Duration.ofSeconds(2)));

        assertTrue(failure.getMessage().contains("offline"));
        assertEquals(0, connector.calls.get());
    }

    @Test
    void nativeDescriptorReplaysCapturedOutputBeforeRejectingUnavailableLiveExecution() {
        QueryStepDescriptor descriptor = QueryStepDescriptor.nativeQuery(
            "LoadCustomerRisk",
            Lookup.class.getName(),
            Snapshot.class.getName(),
            "ONE_TO_ONE",
            new NativeQuerySelector(
                ConnectorBindingName.of("risk"),
                new ConnectorOperationIdentity(
                    ConnectorProviderId.of("acme.risk"), "risk.find", ConnectorOperationKind.QUERY, 1),
                1),
            Map.of());
        Lookup input = new Lookup("customer-1", "US");
        String inputJson;
        try {
            inputJson = PipelineJson.mapper().writeValueAsString(PipelineJson.mapper().valueToTree(input));
        } catch (Exception failure) {
            throw new IllegalStateException("test query input could not be serialized", failure);
        }
        String captureKey = captureKey("tenant-1", "exec-1", 2, descriptor, inputJson);
        QueryCaptureRecord captured = new QueryCaptureRecord(
            "tenant-1", "exec-1", 2, descriptor.queryId(), descriptor.version(), captureKey,
            inputJson, "{\"customerId\":\"captured\",\"riskBand\":\"LOW\",\"callNumber\":7}",
            Snapshot.class.getName(), Instant.now());
        QueryStepSupport support = new QueryStepSupport(List.of(), List.of(new PreloadedCaptureStore(captured)));
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant-1", "exec-1", 2));

        Snapshot replayed = support.queryOneToOne(
            descriptor, input, Snapshot.class).await().atMost(Duration.ofSeconds(2));

        assertEquals(new Snapshot("captured", "LOW", 7), replayed);
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

    private static String captureKey(
        String tenant,
        String execution,
        int stepIndex,
        QueryStepDescriptor descriptor,
        String inputJson
    ) {
        String basis = tenant + ":" + execution + ":" + stepIndex + ":"
            + descriptor.queryId() + ":" + descriptor.version() + ":" + inputJson;
        try {
            String tenantKey = Base64.getUrlEncoder().withoutPadding().encodeToString(
                tenant.getBytes(StandardCharsets.UTF_8));
            return tenantKey + "." + HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(basis.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception failure) {
            throw new IllegalStateException("SHA-256 digest is unavailable to the test", failure);
        }
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

    private record PreloadedCaptureStore(QueryCaptureRecord record) implements QueryCaptureStore {
        @Override
        public CompletionStage<Optional<QueryCaptureRecord>> get(String captureKey) {
            return CompletableFuture.completedFuture(
                record.captureKey().equals(captureKey) ? Optional.of(record) : Optional.empty());
        }

        @Override
        public CompletionStage<QueryCaptureRecord> putIfAbsent(QueryCaptureRecord candidate) {
            return CompletableFuture.completedFuture(record);
        }

        @Override
        public CompletionStage<Boolean> remove(String captureKey) {
            return CompletableFuture.completedFuture(false);
        }

        @Override
        public CompletionStage<Void> clear() {
            return ConnectorCompletionStages.completed();
        }
    }

    private static final class FailingCaptureStore implements QueryCaptureStore {
        @Override
        public CompletionStage<Optional<QueryCaptureRecord>> get(String captureKey) {
            return CompletableFuture.failedFuture(new QueryCaptureStoreException("capture store offline"));
        }

        @Override
        public CompletionStage<QueryCaptureRecord> putIfAbsent(QueryCaptureRecord record) {
            return CompletableFuture.failedFuture(new QueryCaptureStoreException("capture store offline"));
        }

        @Override
        public CompletionStage<Boolean> remove(String captureKey) {
            return CompletableFuture.failedFuture(new QueryCaptureStoreException("capture store offline"));
        }

        @Override
        public CompletionStage<Void> clear() {
            return CompletableFuture.failedFuture(new QueryCaptureStoreException("capture store offline"));
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
