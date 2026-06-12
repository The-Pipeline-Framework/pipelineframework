package org.pipelineframework.orchestrator.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

class PipelineWorkerRegistryTest {

    @Test
    void memoryRegistryRegistersHeartbeatsDrainsAndLists() {
        InMemoryPipelineWorkerRegistry registry = new InMemoryPipelineWorkerRegistry();
        PipelineWorkerRegistration registration = registration("worker-1", "rest", "sha256:release");

        PipelineWorkerRecord registered = registry.register(registration, 1_000L)
            .await().atMost(Duration.ofSeconds(2));
        assertEquals(PipelineWorkerState.HEALTHY, registered.state());

        PipelineWorkerRecord heartbeat = registry.heartbeat(
                "tenant-1",
                "org.example.restaurant",
                "worker-1",
                2_000L,
                Duration.ofSeconds(10))
            .await().atMost(Duration.ofSeconds(2))
            .orElseThrow();
        assertEquals(2_000L, heartbeat.lastHeartbeatAtEpochMs());

        PipelineWorkerRecord draining = registry.markDraining(
                "tenant-1",
                "org.example.restaurant",
                "worker-1",
                3_000L,
                Duration.ofSeconds(10))
            .await().atMost(Duration.ofSeconds(2))
            .orElseThrow();
        assertEquals(PipelineWorkerState.DRAINING, draining.state());

        List<PipelineWorkerRecord> listed = registry.list(
                "tenant-1",
                "org.example.restaurant",
                4_000L,
                Duration.ofSeconds(10))
            .await().atMost(Duration.ofSeconds(2));
        assertEquals(1, listed.size());
        assertEquals(PipelineWorkerState.DRAINING, listed.get(0).state());
    }

    @Test
    void memoryRegistryMarksWorkersStaleAfterHeartbeatTtl() {
        InMemoryPipelineWorkerRegistry registry = new InMemoryPipelineWorkerRegistry();
        registry.register(registration("worker-1", "local", "sha256:release"), 1_000L)
            .await().atMost(Duration.ofSeconds(2));

        List<PipelineWorkerRecord> listed = registry.list(
                "tenant-1",
                "org.example.restaurant",
                5_000L,
                Duration.ofSeconds(1))
            .await().atMost(Duration.ofSeconds(2));

        assertEquals(PipelineWorkerState.STALE, listed.get(0).state());
    }

    @Test
    void dynamoRegistryAppendsRegistrationEventWithConditionalPut() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = dynamoConfig();
        when(client.putItem(any(PutItemRequest.class))).thenReturn(PutItemResponse.builder().build());
        when(client.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder()
            .items(List.of(eventItem("worker-1", "rest", "REGISTER", 1_000L)))
            .build());
        DynamoPipelineWorkerRegistry registry = new DynamoPipelineWorkerRegistry(client, config);

        PipelineWorkerRecord record = registry.register(registration("worker-1", "rest", "sha256:release"), 1_000L)
            .await().atMost(Duration.ofSeconds(2));

        assertEquals("worker-1", record.workerId());
        assertEquals(PipelineWorkerState.HEALTHY, record.state());
        ArgumentCaptor<PutItemRequest> putCaptor = ArgumentCaptor.forClass(PutItemRequest.class);
        verify(client).putItem(putCaptor.capture());
        assertEquals("tpf_worker_registry", putCaptor.getValue().tableName());
        assertEquals("attribute_not_exists(#pk) AND attribute_not_exists(#sk)", putCaptor.getValue().conditionExpression());
    }

    @Test
    void dynamoRegistryResolvesLatestActivationStateFromEvents() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = dynamoConfig();
        when(client.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder()
            .items(List.of(
                eventItem("worker-1", "grpc", "REGISTER", 1_000L),
                eventItem("worker-1", "grpc", "HEARTBEAT", 2_000L),
                eventItem("worker-1", "grpc", "DRAIN", 3_000L)))
            .build());
        DynamoPipelineWorkerRegistry registry = new DynamoPipelineWorkerRegistry(client, config);

        List<PipelineWorkerRecord> records = registry.list(
                "tenant-1",
                "org.example.restaurant",
                4_000L,
                Duration.ofSeconds(10))
            .await().atMost(Duration.ofSeconds(2));

        assertEquals(1, records.size());
        assertEquals(PipelineWorkerState.DRAINING, records.get(0).state());
        assertEquals(2_000L, records.get(0).lastHeartbeatAtEpochMs());
        assertEquals(3_000L, records.get(0).drainingSinceEpochMs());
    }

    @Test
    void dynamoRegistryImplementationIsAppendOnlyAndAvoidsNullReturns() throws Exception {
        String content = Files.readString(Path.of(
            "src/main/java/org/pipelineframework/orchestrator/worker/DynamoPipelineWorkerRegistry.java"));

        assertFalse(content.contains("UpdateItemRequest"));
        assertFalse(content.contains(".updateItem("));
        assertFalse(content.contains("return null"));
    }

    private static PipelineWorkerRegistration registration(
        String workerId,
        String protocol,
        String releaseVersion) {
        return new PipelineWorkerRegistration(
            "tenant-1",
            "org.example.restaurant",
            "sha256:contract",
            releaseVersion,
            workerId,
            protocol,
            "http://localhost",
            "restaurant-artifact",
            "sha256:artifact");
    }

    private static Map<String, AttributeValue> eventItem(
        String workerId,
        String protocol,
        String eventType,
        long eventAt) {
        return Map.ofEntries(
            Map.entry("registry_key", avS("tenant-1#org.example.restaurant")),
            Map.entry("registry_sort", avS("worker:" + workerId + ":" + String.format("%019d", eventAt) + ":" + eventType)),
            Map.entry("record_type", avS("worker_event")),
            Map.entry("event_type", avS(eventType)),
            Map.entry("tenant_id", avS("tenant-1")),
            Map.entry("pipeline_id", avS("org.example.restaurant")),
            Map.entry("contract_version", avS("sha256:contract")),
            Map.entry("release_version", avS("sha256:release")),
            Map.entry("worker_id", avS(workerId)),
            Map.entry("protocol", avS(protocol)),
            Map.entry("endpoint", avS("http://localhost")),
            Map.entry("artifact_id", avS("restaurant-artifact")),
            Map.entry("artifact_digest", avS("sha256:artifact")),
            Map.entry("event_at_epoch_ms", avN(eventAt)));
    }

    private static AttributeValue avS(String value) {
        return AttributeValue.builder().s(value).build();
    }

    private static AttributeValue avN(long value) {
        return AttributeValue.builder().n(Long.toString(value)).build();
    }

    @Test
    void memoryRegistryReturnsEmptyOnHeartbeatForNonExistentWorker() {
        InMemoryPipelineWorkerRegistry registry = new InMemoryPipelineWorkerRegistry();

        var result = registry.heartbeat(
                "tenant-1",
                "org.example.restaurant",
                "nonexistent",
                1_000L,
                Duration.ofSeconds(10))
            .await().atMost(Duration.ofSeconds(2));

        assertTrue(result.isEmpty());
    }

    @Test
    void memoryRegistryReturnsEmptyOnDrainForNonExistentWorker() {
        InMemoryPipelineWorkerRegistry registry = new InMemoryPipelineWorkerRegistry();

        var result = registry.markDraining(
                "tenant-1",
                "org.example.restaurant",
                "nonexistent",
                1_000L,
                Duration.ofSeconds(10))
            .await().atMost(Duration.ofSeconds(2));

        assertTrue(result.isEmpty());
    }

    @Test
    void memoryRegistryListFiltersByTenantAndPipeline() {
        InMemoryPipelineWorkerRegistry registry = new InMemoryPipelineWorkerRegistry();
        registry.register(registration("worker-1", "rest", "sha256:release"), 1_000L)
            .await().atMost(Duration.ofSeconds(2));
        // Register for different tenant
        registry.register(new PipelineWorkerRegistration(
            "other-tenant",
            "org.example.restaurant",
            "sha256:contract",
            "sha256:release",
            "worker-2",
            "rest",
            "http://localhost",
            "artifact",
            "sha256:artifact"), 1_000L).await().atMost(Duration.ofSeconds(2));

        List<PipelineWorkerRecord> result = registry.list(
                "tenant-1",
                "org.example.restaurant",
                1_000L,
                Duration.ofSeconds(10))
            .await().atMost(Duration.ofSeconds(2));

        assertEquals(1, result.size());
        assertEquals("worker-1", result.get(0).workerId());
    }

    @Test
    void memoryRegistryReRegisterOverwritesPreviousRecord() {
        InMemoryPipelineWorkerRegistry registry = new InMemoryPipelineWorkerRegistry();
        registry.register(registration("worker-1", "rest", "sha256:release-1"), 1_000L)
            .await().atMost(Duration.ofSeconds(2));

        PipelineWorkerRecord updated = registry.register(registration("worker-1", "rest", "sha256:release-2"), 2_000L)
            .await().atMost(Duration.ofSeconds(2));

        assertEquals("sha256:release-2", updated.releaseVersion());
        assertEquals(2_000L, updated.registeredAtEpochMs());
    }

    @Test
    void memoryRegistryHeartbeatOnDrainingWorkerKeepsDrainingState() {
        InMemoryPipelineWorkerRegistry registry = new InMemoryPipelineWorkerRegistry();
        registry.register(registration("worker-1", "rest", "sha256:release"), 1_000L)
            .await().atMost(Duration.ofSeconds(2));
        registry.markDraining(
                "tenant-1",
                "org.example.restaurant",
                "worker-1",
                2_000L,
                Duration.ofSeconds(10))
            .await().atMost(Duration.ofSeconds(2));

        PipelineWorkerRecord result = registry.heartbeat(
                "tenant-1",
                "org.example.restaurant",
                "worker-1",
                3_000L,
                Duration.ofSeconds(10))
            .await().atMost(Duration.ofSeconds(2))
            .orElseThrow();

        assertEquals(PipelineWorkerState.DRAINING, result.state());
        assertEquals(3_000L, result.lastHeartbeatAtEpochMs());
    }

    @Test
    void memoryRegistryMultipleWorkersListedCorrectly() {
        InMemoryPipelineWorkerRegistry registry = new InMemoryPipelineWorkerRegistry();
        registry.register(registration("worker-1", "rest", "sha256:release"), 1_000L)
            .await().atMost(Duration.ofSeconds(2));
        registry.register(registration("worker-2", "grpc", "sha256:release"), 1_000L)
            .await().atMost(Duration.ofSeconds(2));

        List<PipelineWorkerRecord> result = registry.list(
                "tenant-1",
                "org.example.restaurant",
                1_500L,
                Duration.ofSeconds(10))
            .await().atMost(Duration.ofSeconds(2));

        assertEquals(2, result.size());
        assertTrue(result.stream().allMatch(r -> r.state() == PipelineWorkerState.HEALTHY));
    }

    @Test
    void registryMatchingDefaultMethodFiltersToProviderAndRelease() {
        InMemoryPipelineWorkerRegistry registry = new InMemoryPipelineWorkerRegistry();
        registry.register(registration("worker-1", "rest", "sha256:release"), 1_000L)
            .await().atMost(Duration.ofSeconds(2));
        registry.register(registration("worker-2", "grpc", "sha256:release"), 1_000L)
            .await().atMost(Duration.ofSeconds(2));

        PipelineWorkerAvailabilityRequest request = new PipelineWorkerAvailabilityRequest(
            "tenant-1",
            "org.example.restaurant",
            "sha256:contract",
            "sha256:release",
            "restaurant-artifact",
            "sha256:artifact");

        List<PipelineWorkerRecord> matched = registry.matching(
                request,
                "rest",
                1_000L,
                Duration.ofSeconds(10))
            .await().atMost(Duration.ofSeconds(2));

        assertEquals(1, matched.size());
        assertEquals("worker-1", matched.get(0).workerId());
    }

    @Test
    void registryMatchingDefaultMethodIncludesArtifactMismatchRecords() {
        InMemoryPipelineWorkerRegistry registry = new InMemoryPipelineWorkerRegistry();
        // Register worker with different artifact digest
        registry.register(new PipelineWorkerRegistration(
            "tenant-1",
            "org.example.restaurant",
            "sha256:contract",
            "sha256:release",
            "worker-1",
            "rest",
            "http://localhost",
            "restaurant-artifact",
            "sha256:other-artifact"), 1_000L).await().atMost(Duration.ofSeconds(2));

        PipelineWorkerAvailabilityRequest request = new PipelineWorkerAvailabilityRequest(
            "tenant-1",
            "org.example.restaurant",
            "sha256:contract",
            "sha256:release",
            "restaurant-artifact",
            "sha256:artifact");

        List<PipelineWorkerRecord> matched = registry.matching(
                request,
                "rest",
                1_000L,
                Duration.ofSeconds(10))
            .await().atMost(Duration.ofSeconds(2));

        // Artifact mismatch records are still returned by matching()
        assertEquals(1, matched.size());
        assertFalse(matched.get(0).matches(request, "rest"));
        assertTrue(matched.get(0).hasArtifactMismatch(request, "rest"));
    }

    @Test
    void dynamoRegistryResolvesHealthyStateForRecentHeartbeat() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = dynamoConfig();
        when(client.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder()
            .items(List.of(
                eventItem("worker-1", "rest", "REGISTER", 1_000L),
                eventItem("worker-1", "rest", "HEARTBEAT", 5_000L)))
            .build());
        DynamoPipelineWorkerRegistry registry = new DynamoPipelineWorkerRegistry(client, config);

        List<PipelineWorkerRecord> records = registry.list(
                "tenant-1",
                "org.example.restaurant",
                6_000L,
                Duration.ofSeconds(10))
            .await().atMost(Duration.ofSeconds(2));

        assertEquals(1, records.size());
        assertEquals(PipelineWorkerState.HEALTHY, records.get(0).state());
        assertEquals(5_000L, records.get(0).lastHeartbeatAtEpochMs());
    }

    @Test
    void dynamoRegistryMarksWorkerStaleAfterTtl() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = dynamoConfig();
        when(client.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder()
            .items(List.of(eventItem("worker-1", "rest", "REGISTER", 1_000L)))
            .build());
        DynamoPipelineWorkerRegistry registry = new DynamoPipelineWorkerRegistry(client, config);

        List<PipelineWorkerRecord> records = registry.list(
                "tenant-1",
                "org.example.restaurant",
                100_000L,
                Duration.ofSeconds(1))
            .await().atMost(Duration.ofSeconds(2));

        assertEquals(1, records.size());
        assertEquals(PipelineWorkerState.STALE, records.get(0).state());
    }

    private static PipelineOrchestratorConfig dynamoConfig() {
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        PipelineOrchestratorConfig.DynamoConfig dynamo = mock(PipelineOrchestratorConfig.DynamoConfig.class);

        when(dynamo.workerTable()).thenReturn("tpf_worker_registry");
        return config;
    }
}
