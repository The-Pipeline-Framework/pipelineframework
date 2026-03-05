package org.pipelineframework.orchestrator;

import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class DynamoExecutionStateStoreTest {

    @Test
    void providerNameIsDynamo() {
        DynamoExecutionStateStore store = new DynamoExecutionStateStore();
        assertEquals("dynamo", store.providerName());
    }

    @Test
    void priorityIsNegative() {
        DynamoExecutionStateStore store = new DynamoExecutionStateStore();
        assertEquals(-1000, store.priority());
    }

    @Test
    void startupValidationReportsMissingExecutionTable() {
        PipelineOrchestratorConfig config = mockConfig("", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(null, config);

        var validationError = store.startupValidationError(config);

        assertTrue(validationError.isPresent());
        assertTrue(validationError.get().contains("execution-table"));
    }

    @Test
    void startupValidationPassesWhenTablesConfigured() {
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(null, config);

        var validationError = store.startupValidationError(config);

        assertTrue(validationError.isEmpty());
    }

    @Test
    void createOrGetReturnsDuplicateWhenExistingRecordFound() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        long now = System.currentTimeMillis();
        long ttl = now / 1000 + 3600;
        ExecutionCreateCommand command = new ExecutionCreateCommand("tenant-a", "key-1", "payload", now, ttl);

        when(client.getItem(any(GetItemRequest.class)))
            .thenReturn(GetItemResponse.builder().item(Map.of(
                "tenant_execution_key", AttributeValue.builder().s("8:tenant-a:5:key-1").build(),
                "execution_id", AttributeValue.builder().s("exec-1").build()))
                .build())
            .thenReturn(GetItemResponse.builder().item(executionItem("tenant-a", "exec-1", "key-1", ttl)).build());

        CreateExecutionResult result = store.createOrGetExecution(command).await().indefinitely();

        assertTrue(result.duplicate());
        assertEquals("exec-1", result.record().executionId());
        verify(client, never()).transactWriteItems(any(TransactWriteItemsRequest.class));
    }

    @Test
    void claimLeaseReturnsEmptyOnConditionalFailure() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenThrow(ConditionalCheckFailedException.builder().message("stale").build());

        Optional<ExecutionRecord<Object, Object>> claimed = store.claimLease(
                "tenant-a",
                "exec-1",
                "worker-1",
                System.currentTimeMillis(),
                1000)
            .await().indefinitely();

        assertTrue(claimed.isEmpty());
    }

    @Test
    void markSucceededReturnsUpdatedRecordWhenConditionMatches() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        long now = System.currentTimeMillis();
        long ttl = now / 1000 + 3600;
        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenReturn(UpdateItemResponse.builder()
                .attributes(executionItem("tenant-a", "exec-1", "key-1", ttl, ExecutionStatus.SUCCEEDED))
                .build());

        Optional<ExecutionRecord<Object, Object>> updated = store.markSucceeded(
                "tenant-a",
                "exec-1",
                1L,
                "exec-1:0:0",
                java.util.List.of("ok"),
                now)
            .await().indefinitely();

        assertTrue(updated.isPresent());
        assertEquals(ExecutionStatus.SUCCEEDED, updated.get().status());
        assertFalse(updated.get().executionId().isBlank());
    }

    @Test
    void scheduleRetryUpdatesExecutionState() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        long now = System.currentTimeMillis();
        long ttl = now / 1000 + 3600;
        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenReturn(UpdateItemResponse.builder()
                .attributes(executionItem("tenant-a", "exec-1", "key-1", ttl, ExecutionStatus.WAIT_RETRY))
                .build());

        Optional<ExecutionRecord<Object, Object>> updated = store.scheduleRetry(
                "tenant-a",
                "exec-1",
                1L,
                2,
                now + 10000,
                "exec-1:1:0",
                "TIMEOUT",
                "Operation timed out",
                now)
            .await().indefinitely();

        assertTrue(updated.isPresent());
        assertEquals(ExecutionStatus.WAIT_RETRY, updated.get().status());
    }

    @Test
    void markTerminalFailureWithFailedStatus() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        long now = System.currentTimeMillis();
        long ttl = now / 1000 + 3600;
        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenReturn(UpdateItemResponse.builder()
                .attributes(executionItem("tenant-a", "exec-1", "key-1", ttl, ExecutionStatus.FAILED))
                .build());

        Optional<ExecutionRecord<Object, Object>> updated = store.markTerminalFailure(
                "tenant-a",
                "exec-1",
                1L,
                ExecutionStatus.FAILED,
                "exec-1:2:0",
                "MAX_RETRIES",
                "Exceeded maximum retry attempts",
                now)
            .await().indefinitely();

        assertTrue(updated.isPresent());
        assertEquals(ExecutionStatus.FAILED, updated.get().status());
    }

    @Test
    void markTerminalFailureWithDlqStatus() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        long now = System.currentTimeMillis();
        long ttl = now / 1000 + 3600;
        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenReturn(UpdateItemResponse.builder()
                .attributes(executionItem("tenant-a", "exec-1", "key-1", ttl, ExecutionStatus.DLQ))
                .build());

        Optional<ExecutionRecord<Object, Object>> updated = store.markTerminalFailure(
                "tenant-a",
                "exec-1",
                1L,
                ExecutionStatus.DLQ,
                "exec-1:2:0",
                "POISON_MESSAGE",
                "Message cannot be processed",
                now)
            .await().indefinitely();

        assertTrue(updated.isPresent());
        assertEquals(ExecutionStatus.DLQ, updated.get().status());
    }

    @Test
    void markTerminalFailureReturnsEmptyForSucceededStatus() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        long now = System.currentTimeMillis();

        Optional<ExecutionRecord<Object, Object>> updated = store.markTerminalFailure(
                "tenant-a",
                "exec-1",
                1L,
                ExecutionStatus.SUCCEEDED,
                "exec-1:2:0",
                "ERROR",
                "Error",
                now)
            .await().indefinitely();

        assertFalse(updated.isPresent());
        verify(client, never()).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    void getExecutionReturnsEmptyWhenNotFound() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        when(client.getItem(any(GetItemRequest.class)))
            .thenReturn(GetItemResponse.builder().build());

        Optional<ExecutionRecord<Object, Object>> result = store.getExecution("tenant-a", "exec-1")
            .await().indefinitely();

        assertFalse(result.isPresent());
    }

    @Test
    void findDueExecutionsReturnsEmptyWhenLimitIsZero() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);

        java.util.List<ExecutionRecord<Object, Object>> results = store.findDueExecutions(System.currentTimeMillis(), 0)
            .await().indefinitely();

        assertTrue(results.isEmpty());
        verify(client, never()).scan(any());
    }

    @Test
    void findDueExecutionsReturnsEmptyWhenNegativeLimit() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);

        java.util.List<ExecutionRecord<Object, Object>> results = store.findDueExecutions(System.currentTimeMillis(), -1)
            .await().indefinitely();

        assertTrue(results.isEmpty());
        verify(client, never()).scan(any());
    }

    @Test
    void startupValidationReportsMissingExecutionKeyTable() {
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(null, config);

        var validationError = store.startupValidationError(config);

        assertTrue(validationError.isPresent());
        assertTrue(validationError.get().contains("execution-key-table"));
    }

    @Test
    void startupValidationReportsNullConfig() {
        DynamoExecutionStateStore store = new DynamoExecutionStateStore();

        var validationError = store.startupValidationError(null);

        assertTrue(validationError.isPresent());
        assertTrue(validationError.get().contains("dynamo"));
    }

    @Test
    void claimLeaseSucceedsWithValidCondition() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        long now = System.currentTimeMillis();
        long ttl = now / 1000 + 3600;
        Map<String, AttributeValue> claimedItem = executionItem("tenant-a", "exec-1", "key-1", ttl, ExecutionStatus.RUNNING);
        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenReturn(UpdateItemResponse.builder().attributes(claimedItem).build());

        Optional<ExecutionRecord<Object, Object>> claimed = store.claimLease(
                "tenant-a",
                "exec-1",
                "worker-1",
                now,
                30000)
            .await().indefinitely();

        assertTrue(claimed.isPresent());
        assertEquals(ExecutionStatus.RUNNING, claimed.get().status());
    }

    @Test
    void scheduleRetryReturnsEmptyOnVersionConflict() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenThrow(ConditionalCheckFailedException.builder().message("version mismatch").build());

        Optional<ExecutionRecord<Object, Object>> updated = store.scheduleRetry(
                "tenant-a",
                "exec-1",
                1L,
                2,
                System.currentTimeMillis() + 10000,
                "exec-1:1:0",
                "ERROR",
                "Error message",
                System.currentTimeMillis())
            .await().indefinitely();

        assertFalse(updated.isPresent());
    }

    @Test
    void markSucceededReturnsEmptyOnVersionConflict() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenThrow(ConditionalCheckFailedException.builder().message("version mismatch").build());

        Optional<ExecutionRecord<Object, Object>> updated = store.markSucceeded(
                "tenant-a",
                "exec-1",
                1L,
                "exec-1:0:0",
                "result",
                System.currentTimeMillis())
            .await().indefinitely();

        assertFalse(updated.isPresent());
    }

    @Test
    void markTerminalFailureReturnsEmptyOnVersionConflict() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenThrow(ConditionalCheckFailedException.builder().message("version mismatch").build());

        Optional<ExecutionRecord<Object, Object>> updated = store.markTerminalFailure(
                "tenant-a",
                "exec-1",
                1L,
                ExecutionStatus.FAILED,
                "exec-1:2:0",
                "ERROR",
                "Error message",
                System.currentTimeMillis())
            .await().indefinitely();

        assertFalse(updated.isPresent());
    }

    private static PipelineOrchestratorConfig mockConfig(String executionTable, String keyTable) {
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        PipelineOrchestratorConfig.DynamoConfig dynamo = mock(PipelineOrchestratorConfig.DynamoConfig.class);
        when(config.dynamo()).thenReturn(dynamo);
        when(dynamo.executionTable()).thenReturn(executionTable);
        when(dynamo.executionKeyTable()).thenReturn(keyTable);
        when(dynamo.region()).thenReturn(Optional.empty());
        when(dynamo.endpointOverride()).thenReturn(Optional.empty());
        return config;
    }

    private static Map<String, AttributeValue> executionItem(
        String tenantId,
        String executionId,
        String executionKey,
        long ttl
    ) {
        return executionItem(tenantId, executionId, executionKey, ttl, ExecutionStatus.QUEUED);
    }

    private static Map<String, AttributeValue> executionItem(
        String tenantId,
        String executionId,
        String executionKey,
        long ttl,
        ExecutionStatus status
    ) {
        return Map.ofEntries(
            Map.entry("tenant_id", AttributeValue.builder().s(tenantId).build()),
            Map.entry("execution_id", AttributeValue.builder().s(executionId).build()),
            Map.entry("execution_key", AttributeValue.builder().s(executionKey).build()),
            Map.entry("status", AttributeValue.builder().s(status.name()).build()),
            Map.entry("version", AttributeValue.builder().n("0").build()),
            Map.entry("current_step_index", AttributeValue.builder().n("0").build()),
            Map.entry("attempt", AttributeValue.builder().n("0").build()),
            Map.entry("lease_expires_epoch_ms", AttributeValue.builder().n("0").build()),
            Map.entry("next_due_epoch_ms", AttributeValue.builder().n("0").build()),
            Map.entry("created_at_epoch_ms", AttributeValue.builder().n("1").build()),
            Map.entry("updated_at_epoch_ms", AttributeValue.builder().n("1").build()),
            Map.entry("ttl_epoch_s", AttributeValue.builder().n(Long.toString(ttl)).build()));
    }
}