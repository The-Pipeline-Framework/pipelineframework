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

    @Test
    void scheduleRetryReturnsUpdatedRecordOnSuccess() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        long now = System.currentTimeMillis();
        long ttl = now / 1000 + 3600;
        Map<String, AttributeValue> updatedItem = executionItem("tenant-a", "exec-2", "key-2", ttl);
        updatedItem.put("status", AttributeValue.builder().s(ExecutionStatus.WAIT_RETRY.name()).build());
        updatedItem.put("attempt", AttributeValue.builder().n("1").build());
        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenReturn(UpdateItemResponse.builder().attributes(updatedItem).build());

        Optional<ExecutionRecord<Object, Object>> updated = store.scheduleRetry(
                "tenant-a",
                "exec-2",
                0L,
                1,
                now + 5000,
                "exec-2:0:0",
                "TIMEOUT",
                "timeout occurred",
                now)
            .await().indefinitely();

        assertTrue(updated.isPresent());
        assertEquals(ExecutionStatus.WAIT_RETRY, updated.get().status());
    }

    @Test
    void scheduleRetryReturnsEmptyOnStaleVersion() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenThrow(ConditionalCheckFailedException.builder().message("version mismatch").build());

        Optional<ExecutionRecord<Object, Object>> updated = store.scheduleRetry(
                "tenant-a",
                "exec-3",
                5L,
                2,
                System.currentTimeMillis() + 10000,
                "exec-3:0:1",
                "ERROR",
                "error message",
                System.currentTimeMillis())
            .await().indefinitely();

        assertTrue(updated.isEmpty());
    }

    @Test
    void markTerminalFailureWithFailedStatusUpdatesRecord() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        long now = System.currentTimeMillis();
        long ttl = now / 1000 + 3600;
        Map<String, AttributeValue> failedItem = executionItem("tenant-a", "exec-4", "key-4", ttl, ExecutionStatus.FAILED);
        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenReturn(UpdateItemResponse.builder().attributes(failedItem).build());

        Optional<ExecutionRecord<Object, Object>> updated = store.markTerminalFailure(
                "tenant-a",
                "exec-4",
                2L,
                ExecutionStatus.FAILED,
                "exec-4:0:2",
                "MAX_RETRIES",
                "max retries exceeded",
                now)
            .await().indefinitely();

        assertTrue(updated.isPresent());
        assertEquals(ExecutionStatus.FAILED, updated.get().status());
    }

    @Test
    void markTerminalFailureWithDlqStatusUpdatesRecord() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        long now = System.currentTimeMillis();
        long ttl = now / 1000 + 3600;
        Map<String, AttributeValue> dlqItem = executionItem("tenant-a", "exec-5", "key-5", ttl, ExecutionStatus.DLQ);
        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenReturn(UpdateItemResponse.builder().attributes(dlqItem).build());

        Optional<ExecutionRecord<Object, Object>> updated = store.markTerminalFailure(
                "tenant-a",
                "exec-5",
                3L,
                ExecutionStatus.DLQ,
                "exec-5:0:3",
                "UNRECOVERABLE",
                "sent to DLQ",
                now)
            .await().indefinitely();

        assertTrue(updated.isPresent());
        assertEquals(ExecutionStatus.DLQ, updated.get().status());
    }

    @Test
    void markTerminalFailureWithSucceededStatusReturnsEmpty() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);

        Optional<ExecutionRecord<Object, Object>> updated = store.markTerminalFailure(
                "tenant-a",
                "exec-6",
                1L,
                ExecutionStatus.SUCCEEDED,
                "exec-6:0:1",
                "ERROR",
                "should not update",
                System.currentTimeMillis())
            .await().indefinitely();

        assertTrue(updated.isEmpty());
        verify(client, never()).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    void getExecutionReturnsEmptyForNonExistentRecord() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        when(client.getItem(any(GetItemRequest.class)))
            .thenReturn(GetItemResponse.builder().build());

        Optional<ExecutionRecord<Object, Object>> result = store.getExecution("tenant-a", "nonexistent")
            .await().indefinitely();

        assertTrue(result.isEmpty());
    }

    @Test
    void getExecutionReturnsEmptyForExpiredRecord() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        long now = System.currentTimeMillis();
        long expiredTtl = now / 1000 - 100;
        when(client.getItem(any(GetItemRequest.class)))
            .thenReturn(GetItemResponse.builder()
                .item(executionItem("tenant-a", "exec-7", "key-7", expiredTtl))
                .build());

        Optional<ExecutionRecord<Object, Object>> result = store.getExecution("tenant-a", "exec-7")
            .await().indefinitely();

        assertTrue(result.isEmpty());
    }

    @Test
    void findDueExecutionsReturnsEmptyListForZeroLimit() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);

        var result = store.findDueExecutions(System.currentTimeMillis(), 0)
            .await().indefinitely();

        assertTrue(result.isEmpty());
        verify(client, never()).scan(any());
    }

    @Test
    void findDueExecutionsReturnsEmptyListForNegativeLimit() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);

        var result = store.findDueExecutions(System.currentTimeMillis(), -5)
            .await().indefinitely();

        assertTrue(result.isEmpty());
        verify(client, never()).scan(any());
    }

    @Test
    void claimLeaseSucceedsWhenConditionsMet() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        long now = System.currentTimeMillis();
        long ttl = now / 1000 + 3600;
        Map<String, AttributeValue> runningItem = executionItem("tenant-a", "exec-8", "key-8", ttl, ExecutionStatus.RUNNING);
        runningItem.put("lease_owner", AttributeValue.builder().s("worker-1").build());
        runningItem.put("lease_expires_epoch_ms", AttributeValue.builder().n(Long.toString(now + 30000)).build());
        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenReturn(UpdateItemResponse.builder().attributes(runningItem).build());

        Optional<ExecutionRecord<Object, Object>> claimed = store.claimLease(
                "tenant-a",
                "exec-8",
                "worker-1",
                now,
                30000)
            .await().indefinitely();

        assertTrue(claimed.isPresent());
        assertEquals(ExecutionStatus.RUNNING, claimed.get().status());
    }

    @Test
    void markSucceededReturnsEmptyWhenVersionMismatch() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenThrow(ConditionalCheckFailedException.builder().message("version conflict").build());

        Optional<ExecutionRecord<Object, Object>> updated = store.markSucceeded(
                "tenant-a",
                "exec-9",
                10L,
                "exec-9:0:0",
                "result",
                System.currentTimeMillis())
            .await().indefinitely();

        assertTrue(updated.isEmpty());
    }
}