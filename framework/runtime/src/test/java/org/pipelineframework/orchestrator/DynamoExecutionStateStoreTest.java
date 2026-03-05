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
    void scheduleRetryUpdatesExecutionToWaitRetryStatus() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        long now = System.currentTimeMillis();
        long nextDue = now + 10000;
        long ttl = now / 1000 + 3600;
        Map<String, AttributeValue> updatedItem = executionItem("tenant-a", "exec-1", "key-1", ttl);
        updatedItem.put("status", AttributeValue.builder().s(ExecutionStatus.WAIT_RETRY.name()).build());
        updatedItem.put("attempt", AttributeValue.builder().n("1").build());
        updatedItem.put("next_due_epoch_ms", AttributeValue.builder().n(Long.toString(nextDue)).build());
        updatedItem.put("error_code", AttributeValue.builder().s("TIMEOUT").build());
        updatedItem.put("error_message", AttributeValue.builder().s("Request timed out").build());

        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenReturn(UpdateItemResponse.builder().attributes(updatedItem).build());

        Optional<ExecutionRecord<Object, Object>> result = store.scheduleRetry(
                "tenant-a", "exec-1", 0L, 1, nextDue,
                "exec-1:0:1", "TIMEOUT", "Request timed out", now)
            .await().indefinitely();

        assertTrue(result.isPresent());
        assertEquals(ExecutionStatus.WAIT_RETRY, result.get().status());
        verify(client).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    void scheduleRetryReturnsEmptyOnVersionMismatch() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenThrow(ConditionalCheckFailedException.builder().message("version mismatch").build());

        Optional<ExecutionRecord<Object, Object>> result = store.scheduleRetry(
                "tenant-a", "exec-1", 5L, 2, System.currentTimeMillis() + 20000,
                "exec-1:0:2", "ERROR", "failed", System.currentTimeMillis())
            .await().indefinitely();

        assertTrue(result.isEmpty());
    }

    @Test
    void markTerminalFailureUpdatesStatusToFailed() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        long now = System.currentTimeMillis();
        long ttl = now / 1000 + 3600;
        Map<String, AttributeValue> failedItem = executionItem("tenant-a", "exec-1", "key-1", ttl, ExecutionStatus.FAILED);
        failedItem.put("error_code", AttributeValue.builder().s("MAX_RETRIES").build());
        failedItem.put("error_message", AttributeValue.builder().s("Max retries exceeded").build());

        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenReturn(UpdateItemResponse.builder().attributes(failedItem).build());

        Optional<ExecutionRecord<Object, Object>> result = store.markTerminalFailure(
                "tenant-a", "exec-1", 3L, ExecutionStatus.FAILED,
                "exec-1:0:3", "MAX_RETRIES", "Max retries exceeded", now)
            .await().indefinitely();

        assertTrue(result.isPresent());
        assertEquals(ExecutionStatus.FAILED, result.get().status());
    }

    @Test
    void markTerminalFailureUpdatesStatusToDlq() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        long now = System.currentTimeMillis();
        long ttl = now / 1000 + 3600;
        Map<String, AttributeValue> dlqItem = executionItem("tenant-a", "exec-1", "key-1", ttl, ExecutionStatus.DLQ);

        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenReturn(UpdateItemResponse.builder().attributes(dlqItem).build());

        Optional<ExecutionRecord<Object, Object>> result = store.markTerminalFailure(
                "tenant-a", "exec-1", 1L, ExecutionStatus.DLQ,
                "exec-1:0:1", "FATAL", "Fatal error", now)
            .await().indefinitely();

        assertTrue(result.isPresent());
        assertEquals(ExecutionStatus.DLQ, result.get().status());
    }

    @Test
    void markTerminalFailureRejectsNonTerminalStatus() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);

        Optional<ExecutionRecord<Object, Object>> result = store.markTerminalFailure(
                "tenant-a", "exec-1", 1L, ExecutionStatus.RUNNING,
                "exec-1:0:1", "ERROR", "error", System.currentTimeMillis())
            .await().indefinitely();

        assertTrue(result.isEmpty());
        verify(client, never()).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    void getExecutionReturnsRecordWhenFound() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        long now = System.currentTimeMillis();
        long ttl = now / 1000 + 3600;

        when(client.getItem(any(GetItemRequest.class)))
            .thenReturn(GetItemResponse.builder()
                .item(executionItem("tenant-a", "exec-1", "key-1", ttl))
                .build());

        Optional<ExecutionRecord<Object, Object>> result = store.getExecution("tenant-a", "exec-1")
            .await().indefinitely();

        assertTrue(result.isPresent());
        assertEquals("exec-1", result.get().executionId());
        assertEquals("tenant-a", result.get().tenantId());
    }

    @Test
    void getExecutionReturnsEmptyWhenNotFound() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);

        when(client.getItem(any(GetItemRequest.class)))
            .thenReturn(GetItemResponse.builder().item(Map.of()).build());

        Optional<ExecutionRecord<Object, Object>> result = store.getExecution("tenant-a", "exec-1")
            .await().indefinitely();

        assertTrue(result.isEmpty());
    }

    @Test
    void getExecutionDeletesExpiredRecord() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        long now = System.currentTimeMillis();
        long expiredTtl = (now / 1000) - 100;

        when(client.getItem(any(GetItemRequest.class)))
            .thenReturn(GetItemResponse.builder()
                .item(executionItem("tenant-a", "exec-1", "key-1", expiredTtl))
                .build());

        Optional<ExecutionRecord<Object, Object>> result = store.getExecution("tenant-a", "exec-1")
            .await().indefinitely();

        assertTrue(result.isEmpty());
        verify(client, atLeastOnce()).deleteItem(any(software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest.class));
    }

    @Test
    void claimLeaseSucceedsWhenConditionsMet() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        long now = System.currentTimeMillis();
        long ttl = now / 1000 + 3600;
        Map<String, AttributeValue> runningItem = executionItem("tenant-a", "exec-1", "key-1", ttl, ExecutionStatus.RUNNING);
        runningItem.put("lease_owner", AttributeValue.builder().s("worker-1").build());
        runningItem.put("lease_expires_epoch_ms", AttributeValue.builder().n(Long.toString(now + 30000)).build());

        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenReturn(UpdateItemResponse.builder().attributes(runningItem).build());

        Optional<ExecutionRecord<Object, Object>> claimed = store.claimLease(
                "tenant-a", "exec-1", "worker-1", now, 30000)
            .await().indefinitely();

        assertTrue(claimed.isPresent());
        assertEquals(ExecutionStatus.RUNNING, claimed.get().status());
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
    void createOrGetCreatesNewExecutionWhenNotExists() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        long now = System.currentTimeMillis();
        long ttl = now / 1000 + 3600;
        ExecutionCreateCommand command = new ExecutionCreateCommand("tenant-a", "key-1", "payload", now, ttl);

        when(client.getItem(any(GetItemRequest.class)))
            .thenReturn(GetItemResponse.builder().item(Map.of()).build());

        CreateExecutionResult result = store.createOrGetExecution(command).await().indefinitely();

        assertFalse(result.duplicate());
        assertEquals("tenant-a", result.record().tenantId());
        assertEquals("key-1", result.record().executionKey());
        assertEquals(ExecutionStatus.QUEUED, result.record().status());
        verify(client).transactWriteItems(any(TransactWriteItemsRequest.class));
    }

    @Test
    void markSucceededReturnsEmptyOnVersionMismatch() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenThrow(ConditionalCheckFailedException.builder().message("version mismatch").build());

        Optional<ExecutionRecord<Object, Object>> result = store.markSucceeded(
                "tenant-a", "exec-1", 99L, "transition", "result", System.currentTimeMillis())
            .await().indefinitely();

        assertTrue(result.isEmpty());
    }

    @Test
    void markTerminalFailureReturnsEmptyOnVersionMismatch() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenThrow(ConditionalCheckFailedException.builder().message("version mismatch").build());

        Optional<ExecutionRecord<Object, Object>> result = store.markTerminalFailure(
                "tenant-a", "exec-1", 99L, ExecutionStatus.FAILED,
                "transition", "ERROR", "error", System.currentTimeMillis())
            .await().indefinitely();

        assertTrue(result.isEmpty());
    }
}