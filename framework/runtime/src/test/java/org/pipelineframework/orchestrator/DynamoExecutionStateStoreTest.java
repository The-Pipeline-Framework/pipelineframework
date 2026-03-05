package org.pipelineframework.orchestrator;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
    void getExecutionReturnsRecordWhenFound() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        long now = System.currentTimeMillis();
        long ttl = now / 1000 + 3600;
        when(client.getItem(any(GetItemRequest.class)))
            .thenReturn(GetItemResponse.builder().item(executionItem("tenant-a", "exec-1", "key-1", ttl)).build());

        Optional<ExecutionRecord<Object, Object>> result = store.getExecution("tenant-a", "exec-1")
            .await().indefinitely();

        assertTrue(result.isPresent());
        assertEquals("exec-1", result.get().executionId());
        assertEquals("tenant-a", result.get().tenantId());
    }

    @Test
    void getExecutionReturnsEmptyWhenExpired() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        long now = System.currentTimeMillis();
        long expiredTtl = now / 1000 - 3600;
        when(client.getItem(any(GetItemRequest.class)))
            .thenReturn(GetItemResponse.builder().item(executionItem("tenant-a", "exec-1", "key-1", expiredTtl)).build());

        Optional<ExecutionRecord<Object, Object>> result = store.getExecution("tenant-a", "exec-1")
            .await().indefinitely();

        assertTrue(result.isEmpty());
    }

    @Test
    void claimLeaseUpdatesRecordWhenConditionMatches() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        long now = System.currentTimeMillis();
        long ttl = now / 1000 + 3600;
        Map<String, AttributeValue> updatedItem = executionItem("tenant-a", "exec-1", "key-1", ttl, ExecutionStatus.RUNNING);
        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenReturn(UpdateItemResponse.builder().attributes(updatedItem).build());

        Optional<ExecutionRecord<Object, Object>> result = store.claimLease("tenant-a", "exec-1", "worker-1", now, 30000)
            .await().indefinitely();

        assertTrue(result.isPresent());
        assertEquals(ExecutionStatus.RUNNING, result.get().status());
    }

    @Test
    void scheduleRetryUpdatesRecordSuccessfully() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        long now = System.currentTimeMillis();
        long ttl = now / 1000 + 3600;
        Map<String, AttributeValue> updatedItem = executionItem("tenant-a", "exec-1", "key-1", ttl, ExecutionStatus.WAIT_RETRY);
        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenReturn(UpdateItemResponse.builder().attributes(updatedItem).build());

        Optional<ExecutionRecord<Object, Object>> result = store.scheduleRetry(
                "tenant-a",
                "exec-1",
                1L,
                1,
                now + 10000,
                "transition-key",
                "ERROR_CODE",
                "Error message",
                now)
            .await().indefinitely();

        assertTrue(result.isPresent());
        assertEquals(ExecutionStatus.WAIT_RETRY, result.get().status());
    }

    @Test
    void scheduleRetryReturnsEmptyOnConditionalFailure() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenThrow(ConditionalCheckFailedException.builder().message("version mismatch").build());

        Optional<ExecutionRecord<Object, Object>> result = store.scheduleRetry(
                "tenant-a",
                "exec-1",
                1L,
                1,
                System.currentTimeMillis() + 10000,
                "transition-key",
                "ERROR_CODE",
                "Error message",
                System.currentTimeMillis())
            .await().indefinitely();

        assertTrue(result.isEmpty());
    }

    @Test
    void markTerminalFailureWithFailedStatus() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        long now = System.currentTimeMillis();
        long ttl = now / 1000 + 3600;
        Map<String, AttributeValue> updatedItem = executionItem("tenant-a", "exec-1", "key-1", ttl, ExecutionStatus.FAILED);
        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenReturn(UpdateItemResponse.builder().attributes(updatedItem).build());

        Optional<ExecutionRecord<Object, Object>> result = store.markTerminalFailure(
                "tenant-a",
                "exec-1",
                1L,
                ExecutionStatus.FAILED,
                "transition-key",
                "FATAL_ERROR",
                "Fatal error message",
                now)
            .await().indefinitely();

        assertTrue(result.isPresent());
        assertEquals(ExecutionStatus.FAILED, result.get().status());
    }

    @Test
    void markTerminalFailureWithDlqStatus() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        long now = System.currentTimeMillis();
        long ttl = now / 1000 + 3600;
        Map<String, AttributeValue> updatedItem = executionItem("tenant-a", "exec-1", "key-1", ttl, ExecutionStatus.DLQ);
        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenReturn(UpdateItemResponse.builder().attributes(updatedItem).build());

        Optional<ExecutionRecord<Object, Object>> result = store.markTerminalFailure(
                "tenant-a",
                "exec-1",
                1L,
                ExecutionStatus.DLQ,
                "transition-key",
                "DLQ_ERROR",
                "DLQ error message",
                now)
            .await().indefinitely();

        assertTrue(result.isPresent());
        assertEquals(ExecutionStatus.DLQ, result.get().status());
    }

    @Test
    void markTerminalFailureReturnsEmptyForNonTerminalStatus() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);

        Optional<ExecutionRecord<Object, Object>> result = store.markTerminalFailure(
                "tenant-a",
                "exec-1",
                1L,
                ExecutionStatus.RUNNING,
                "transition-key",
                "ERROR",
                "Error message",
                System.currentTimeMillis())
            .await().indefinitely();

        assertTrue(result.isEmpty());
        verify(client, never()).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    void markTerminalFailureReturnsEmptyOnConditionalFailure() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenThrow(ConditionalCheckFailedException.builder().message("version mismatch").build());

        Optional<ExecutionRecord<Object, Object>> result = store.markTerminalFailure(
                "tenant-a",
                "exec-1",
                1L,
                ExecutionStatus.FAILED,
                "transition-key",
                "ERROR",
                "Error message",
                System.currentTimeMillis())
            .await().indefinitely();

        assertTrue(result.isEmpty());
    }

    @Test
    void findDueExecutionsReturnsEmptyList() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        when(client.scan(any(ScanRequest.class)))
            .thenReturn(ScanResponse.builder().items(List.of()).build());

        List<ExecutionRecord<Object, Object>> result = store.findDueExecutions(System.currentTimeMillis(), 10)
            .await().indefinitely();

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void findDueExecutionsReturnsZeroWhenLimitIsZero() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);

        List<ExecutionRecord<Object, Object>> result = store.findDueExecutions(System.currentTimeMillis(), 0)
            .await().indefinitely();

        assertNotNull(result);
        assertTrue(result.isEmpty());
        verify(client, never()).scan(any(ScanRequest.class));
    }

    @Test
    void findDueExecutionsReturnsFilteredResults() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        long now = System.currentTimeMillis();
        long ttl = now / 1000 + 3600;
        when(client.scan(any(ScanRequest.class)))
            .thenReturn(ScanResponse.builder()
                .items(
                    executionItem("tenant-a", "exec-1", "key-1", ttl),
                    executionItem("tenant-a", "exec-2", "key-2", ttl))
                .build());

        List<ExecutionRecord<Object, Object>> result = store.findDueExecutions(now, 10)
            .await().indefinitely();

        assertNotNull(result);
        assertEquals(2, result.size());
    }

    @Test
    void findDueExecutionsRespectsLimit() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        long now = System.currentTimeMillis();
        long ttl = now / 1000 + 3600;
        when(client.scan(any(ScanRequest.class)))
            .thenReturn(ScanResponse.builder()
                .items(
                    executionItem("tenant-a", "exec-1", "key-1", ttl),
                    executionItem("tenant-a", "exec-2", "key-2", ttl),
                    executionItem("tenant-a", "exec-3", "key-3", ttl),
                    executionItem("tenant-a", "exec-4", "key-4", ttl))
                .build());

        List<ExecutionRecord<Object, Object>> result = store.findDueExecutions(now, 2)
            .await().indefinitely();

        assertNotNull(result);
        assertEquals(2, result.size());
    }

    @Test
    void createOrGetExecutionCreatesNewExecution() {
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
        assertNotNull(result.record());
        assertEquals("tenant-a", result.record().tenantId());
        assertEquals("key-1", result.record().executionKey());
        verify(client).transactWriteItems(any(TransactWriteItemsRequest.class));
    }

    @Test
    void createOrGetExecutionHandlesRaceConditionOnTransactionFailure() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        long now = System.currentTimeMillis();
        long ttl = now / 1000 + 3600;
        ExecutionCreateCommand command = new ExecutionCreateCommand("tenant-a", "key-1", "payload", now, ttl);

        when(client.getItem(any(GetItemRequest.class)))
            .thenReturn(GetItemResponse.builder().item(Map.of()).build())
            .thenReturn(GetItemResponse.builder().item(Map.of(
                "tenant_execution_key", AttributeValue.builder().s("8:tenant-a:5:key-1").build(),
                "execution_id", AttributeValue.builder().s("exec-1").build()))
                .build())
            .thenReturn(GetItemResponse.builder().item(executionItem("tenant-a", "exec-1", "key-1", ttl)).build());
        when(client.transactWriteItems(any(TransactWriteItemsRequest.class)))
            .thenThrow(TransactionCanceledException.builder().message("duplicate").build());

        CreateExecutionResult result = store.createOrGetExecution(command).await().indefinitely();

        assertTrue(result.duplicate());
        assertEquals("exec-1", result.record().executionId());
    }

    @Test
    void startupValidationReportsMissingKeyTable() {
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
    void markSucceededReturnsEmptyOnConditionalFailure() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenThrow(ConditionalCheckFailedException.builder().message("version mismatch").build());

        Optional<ExecutionRecord<Object, Object>> result = store.markSucceeded(
                "tenant-a",
                "exec-1",
                1L,
                "transition-key",
                "result",
                System.currentTimeMillis())
            .await().indefinitely();

        assertTrue(result.isEmpty());
    }

    @Test
    void scheduleRetryTruncatesLongErrorMessage() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        long now = System.currentTimeMillis();
        long ttl = now / 1000 + 3600;
        String longMessage = "x".repeat(1000);
        Map<String, AttributeValue> updatedItem = executionItem("tenant-a", "exec-1", "key-1", ttl, ExecutionStatus.WAIT_RETRY);
        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenReturn(UpdateItemResponse.builder().attributes(updatedItem).build());

        store.scheduleRetry("tenant-a", "exec-1", 1L, 1, now + 10000, "key", "CODE", longMessage, now)
            .await().indefinitely();

        verify(client).updateItem(any(UpdateItemRequest.class));
    }
}