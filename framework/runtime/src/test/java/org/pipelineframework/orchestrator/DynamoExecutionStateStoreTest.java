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
    void claimLeaseRejectsNonPositiveLeaseDuration() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () ->
            store.claimLease("tenant-a", "exec-1", "worker-1", System.currentTimeMillis(), 0)
                .await().indefinitely());

        assertTrue(error.getMessage().contains("leaseMs must be > 0"));
        verifyNoInteractions(client);
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
    void markTerminalFailureRejectsUnsupportedStatus() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () ->
            store.markTerminalFailure(
                    "tenant-a",
                    "exec-1",
                    1L,
                    ExecutionStatus.SUCCEEDED,
                    "exec-1:0:0",
                    "ERR",
                    "unsupported",
                    System.currentTimeMillis())
                .await().indefinitely());

        assertTrue(error.getMessage().contains("Unsupported terminal status"));
        verifyNoInteractions(client);
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
    void scheduleRetryUpdatesExecutionWithRetryDetails() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        long now = System.currentTimeMillis();
        long nextDue = now + 10000;
        long ttl = now / 1000 + 3600;
        Map<String, AttributeValue> retryItem = executionItem("tenant-a", "exec-1", "key-1", ttl, ExecutionStatus.WAIT_RETRY);
        retryItem.put("attempt", AttributeValue.builder().n("1").build());
        retryItem.put("next_due_epoch_ms", AttributeValue.builder().n(Long.toString(nextDue)).build());
        retryItem.put("error_code", AttributeValue.builder().s("TIMEOUT").build());
        retryItem.put("error_message", AttributeValue.builder().s("Request timeout").build());

        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenReturn(UpdateItemResponse.builder().attributes(retryItem).build());

        Optional<ExecutionRecord<Object, Object>> result = store.scheduleRetry(
                "tenant-a",
                "exec-1",
                0L,
                1,
                nextDue,
                "exec-1:0:1",
                "TIMEOUT",
                "Request timeout",
                now)
            .await().indefinitely();

        assertTrue(result.isPresent());
        assertEquals(ExecutionStatus.WAIT_RETRY, result.get().status());
        assertEquals(1, result.get().attempt());
        assertEquals("TIMEOUT", result.get().errorCode());
        assertEquals("Request timeout", result.get().errorMessage());
    }

    @Test
    void markTerminalFailureWithDLQStatusSucceeds() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        long now = System.currentTimeMillis();
        long ttl = now / 1000 + 3600;
        Map<String, AttributeValue> dlqItem = executionItem("tenant-a", "exec-1", "key-1", ttl, ExecutionStatus.DLQ);

        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenReturn(UpdateItemResponse.builder().attributes(dlqItem).build());

        Optional<ExecutionRecord<Object, Object>> result = store.markTerminalFailure(
                "tenant-a",
                "exec-1",
                0L,
                ExecutionStatus.DLQ,
                "exec-1:0:0",
                "MAX_RETRIES",
                "Maximum retries exceeded",
                now)
            .await().indefinitely();

        assertTrue(result.isPresent());
        assertEquals(ExecutionStatus.DLQ, result.get().status());
    }

    @Test
    void markTerminalFailureWithFailedStatusSucceeds() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        long now = System.currentTimeMillis();
        long ttl = now / 1000 + 3600;
        Map<String, AttributeValue> failedItem = executionItem("tenant-a", "exec-1", "key-1", ttl, ExecutionStatus.FAILED);

        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenReturn(UpdateItemResponse.builder().attributes(failedItem).build());

        Optional<ExecutionRecord<Object, Object>> result = store.markTerminalFailure(
                "tenant-a",
                "exec-1",
                0L,
                ExecutionStatus.FAILED,
                "exec-1:0:0",
                "FATAL",
                "Fatal error",
                now)
            .await().indefinitely();

        assertTrue(result.isPresent());
        assertEquals(ExecutionStatus.FAILED, result.get().status());
    }

    @Test
    void findDueExecutionsReturnsEmptyListWhenLimitIsZero() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);

        var result = store.findDueExecutions(System.currentTimeMillis(), 0)
            .await().indefinitely();

        assertTrue(result.isEmpty());
        verifyNoInteractions(client);
    }

    @Test
    void claimLeaseUpdatesLeaseOwnerAndExpiry() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        long now = System.currentTimeMillis();
        long leaseExpiry = now + 30000;
        long ttl = now / 1000 + 3600;
        Map<String, AttributeValue> claimedItem = executionItem("tenant-a", "exec-1", "key-1", ttl, ExecutionStatus.RUNNING);
        claimedItem.put("lease_owner", AttributeValue.builder().s("worker-1").build());
        claimedItem.put("lease_expires_epoch_ms", AttributeValue.builder().n(Long.toString(leaseExpiry)).build());

        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenReturn(UpdateItemResponse.builder().attributes(claimedItem).build());

        Optional<ExecutionRecord<Object, Object>> result = store.claimLease(
                "tenant-a",
                "exec-1",
                "worker-1",
                now,
                30000)
            .await().indefinitely();

        assertTrue(result.isPresent());
        assertEquals(ExecutionStatus.RUNNING, result.get().status());
        assertEquals("worker-1", result.get().leaseOwner());
    }

    @Test
    void scheduleRetryReturnsEmptyOnVersionMismatch() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenThrow(ConditionalCheckFailedException.builder().message("version mismatch").build());

        Optional<ExecutionRecord<Object, Object>> result = store.scheduleRetry(
                "tenant-a",
                "exec-1",
                0L,
                1,
                System.currentTimeMillis() + 10000,
                "exec-1:0:1",
                "ERROR",
                "Test error",
                System.currentTimeMillis())
            .await().indefinitely();

        assertTrue(result.isEmpty());
    }

    @Test
    void markSucceededReturnsEmptyOnVersionMismatch() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenThrow(ConditionalCheckFailedException.builder().message("version mismatch").build());

        Optional<ExecutionRecord<Object, Object>> result = store.markSucceeded(
                "tenant-a",
                "exec-1",
                1L,
                "exec-1:0:0",
                "result",
                System.currentTimeMillis())
            .await().indefinitely();

        assertTrue(result.isEmpty());
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
    void startupValidationReportsMissingDynamoConfig() {
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        when(config.dynamo()).thenReturn(null);
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(null, config);

        var validationError = store.startupValidationError(config);

        assertTrue(validationError.isPresent());
        assertTrue(validationError.get().contains("dynamo.* configuration"));
    }
}