package org.pipelineframework.awaitable.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.pipelineframework.awaitable.AwaitCreateCommand;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

class DynamoAwaitInteractionStoreTest {

    @Test
    void createOrGetWritesUnitPendingAndDeadlineQueryKeys() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        DynamoAwaitInteractionStore store = new DynamoAwaitInteractionStore(client, mockConfig());
        when(client.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().build());

        var result = store.createOrGet(command(
                "tenant-a",
                "execution-1",
                "review",
                "idem-1",
                "corr-1",
                "unit-1",
                2,
                "alice",
                "finance",
                20_000L))
            .await().indefinitely();

        assertFalse(result.duplicate());
        ArgumentCaptor<TransactWriteItemsRequest> captor = ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
        verify(client).transactWriteItems(captor.capture());
        Map<String, AttributeValue> item = captor.getValue().transactItems().getFirst().put().item();
        assertEquals(scoped("tenant-a", "unit-1"), item.get("query_unit_key").s());
        assertTrue(item.get("query_unit_sort").s().startsWith("0000000002#cause-1#"));
        assertEquals("tenant-a", item.get("query_pending_tenant_key").s());
        assertEquals(scoped("tenant-a", "alice"), item.get("query_pending_assignee_key").s());
        assertEquals(scoped("tenant-a", "finance"), item.get("query_pending_group_key").s());
        assertEquals(scoped("tenant-a", "review"), item.get("query_pending_step_key").s());
        assertTrue(item.get("query_pending_deadline_sort").s().startsWith("0000000000000020000#"));
        assertEquals("active", item.get("query_deadline_key").s());
        assertTrue(item.get("query_deadline_sort").s().startsWith("0000000000000020000#tenant-a#"));
    }

    @Test
    void findByUnitQueriesUnitIndexAndKeepsTenantsAndUnitsIsolated() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        DynamoAwaitInteractionStore store = new DynamoAwaitInteractionStore(client, mockConfig());
        when(client.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder()
            .items(List.of(
                item("tenant-a", "interaction-2", "unit-1", 2, AwaitInteractionStatus.WAITING, 30_000L, "alice", "finance"),
                item("tenant-b", "interaction-other-tenant", "unit-1", 1, AwaitInteractionStatus.WAITING, 10_000L, "alice", "finance"),
                item("tenant-a", "interaction-other-unit", "unit-2", 1, AwaitInteractionStatus.WAITING, 10_000L, "alice", "finance"),
                item("tenant-a", "interaction-0", "unit-1", 0, AwaitInteractionStatus.WAITING, 10_000L, "alice", "finance")))
            .build());

        List<AwaitInteractionRecord> records = store.findByUnit("tenant-a", "unit-1").await().indefinitely();

        assertEquals(List.of(0, 2), records.stream().map(AwaitInteractionRecord::itemIndex).toList());
        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(client).query(captor.capture());
        QueryRequest request = captor.getValue();
        assertEquals("await-interaction-by-unit", request.indexName());
        assertEquals("#unitKey = :unitKey", request.keyConditionExpression());
        assertEquals("query_unit_key", request.expressionAttributeNames().get("#unitKey"));
        assertEquals(scoped("tenant-a", "unit-1"), request.expressionAttributeValues().get(":unitKey").s());
        verify(client, never()).scan(any(ScanRequest.class));
    }

    @Test
    void queryPendingUsesNarrowestAssigneeIndexAndFiltersRemainingFields() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        DynamoAwaitInteractionStore store = new DynamoAwaitInteractionStore(client, mockConfig());
        when(client.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder()
            .items(List.of(
                item("tenant-a", "interaction-1", "unit-1", null, AwaitInteractionStatus.WAITING, 10_000L, "alice", "finance"),
                item("tenant-a", "interaction-other-group", "unit-2", null, AwaitInteractionStatus.WAITING, 11_000L, "alice", "legal"),
                item("tenant-a", "interaction-terminal", "unit-3", null, AwaitInteractionStatus.COMPLETED, 9_000L, "alice", "finance")))
            .build());

        List<AwaitInteractionRecord> records = store.queryPending("tenant-a", "alice", "finance", "review", 10)
            .await().indefinitely();

        assertEquals(1, records.size());
        assertEquals("interaction-1", records.getFirst().interactionId());
        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(client).query(captor.capture());
        QueryRequest request = captor.getValue();
        assertEquals("await-interaction-pending-by-assignee", request.indexName());
        assertEquals("#pendingKey = :pendingKey", request.keyConditionExpression());
        assertEquals("query_pending_assignee_key", request.expressionAttributeNames().get("#pendingKey"));
        assertEquals(scoped("tenant-a", "alice"), request.expressionAttributeValues().get(":pendingKey").s());
        assertFalse(request.filterExpression().contains("#assignee = :assignee"));
        assertTrue(request.filterExpression().contains("#group = :group"));
        assertTrue(request.filterExpression().contains("#stepId = :stepId"));
        verify(client, never()).scan(any(ScanRequest.class));
    }

    @Test
    void findTimedOutQueriesDeadlineIndexAcrossTenantsAndRespectsLimit() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        DynamoAwaitInteractionStore store = new DynamoAwaitInteractionStore(client, mockConfig());
        when(client.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder()
            .items(List.of(
                item("tenant-b", "interaction-b", "unit-b", null, AwaitInteractionStatus.WAITING, 1_500L, "bob", "legal"),
                item("tenant-a", "interaction-a", "unit-a", null, AwaitInteractionStatus.WAITING, 1_000L, "alice", "finance"),
                item("tenant-c", "interaction-future", "unit-c", null, AwaitInteractionStatus.WAITING, 4_000L, "carol", "ops"),
                item("tenant-d", "interaction-terminal", "unit-d", null, AwaitInteractionStatus.CANCELLED, 500L, "dan", "ops")))
            .build());

        List<AwaitInteractionRecord> records = store.findTimedOut(2_000L, 2).await().indefinitely();

        assertEquals(List.of("interaction-a", "interaction-b"), records.stream().map(AwaitInteractionRecord::interactionId).toList());
        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(client).query(captor.capture());
        QueryRequest request = captor.getValue();
        assertEquals("await-interaction-pending-by-deadline", request.indexName());
        assertEquals("#deadlineKey = :deadlineKey AND #deadlineSort <= :deadlineCutoff", request.keyConditionExpression());
        assertEquals("active", request.expressionAttributeValues().get(":deadlineKey").s());
        assertEquals("0000000000000002000~", request.expressionAttributeValues().get(":deadlineCutoff").s());
        verify(client, never()).scan(any(ScanRequest.class));
    }

    @Test
    void terminalTransitionRemovesActivePendingAndDeadlineQueryKeys() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        DynamoAwaitInteractionStore store = new DynamoAwaitInteractionStore(client, mockConfig());
        when(client.updateItem(any(UpdateItemRequest.class))).thenReturn(UpdateItemResponse.builder()
            .attributes(item("tenant-a", "interaction-1", "unit-1", null, AwaitInteractionStatus.TIMED_OUT, 10_000L, "alice", "finance"))
            .build());

        var updated = store.markTimedOut("tenant-a", "interaction-1", 0L, 11_000L).await().indefinitely();

        assertTrue(updated.isPresent());
        ArgumentCaptor<UpdateItemRequest> captor = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(client).updateItem(captor.capture());
        String updateExpression = captor.getValue().updateExpression();
        assertTrue(updateExpression.contains("REMOVE"));
        assertTrue(updateExpression.contains("#pendingTenantKey"));
        assertTrue(updateExpression.contains("#pendingAssigneeKey"));
        assertTrue(updateExpression.contains("#pendingGroupKey"));
        assertTrue(updateExpression.contains("#pendingStepKey"));
        assertTrue(updateExpression.contains("#deadlineKey"));
        assertTrue(updateExpression.contains("#deadlineSort"));
        verify(client, never()).scan(any(ScanRequest.class));
    }

    private static AwaitCreateCommand command(
        String tenantId,
        String executionId,
        String stepId,
        String idempotencyKey,
        String correlationId,
        String unitId,
        Integer itemIndex,
        String assignee,
        String group,
        long deadlineEpochMs) {
        return new AwaitCreateCommand(
            tenantId,
            executionId,
            stepId,
            0,
            String.class.getName(),
            "cause-1",
            idempotencyKey,
            correlationId,
            Map.of("orderId", "o-1"),
            assignee,
            group,
            "interaction-api",
            unitId,
            itemIndex,
            1_000L,
            deadlineEpochMs,
            Long.MAX_VALUE);
    }

    private static Map<String, AttributeValue> item(
        String tenantId,
        String interactionId,
        String unitId,
        Integer itemIndex,
        AwaitInteractionStatus status,
        long deadlineEpochMs,
        String assignee,
        String group) {
        Map<String, AttributeValue> item = new java.util.HashMap<>();
        item.put("tenant_id", avS(tenantId));
        item.put("interaction_id", avS(interactionId));
        item.put("execution_id", avS("execution-" + interactionId));
        item.put("step_id", avS("review"));
        item.put("step_index", avN(0));
        item.put("output_type", avS(String.class.getName()));
        item.put("correlation_id", avS("correlation-" + interactionId));
        item.put("causation_id", avS("cause-" + interactionId));
        item.put("idempotency_key", avS("idem-" + interactionId));
        item.put("version", avN(0));
        item.put("status", avS(status.name()));
        item.put("unit_id", avS(unitId));
        item.put("assignee", avS(assignee));
        item.put("group_name", avS(group));
        item.put("transport_type", avS("interaction-api"));
        item.put("deadline_epoch_ms", avN(deadlineEpochMs));
        item.put("created_at_epoch_ms", avN(1_000L + deadlineEpochMs));
        item.put("updated_at_epoch_ms", avN(2_000L + deadlineEpochMs));
        item.put("ttl_epoch_s", avN(Long.MAX_VALUE));
        if (itemIndex != null) {
            item.put("item_index", avN(itemIndex));
        }
        return item;
    }

    private static PipelineOrchestratorConfig mockConfig() {
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        PipelineOrchestratorConfig.DynamoConfig dynamo = mock(PipelineOrchestratorConfig.DynamoConfig.class);
        when(config.dynamo()).thenReturn(dynamo);
        when(dynamo.awaitInteractionTable()).thenReturn("tpf_await_interaction");
        when(dynamo.awaitInteractionKeyTable()).thenReturn("tpf_await_interaction_key");
        when(dynamo.region()).thenReturn(Optional.empty());
        when(dynamo.endpointOverride()).thenReturn(Optional.empty());
        return config;
    }

    private static String scoped(String left, String right) {
        return left.length() + ":" + left + ":" + right.length() + ":" + right;
    }

    private static AttributeValue avS(String value) {
        return AttributeValue.builder().s(value).build();
    }

    private static AttributeValue avN(long value) {
        return AttributeValue.builder().n(Long.toString(value)).build();
    }
}
