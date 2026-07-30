package org.pipelineframework.orchestrator;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.inject.Instance;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.pipelineframework.cache.ProtobufMessageParser;
import org.pipelineframework.config.pipeline.PipelineJson;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

class DynamoExecutionStateStoreTest {

    @Test
    void writesAndRestoresTypedExecutionInputAndMaterializedChildResults() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        ExecutionDurablePayloadResolver payloads = mock(ExecutionDurablePayloadResolver.class);
        store.durablePayloadResolver = payloads;
        String typedInput = typed("PaymentRecord");
        String typedChildren = typed("List<PaymentOutput>");
        PaymentRecord input = new PaymentRecord("payment-1");
        List<PaymentOutput> outputs = List.of(new PaymentOutput("payment-1", "approved"));
        long now = System.currentTimeMillis();
        long ttl = now / 1000 + 3600;
        when(client.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().build());
        when(payloads.encode(any(), eq(ExecutionDurablePayloadResolver.Slot.INPUT), eq(input))).thenReturn(typedInput);

        store.createOrGetExecution(new ExecutionCreateCommand(
            "tenant-a", "key-typed", "org.example.pipeline", "sha256:contract", "sha256:release", input,
            ExecutionResultShape.SINGLE, now, ttl)).await().indefinitely();

        ArgumentCaptor<TransactWriteItemsRequest> created = ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
        verify(client).transactWriteItems(created.capture());
        assertEquals(typedInput, created.getValue().transactItems().getFirst().put().item().get("input_payload_json").s());

        Map<String, AttributeValue> inputRecord = new java.util.HashMap<>(executionItem(
            "tenant-a", "exec-typed", "key-typed", ttl, ExecutionStatus.RUNNING));
        inputRecord.put("input_payload_json", AttributeValue.builder().s(typedInput).build());
        inputRecord.put("input_payload_type_id", AttributeValue.builder().s("typed-durable").build());
        inputRecord.put("input_payload_encoding", AttributeValue.builder().s("application/tpf-canonical+json").build());
        when(client.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().item(inputRecord).build());
        when(payloads.decode(any(), eq(ExecutionDurablePayloadResolver.Slot.INPUT), eq(typedInput))).thenReturn(input);
        assertEquals(input, store.getExecution("tenant-a", "exec-typed").await().indefinitely().orElseThrow().inputPayload());

        Map<String, AttributeValue> persisted = new java.util.HashMap<>(executionItem(
            "tenant-a", "exec-typed", "key-typed", ttl, ExecutionStatus.RUNNING));
        persisted.put("current_step_index", AttributeValue.builder().n("1").build());
        Map<String, AttributeValue> completed = new java.util.HashMap<>(persisted);
        completed.put("status", AttributeValue.builder().s(ExecutionStatus.SUCCEEDED.name()).build());
        completed.put("result_payload_json", AttributeValue.builder().s(typedChildren).build());
        when(client.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().item(persisted).build());
        when(client.updateItem(any(UpdateItemRequest.class))).thenReturn(UpdateItemResponse.builder().attributes(completed).build());
        when(payloads.encode(any(), eq(ExecutionDurablePayloadResolver.Slot.RESULT), eq(outputs))).thenReturn(typedChildren);
        when(payloads.decode(any(), eq(ExecutionDurablePayloadResolver.Slot.RESULT), eq(typedChildren))).thenReturn(outputs);

        Optional<ExecutionRecord<Object, Object>> restored = store.markSucceeded(
            "tenant-a", "exec-typed", 0L, "typed-transition", outputs, now).await().indefinitely();

        assertEquals(outputs, restored.orElseThrow().resultPayload());
        assertInstanceOf(PaymentOutput.class, ((List<?>) restored.orElseThrow().resultPayload()).getFirst());
        ArgumentCaptor<UpdateItemRequest> update = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(client).updateItem(update.capture());
        assertEquals(typedChildren, update.getValue().expressionAttributeValues().get(":result").s());
        verify(payloads).decode(any(), eq(ExecutionDurablePayloadResolver.Slot.RESULT), eq(typedChildren));
    }

    @Test
    void writesAnAwaitItemChildInputUsingItsContinuationContract() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, mockConfig("tpf_execution", "tpf_execution_key"));
        ExecutionDurablePayloadResolver payloads = mock(ExecutionDurablePayloadResolver.class);
        store.durablePayloadResolver = payloads;
        PaymentStatus continuation = new PaymentStatus("payment-1", "approved");
        ExecutionInputSnapshot input = new ExecutionInputSnapshot(ExecutionInputShape.UNI, continuation);
        long now = System.currentTimeMillis();
        long ttl = now / 1000 + 3600;
        when(client.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().build());
        when(payloads.encode(any(), eq(PaymentStatus.class.getName()), eq(continuation)))
            .thenReturn(typed("PaymentStatus"));

        store.createOrGetExecution(new ExecutionCreateCommand(
            "tenant-a", "await-item-child", "org.example.pipeline", "sha256:contract", "sha256:release", input,
            ExecutionResultShape.MATERIALIZED_MULTI, Optional.of(PaymentStatus.class.getName()), 3, now, ttl)).await().indefinitely();

        ArgumentCaptor<TransactWriteItemsRequest> created = ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
        verify(client).transactWriteItems(created.capture());
        Map<String, AttributeValue> item = created.getValue().transactItems().getFirst().put().item();
        assertEquals("3", item.get("current_step_index").n());
        assertEquals(typed("PaymentStatus"), item.get("input_payload_json").s());
        verify(payloads).encode(any(), eq(PaymentStatus.class.getName()), eq(continuation));
        verify(payloads, never()).encode(any(), eq(ExecutionDurablePayloadResolver.Slot.INPUT), eq(continuation));
    }

    @Test
    void legacyInputWriteDoesNotClaimTypedDurableMetadata() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, mockConfig("tpf_execution", "tpf_execution_key"));
        long now = System.currentTimeMillis();
        when(client.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().build());

        store.createOrGetExecution(new ExecutionCreateCommand(
            "tenant-a", "legacy-key", new PaymentRecord("payment-1"), ExecutionResultShape.SINGLE,
            now, now / 1000 + 3600)).await().indefinitely();

        ArgumentCaptor<TransactWriteItemsRequest> created = ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
        verify(client).transactWriteItems(created.capture());
        Map<String, AttributeValue> item = created.getValue().transactItems().getFirst().put().item();
        assertFalse(item.containsKey("input_payload_type_id"));
        assertFalse(item.containsKey("input_payload_encoding"));
    }

    @Test
    void rejectsExternalTypedPayloadWhenItsEnvelopeDigestDoesNotMatch() throws Exception {
        DynamoDbClient client = mock(DynamoDbClient.class);
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, mockConfig("tpf_execution", "tpf_execution_key"));
        store.durablePayloadResolver = mock(ExecutionDurablePayloadResolver.class);
        long ttl = System.currentTimeMillis() / 1000 + 3600;
        String payload = typed("PaymentOutput");
        Map<String, AttributeValue> execution = new java.util.HashMap<>(executionItem(
            "tenant-a", "exec-external", "key-external", ttl, ExecutionStatus.SUCCEEDED));
        execution.put("result_payload_reference", AttributeValue.builder().s("payload-1").build());
        execution.put("result_payload_digest", AttributeValue.builder().s("deliberately-wrong").build());
        byte[] bytes = payload.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        String hash = java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(bytes));
        Map<String, AttributeValue> manifest = Map.of(
            "payload_id", AttributeValue.builder().s("payload-1").build(),
            "payload_part", AttributeValue.builder().s("MANIFEST").build(),
            "payload_length_bytes", AttributeValue.builder().n(Integer.toString(bytes.length)).build(),
            "chunk_count", AttributeValue.builder().n("1").build(),
            "sha256", AttributeValue.builder().s(hash).build());
        Map<String, AttributeValue> chunk = Map.of(
            "payload_id", AttributeValue.builder().s("payload-1").build(),
            "payload_part", AttributeValue.builder().s("CHUNK#00000000").build(),
            "payload_bytes", AttributeValue.builder().b(software.amazon.awssdk.core.SdkBytes.fromByteArray(bytes)).build());
        when(client.getItem(any(GetItemRequest.class))).thenReturn(
            GetItemResponse.builder().item(execution).build(), GetItemResponse.builder().item(manifest).build());
        when(client.query(any(software.amazon.awssdk.services.dynamodb.model.QueryRequest.class))).thenReturn(
            software.amazon.awssdk.services.dynamodb.model.QueryResponse.builder().items(chunk).build());

        IllegalStateException failure = assertThrows(IllegalStateException.class,
            () -> store.getExecution("tenant-a", "exec-external").await().indefinitely());

        assertTrue(failure.getMessage().contains("digest mismatch"));
        verify(store.durablePayloadResolver, never()).decode(any(), any(), any());
    }

    private static String typed(String canonicalTypeId) {
        return "{\"canonicalTypeId\":\"" + canonicalTypeId + "\",\"typeExpressionFingerprint\":\"fingerprint\","
            + "\"catalogFingerprint\":\"catalog\",\"encoding\":\"application/tpf-canonical+json\",\"encodingVersion\":1,\"payload\":\"e30=\"}";
    }

    private record PaymentRecord(String id) { }
    private record PaymentStatus(String id, String status) { }
    private record PaymentOutput(String id, String status) { }

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
    void startupValidationReportsMissingExecutionPayloadTable() {
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        when(config.dynamo().executionPayloadTable()).thenReturn("");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(null, config);

        var validationError = store.startupValidationError(config);

        assertTrue(validationError.isPresent());
        assertTrue(validationError.get().contains("execution-payload-table"));
    }

    @Test
    void startupValidationPassesWhenTablesConfigured() {
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(null, config);

        var validationError = store.startupValidationError(config);

        assertTrue(validationError.isEmpty());
    }

    @Test
    void batchExecutionKeyLookupBoundsOneThousandSiblingReads() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        List<String> siblingKeys = java.util.stream.IntStream.range(0, 1_000)
            .mapToObj(index -> "parent:await-item:unit-1:" + index)
            .toList();
        when(client.batchGetItem((BatchGetItemRequest) any())).thenReturn(BatchGetItemResponse.builder()
            .responses(Map.of())
            .unprocessedKeys(Map.of())
            .build());

        List<Optional<ExecutionRecord<Object, Object>>> resolved = store
            .getExecutionsByKey("tenant-a", siblingKeys)
            .await().indefinitely();

        assertEquals(1_000, resolved.size());
        assertTrue(resolved.stream().allMatch(Optional::isEmpty));
        verify(client, times(10)).batchGetItem((BatchGetItemRequest) any());
        verify(client, never()).getItem((GetItemRequest) any());
    }

    @Test
    void batchExecutionKeyLookupRetriesUnprocessedKeysAndPreservesRequestedOrder() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        List<String> siblingKeys = List.of("key-a", "key-b", "key-c");
        long ttl = System.currentTimeMillis() / 1000 + 3600;
        AtomicInteger calls = new AtomicInteger();
        when(client.batchGetItem(any(BatchGetItemRequest.class))).thenAnswer(invocation -> {
            BatchGetItemRequest request = invocation.getArgument(0, BatchGetItemRequest.class);
            int call = calls.getAndIncrement();
            if (request.requestItems().containsKey("tpf_execution_key")) {
                List<Map<String, AttributeValue>> keys = request.requestItems().get("tpf_execution_key").keys();
                if (call == 0) {
                    return BatchGetItemResponse.builder()
                        .responses(Map.of("tpf_execution_key", List.of(
                            executionKeyItem(keys.get(1), "exec-2"),
                            executionKeyItem(keys.get(0), "exec-1"))))
                        .unprocessedKeys(Map.of("tpf_execution_key", software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes.builder()
                            .keys(keys.get(2))
                            .consistentRead(true)
                            .build()))
                        .build();
                }
                return BatchGetItemResponse.builder()
                    .responses(Map.of("tpf_execution_key", List.of(executionKeyItem(keys.get(0), "exec-3"))))
                    .unprocessedKeys(Map.of())
                    .build();
            }
            return BatchGetItemResponse.builder()
                .responses(Map.of("tpf_execution", List.of(
                    executionItem("tenant-a", "exec-3", "key-c", ttl),
                    executionItem("tenant-a", "exec-1", "key-a", ttl),
                    executionItem("tenant-a", "exec-2", "key-b", ttl))))
                .unprocessedKeys(Map.of())
                .build();
        });

        List<Optional<ExecutionRecord<Object, Object>>> resolved = store
            .getExecutionsByKey("tenant-a", siblingKeys)
            .await().indefinitely();

        assertEquals(List.of("exec-1", "exec-2", "exec-3"), resolved.stream()
            .map(optional -> optional.orElseThrow().executionId())
            .toList());
        assertEquals(3, calls.get());
        verify(client, never()).getItem(any(GetItemRequest.class));
    }

    @Test
    void batchExecutionKeyLookupBoundsThrottledRetryDelay() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        when(client.batchGetItem(any(BatchGetItemRequest.class))).thenAnswer(invocation -> {
            BatchGetItemRequest request = invocation.getArgument(0, BatchGetItemRequest.class);
            return BatchGetItemResponse.builder().responses(Map.of()).unprocessedKeys(request.requestItems()).build();
        });

        assertTimeoutPreemptively(Duration.ofSeconds(2), () -> assertThrows(IllegalStateException.class,
            () -> store.getExecutionsByKey("tenant-a", List.of("key-a")).await().indefinitely()));

        verify(client, times(4)).batchGetItem(any(BatchGetItemRequest.class));
    }

    @Test
    void createOrGetReturnsDuplicateWhenExistingRecordFound() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        long now = System.currentTimeMillis();
        long ttl = now / 1000 + 3600;
        ExecutionCreateCommand command = new ExecutionCreateCommand(
            "tenant-a",
            "key-1",
            "org.example.pipeline",
            "sha256:contract",
            "sha256:release",
            "payload",
            ExecutionResultShape.SINGLE,
            now,
            ttl);

        when(client.getItem(any(GetItemRequest.class)))
            .thenReturn(GetItemResponse.builder().item(Map.of(
                "tenant_execution_key", AttributeValue.builder().s("8:tenant-a:5:key-1").build(),
                "execution_id", AttributeValue.builder().s("exec-1").build()))
                .build())
            .thenReturn(GetItemResponse.builder().item(executionItem("tenant-a", "exec-1", "key-1", ttl)).build());

        CreateExecutionResult result = store.createOrGetExecution(command).await().indefinitely();

        assertTrue(result.duplicate());
        assertEquals("exec-1", result.record().executionId());
        assertEquals("org.example.pipeline", result.record().pipelineId());
        assertEquals("sha256:contract", result.record().contractVersion());
        assertEquals("sha256:release", result.record().releaseVersion());
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
    void markSucceededPreservesCanonicalResultItemIdentity() throws Exception {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        long now = System.currentTimeMillis();
        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenReturn(UpdateItemResponse.builder().attributes(Map.of()).build());

        store.markSucceeded(
                "tenant-a",
                "exec-canonical-result",
                1L,
                "exec-canonical-result:0:0",
                List.of(new CanonicalPaymentOutput("payment-1", "APPROVED")),
                now)
            .await().indefinitely();

        ArgumentCaptor<UpdateItemRequest> request = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(client).updateItem(request.capture());
        String serializedResults = request.getValue().expressionAttributeValues().get(":result").s();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> resultItems = PipelineJson.mapper().readValue(serializedResults, List.class);

        assertEquals(1, resultItems.size());
        assertEquals(CanonicalPaymentOutput.class.getName(), resultItems.getFirst().get("payloadTypeId"));
        assertEquals("application/tpf-transition+json", resultItems.getFirst().get("payloadEncoding"));
    }

    @Test
    void materializedResultWritesAnImmutablePayloadBeforeReferencingIt() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        long now = System.currentTimeMillis();
        when(client.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().item(Map.of(
            "result_shape", AttributeValue.builder().s(ExecutionResultShape.MATERIALIZED_MULTI.name()).build(),
            "ttl_epoch_s", AttributeValue.builder().n(Long.toString(now / 1000 + 3600)).build())).build());
        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenReturn(UpdateItemResponse.builder().attributes(Map.of()).build());

        Optional<ExecutionRecord<Object, Object>> updated = store.markSucceeded(
                "tenant-a",
                "exec-1",
                1L,
                "exec-1:0:0",
                List.of("ok"),
                now)
            .await().indefinitely();

        assertTrue(updated.isEmpty());
        verify(client, times(2)).putItem(any(PutItemRequest.class));
        verify(client).updateItem(argThat((UpdateItemRequest request) ->
            request.updateExpression().contains("#resultReference = :resultReference")
                && request.updateExpression().contains("REMOVE #result")
                && request.expressionAttributeValues().containsKey(":resultReference")));
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
        when(dynamo.executionPayloadTable()).thenReturn("tpf_execution_payload");
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

    private static Map<String, AttributeValue> executionKeyItem(
        Map<String, AttributeValue> key,
        String executionId
    ) {
        return Map.of(
            "tenant_execution_key", key.get("tenant_execution_key"),
            "execution_id", AttributeValue.builder().s(executionId).build());
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
            Map.entry("pipeline_id", AttributeValue.builder().s("org.example.pipeline").build()),
            Map.entry("contract_version", AttributeValue.builder().s("sha256:contract").build()),
            Map.entry("release_version", AttributeValue.builder().s("sha256:release").build()),
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
        Map<String, AttributeValue> retryItem = new HashMap<>(executionItem(
            "tenant-a",
            "exec-1",
            "key-1",
            ttl,
            ExecutionStatus.WAIT_RETRY));
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
        verify(client).updateItem(argThat((UpdateItemRequest request) ->
            !request.updateExpression().contains("#resume")
                && !request.updateExpression().contains("#awaitInteraction")));
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
    void redriveTerminalExecutionUsesConditionalTerminalUpdate() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        long now = System.currentTimeMillis();
        long ttl = now / 1000 + 3600;
        Map<String, AttributeValue> queuedItem = new HashMap<>(executionItem(
            "tenant-a",
            "exec-1",
            "key-1",
            ttl,
            ExecutionStatus.QUEUED));
        queuedItem.put("attempt", AttributeValue.builder().n("3").build());

        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenReturn(UpdateItemResponse.builder().attributes(queuedItem).build());

        Optional<ExecutionRecord<Object, Object>> result = store.redriveTerminalExecution(
                "tenant-a",
                "exec-1",
                2L,
                true,
                "redrive",
                now)
            .await().indefinitely();

        assertTrue(result.isPresent());
        assertEquals(ExecutionStatus.QUEUED, result.get().status());
        assertEquals(3, result.get().attempt());
        verify(client).updateItem(argThat((UpdateItemRequest request) ->
            request.conditionExpression().contains("#version = :expected")
                && request.conditionExpression().contains("#status = :dlq OR #status = :failed")
                && request.updateExpression().contains("#attempt = #attempt + :one")
                && request.updateExpression().contains("REMOVE #result, #resultReference, #errorCode, #errorMessage, #leaseOwner")));
    }

    @Test
    void redriveTerminalExecutionReturnsEmptyOnConditionFailure() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config);
        when(client.updateItem(any(UpdateItemRequest.class)))
            .thenThrow(ConditionalCheckFailedException.builder().message("not redrivable").build());

        Optional<ExecutionRecord<Object, Object>> result = store.redriveTerminalExecution(
                "tenant-a",
                "exec-1",
                2L,
                false,
                "redrive",
                System.currentTimeMillis())
            .await().indefinitely();

        assertTrue(result.isEmpty());
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
        Map<String, AttributeValue> claimedItem = new HashMap<>(executionItem(
            "tenant-a",
            "exec-1",
            "key-1",
            ttl,
            ExecutionStatus.RUNNING));
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
    void markSucceededRejectsNullResultPayload() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, mockConfig("tpf_execution", "tpf_execution_key"));

        NullPointerException failure = assertThrows(NullPointerException.class, () -> store.markSucceeded(
                "tenant-a",
                "exec-1",
                1L,
                "exec-1:0:0",
                null,
                System.currentTimeMillis())
            .await().indefinitely());

        assertEquals("resultPayload must not be null", failure.getMessage());
        verifyNoInteractions(client);
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

    @Test
    void getExecutionDecodesSchemaNamePayload() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        @SuppressWarnings("unchecked")
        Instance<ProtobufMessageParser> parsers = mock(Instance.class);
        ProtobufMessageParser parser = mock(ProtobufMessageParser.class);
        DescriptorProtos.FileDescriptorSet payload = DescriptorProtos.FileDescriptorSet.newBuilder()
            .addFile(DescriptorProtos.FileDescriptorProto.newBuilder().setName("checkout.proto").build())
            .build();
        long ttl = System.currentTimeMillis() / 1000 + 3600;
        when(parsers.stream()).thenAnswer(invocation -> java.util.stream.Stream.of(parser));
        when(parser.type()).thenReturn(payload.getDescriptorForType().getFullName());
        when(parser.parseFrom(argThat(bytes -> Arrays.equals(bytes, payload.toByteArray())))).thenReturn(payload);
        when(client.getItem(any(GetItemRequest.class)))
            .thenReturn(GetItemResponse.builder()
                .item(executionItemWithInputPayload(
                    "tenant-a",
                    "exec-1",
                    "key-1",
                    ttl,
                    payload,
                    payload.getDescriptorForType().getFullName()))
                .build());
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config, parsers);

        Optional<ExecutionRecord<Object, Object>> result = store.getExecution("tenant-a", "exec-1")
            .await().indefinitely();

        assertTrue(result.isPresent());
        ExecutionInputSnapshot snapshot = assertInstanceOf(ExecutionInputSnapshot.class, result.get().inputPayload());
        assertEquals(ExecutionInputShape.UNI, snapshot.shape());
        assertEquals(payload, snapshot.payload());
    }

    @Test
    void toJsonStoresProtobufSchemaNameForNewPayloads() {
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(null, mockConfig("tpf_execution", "tpf_execution_key"));
        DescriptorProtos.FileDescriptorSet payload = samplePayload();

        String json = invokeToJson(store, payload);

        assertTrue(json.contains("\"_tpf_message\":\"" + payload.getDescriptorForType().getFullName() + "\""));
        assertTrue(json.contains("\"_tpf_java_class\":\"" + payload.getClass().getName() + "\""));
    }

    @Test
    void fromJsonRoundTripsNestedMapsListsAndIterablesWithProtobufPayloads() {
        @SuppressWarnings("unchecked")
        Instance<ProtobufMessageParser> parsers = mock(Instance.class);
        ProtobufMessageParser parser = mock(ProtobufMessageParser.class);
        DescriptorProtos.FileDescriptorSet payload = samplePayload();
        when(parsers.stream()).thenAnswer(invocation -> java.util.stream.Stream.of(parser));
        when(parser.type()).thenReturn(payload.getDescriptorForType().getFullName());
        when(parser.parseFrom(argThat(bytes -> Arrays.equals(bytes, payload.toByteArray())))).thenReturn(payload);
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(
            null,
            mockConfig("tpf_execution", "tpf_execution_key"),
            parsers);

        Map<String, Object> original = new HashMap<>();
        original.put("_tpf_type", "user-value");
        original.put("nested", Map.of("proto", payload));
        original.put("items", List.of(payload));
        original.put("iterable", Set.of(payload));

        String json = invokeToJson(store, original);
        Object decoded = invokeFromJson(store, json);

        assertTrue(decoded instanceof Map<?, ?>);
        Map<?, ?> decodedMap = (Map<?, ?>) decoded;
        assertEquals("user-value", decodedMap.get("_tpf_type"));
        assertEquals(payload, ((Map<?, ?>) decodedMap.get("nested")).get("proto"));
        assertEquals(List.of(payload), decodedMap.get("items"));
        // Iterables are persisted as JSON arrays, so Set inputs round-trip as Lists after deserialisation.
        assertEquals(List.of(payload), decodedMap.get("iterable"));
    }

    @Test
    void fromJsonReportsCorruptedBase64WithSchemaContext() {
        @SuppressWarnings("unchecked")
        Instance<ProtobufMessageParser> parsers = mock(Instance.class);
        ProtobufMessageParser parser = mock(ProtobufMessageParser.class);
        DescriptorProtos.FileDescriptorSet payload = samplePayload();
        when(parsers.stream()).thenAnswer(invocation -> java.util.stream.Stream.of(parser));
        when(parser.type()).thenReturn(payload.getDescriptorForType().getFullName());
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(
            null,
            mockConfig("tpf_execution", "tpf_execution_key"),
            parsers);

        IllegalStateException error = assertThrows(IllegalStateException.class, () ->
            invokeFromJson(store, wrappedEnvelopeJson(payload.getDescriptorForType().getFullName(), "%%%not-base64%%%")));

        assertTrue(error.getMessage().contains(payload.getDescriptorForType().getFullName()));
        assertTrue(error.getMessage().contains("_tpf_payload_b64"));
    }

    @Test
    void fromJsonReportsUnknownParserWithSchemaContext() {
        @SuppressWarnings("unchecked")
        Instance<ProtobufMessageParser> parsers = mock(Instance.class);
        when(parsers.stream()).thenAnswer(invocation -> java.util.stream.Stream.empty());
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(
            null,
            mockConfig("tpf_execution", "tpf_execution_key"),
            parsers);

        IllegalStateException error = assertThrows(IllegalStateException.class, () ->
            invokeFromJson(store, wrappedEnvelopeJson("checkout.v1.UnknownEvent", Base64.getEncoder().encodeToString(new byte[] {1}))));

        assertTrue(error.getMessage().contains("checkout.v1.UnknownEvent"));
        assertTrue(error.getMessage().contains("No protobuf parser registered"));
    }

    @Test
    void getExecutionDecodesProtobufPayloadReflectivelyWhenParserBeanMissing() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mockConfig("tpf_execution", "tpf_execution_key");
        Timestamp payload = Timestamp.newBuilder()
            .setSeconds(1234)
            .build();
        long ttl = System.currentTimeMillis() / 1000 + 3600;
        when(client.getItem(any(GetItemRequest.class)))
            .thenReturn(GetItemResponse.builder()
                .item(executionItemWithInputPayload(
                    "tenant-a",
                    "exec-2",
                    "key-2",
                    ttl,
                    payload,
                    payload.getDescriptorForType().getFullName()))
                .build());
        DynamoExecutionStateStore store = new DynamoExecutionStateStore(client, config, null);

        Optional<ExecutionRecord<Object, Object>> result = store.getExecution("tenant-a", "exec-2")
            .await().indefinitely();

        assertTrue(result.isPresent());
        ExecutionInputSnapshot snapshot = assertInstanceOf(ExecutionInputSnapshot.class, result.get().inputPayload());
        assertEquals(ExecutionInputShape.UNI, snapshot.shape());
        assertEquals(payload, snapshot.payload());
    }

    private static Map<String, AttributeValue> executionItemWithInputPayload(
        String tenantId,
        String executionId,
        String executionKey,
        long ttl,
        Message payload,
        String messageType
    ) {
        Map<String, AttributeValue> item = new HashMap<>(executionItem(tenantId, executionId, executionKey, ttl));
        item.put("input_shape", AttributeValue.builder().s(ExecutionInputShape.UNI.name()).build());
        item.put("input_payload_json", AttributeValue.builder().s(wrappedEnvelopeJson(
            messageType,
            payload.getClass().getName(),
            Base64.getEncoder().encodeToString(payload.toByteArray())))
            .build());
        return item;
    }

    private record CanonicalPaymentOutput(String paymentId, String status) {
    }

    private static DescriptorProtos.FileDescriptorSet samplePayload() {
        return DescriptorProtos.FileDescriptorSet.newBuilder()
            .addFile(DescriptorProtos.FileDescriptorProto.newBuilder().setName("checkout.proto").build())
            .build();
    }

    private static String wrappedEnvelopeJson(String messageType, String payload) {
        return wrappedEnvelopeJson(messageType, null, payload);
    }

    private static String wrappedEnvelopeJson(String messageType, String messageJavaClass, String payload) {
        try {
            Map<String, Object> envelope = new HashMap<>();
            envelope.put("_tpf_type", "protobuf");
            envelope.put("_tpf_message", messageType);
            if (messageJavaClass != null && !messageJavaClass.isBlank()) {
                envelope.put("_tpf_java_class", messageJavaClass);
            }
            envelope.put("_tpf_payload_b64", payload);
            return PipelineJson.mapper().writeValueAsString(Map.of(
                "_tpf_internal", envelope));
        } catch (Exception e) {
            throw new IllegalStateException("Failed creating protobuf envelope JSON for test.", e);
        }
    }

    private static String invokeToJson(DynamoExecutionStateStore store, Object value) {
        return (String) invoke(store, "toJson", new Class<?>[] {Object.class}, value);
    }

    private static Object invokeFromJson(DynamoExecutionStateStore store, String value) {
        return invoke(store, "fromJson", new Class<?>[] {String.class}, value);
    }

    private static Object invoke(
        DynamoExecutionStateStore store,
        String methodName,
        Class<?>[] parameterTypes,
        Object argument
    ) {
        try {
            Method method = DynamoExecutionStateStore.class.getDeclaredMethod(methodName, parameterTypes);
            method.setAccessible(true);
            return method.invoke(store, argument);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new IllegalStateException("Failed invoking " + methodName + " for test.", cause);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed invoking " + methodName + " for test.", e);
        }
    }
}
