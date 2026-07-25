package org.pipelineframework.awaitable.admission;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DynamoAwaitAdmissionStoreTest {

    @Test
    void claimsAndReleasesAFixedSlotWithConditionalWrites() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        PipelineOrchestratorConfig.DynamoConfig dynamo = mock(PipelineOrchestratorConfig.DynamoConfig.class);
        when(config.dynamo()).thenReturn(dynamo);
        when(dynamo.awaitAdmissionTable()).thenReturn("tpf_await_admission");
        when(client.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder().items(Map.of()).build());

        DynamoAwaitAdmissionStore store = new DynamoAwaitAdmissionStore(client, config);
        AwaitAdmissionScope scope = new AwaitAdmissionScope("payments", "await-provider", "kafka://requests");
        AwaitAdmissionOwner owner = new AwaitAdmissionOwner("unit:item");

        AwaitAdmissionReservation reservation = store.acquire(scope, owner, 2, 10_000, 1_000)
            .toCompletableFuture().join().reservation().orElseThrow();
        assertTrue(store.release(reservation).toCompletableFuture().join());

        verify(client).putItem(argThat((PutItemRequest request) -> request.conditionExpression().contains("attribute_not_exists")
            && request.item().get("scope_key").s().equals(scope.key())));
        verify(client).deleteItem(argThat((DeleteItemRequest request) -> request.conditionExpression().contains("#owner")));
        verify(client).deleteItem(argThat((DeleteItemRequest request) -> request.conditionExpression().contains("#lease")
            && request.expressionAttributeValues().get(":lease").s().equals(reservation.leaseToken())));
    }

    @Test
    void reusesAnExistingOwnerReservation() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineOrchestratorConfig config = admissionConfig();
        AwaitAdmissionScope scope = new AwaitAdmissionScope("payments", "await-provider", "kafka://requests");
        AwaitAdmissionOwner owner = new AwaitAdmissionOwner("tenant:unit:item");
        when(client.query(any(QueryRequest.class))).thenReturn(
            QueryResponse.builder().items(Map.of(
                "owner_key", text("other-owner"),
                "lease_token", text("other-lease"),
                "expires_at", number(10),
                "slot", number(0)))
                .lastEvaluatedKey(Map.of("scope_key", text(scope.key()), "slot", number(0)))
                .build(),
            QueryResponse.builder().items(Map.of(
                "owner_key", text(owner.key()),
                "lease_token", text("lease-1"),
                "expires_at", number(10),
                "slot", number(Math.floorMod(owner.key().hashCode(), 2)))).build());

        AwaitAdmissionAcquireResult result = new DynamoAwaitAdmissionStore(client, config)
            .acquire(scope, owner, 2, 9_000, 1_000).toCompletableFuture().join();

        assertTrue(result.acquired());
        assertTrue(result.reused());
        assertEquals("lease-1", result.reservation().orElseThrow().leaseToken());
        verify(client, times(2)).query(any(QueryRequest.class));
        verify(client).query(argThat((QueryRequest request) -> !request.exclusiveStartKey().isEmpty()));
    }

    @Test
    void reportsUnavailableWhenEverySlotBelongsToAnotherOwner() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        when(client.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder().items(
            Map.of(
                "owner_key", text("other-owner"),
                "lease_token", text("other-lease"),
                "expires_at", number(10),
                "slot", number(0)),
            Map.of(
                "owner_key", text("another-owner"),
                "lease_token", text("another-lease"),
                "expires_at", number(10),
                "slot", number(1)))
            .build());
        AwaitAdmissionScope scope = new AwaitAdmissionScope("payments", "await-provider", "kafka://requests");

        AwaitAdmissionAcquireResult result = new DynamoAwaitAdmissionStore(client, admissionConfig())
            .acquire(scope, new AwaitAdmissionOwner("tenant:unit:item"), 2, 9_000, 1_000)
            .toCompletableFuture().join();

        assertFalse(result.acquired());
        verify(client, times(1)).query(any(QueryRequest.class));
    }

    @Test
    void retriesFromTheFirstSlotAfterAConcurrentClaimAndUsesTheNextFreeSlot() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        AwaitAdmissionOwner owner = new AwaitAdmissionOwner("tenant:unit:item");
        int firstSlot = Math.floorMod(owner.key().hashCode(), 2);
        int nextSlot = Math.floorMod(firstSlot + 1, 2);
        when(client.query(any(QueryRequest.class))).thenReturn(
            QueryResponse.builder().items().build(),
            QueryResponse.builder().items(Map.of(
                "owner_key", text("other-owner"),
                "lease_token", text("other-lease"),
                "expires_at", number(10),
                "slot", number(firstSlot))).build());
        when(client.putItem(any(PutItemRequest.class)))
            .thenThrow(ConditionalCheckFailedException.builder().message("raced").build())
            .thenReturn(null);
        AwaitAdmissionScope scope = new AwaitAdmissionScope("payments", "await-provider", "kafka://requests");

        AwaitAdmissionReservation reservation = new DynamoAwaitAdmissionStore(client, admissionConfig())
            .acquire(scope, owner, 2, 9_000, 1_000).toCompletableFuture().join().reservation().orElseThrow();

        assertEquals(nextSlot, reservation.slot());
    }

    private static PipelineOrchestratorConfig admissionConfig() {
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        PipelineOrchestratorConfig.DynamoConfig dynamo = mock(PipelineOrchestratorConfig.DynamoConfig.class);
        when(config.dynamo()).thenReturn(dynamo);
        when(dynamo.awaitAdmissionTable()).thenReturn("tpf_await_admission");
        return config;
    }

    private static AttributeValue text(String value) {
        return AttributeValue.builder().s(value).build();
    }

    private static AttributeValue number(long value) {
        return AttributeValue.builder().n(Long.toString(value)).build();
    }
}
