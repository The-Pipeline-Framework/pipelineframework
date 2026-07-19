package org.pipelineframework.awaitable.admission;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
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
        when(client.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().item(Map.of()).build());

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
        when(client.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().item(Map.of(
            "owner_key", text(owner.key()),
            "lease_token", text("lease-1"),
            "expires_at", number(10))).build());

        AwaitAdmissionAcquireResult result = new DynamoAwaitAdmissionStore(client, config)
            .acquire(scope, owner, 2, 9_000, 1_000).toCompletableFuture().join();

        assertTrue(result.acquired());
        assertTrue(result.reused());
        assertEquals("lease-1", result.reservation().orElseThrow().leaseToken());
    }

    @Test
    void reportsUnavailableWhenEverySlotBelongsToAnotherOwner() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        when(client.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().item(Map.of(
            "owner_key", text("other-owner"),
            "lease_token", text("other-lease"),
            "expires_at", number(10))).build());
        AwaitAdmissionScope scope = new AwaitAdmissionScope("payments", "await-provider", "kafka://requests");

        AwaitAdmissionAcquireResult result = new DynamoAwaitAdmissionStore(client, admissionConfig())
            .acquire(scope, new AwaitAdmissionOwner("tenant:unit:item"), 2, 9_000, 1_000)
            .toCompletableFuture().join();

        assertFalse(result.acquired());
    }

    @Test
    void retriesFromTheFirstSlotAfterAConcurrentClaimAndUsesTheNextFreeSlot() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        AwaitAdmissionOwner owner = new AwaitAdmissionOwner("tenant:unit:item");
        when(client.getItem(any(GetItemRequest.class))).thenReturn(
            GetItemResponse.builder().item(Map.of()).build(),
            GetItemResponse.builder().item(Map.of(
                "owner_key", text("other-owner"),
                "lease_token", text("other-lease"),
                "expires_at", number(10))).build(),
            GetItemResponse.builder().item(Map.of()).build());
        when(client.putItem(any(PutItemRequest.class)))
            .thenThrow(ConditionalCheckFailedException.builder().message("raced").build())
            .thenReturn(null);
        AwaitAdmissionScope scope = new AwaitAdmissionScope("payments", "await-provider", "kafka://requests");

        AwaitAdmissionReservation reservation = new DynamoAwaitAdmissionStore(client, admissionConfig())
            .acquire(scope, owner, 2, 9_000, 1_000).toCompletableFuture().join().reservation().orElseThrow();

        int firstSlot = Math.floorMod(owner.key().hashCode(), 2);
        assertEquals(Math.floorMod(firstSlot + 1, 2), reservation.slot());
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
