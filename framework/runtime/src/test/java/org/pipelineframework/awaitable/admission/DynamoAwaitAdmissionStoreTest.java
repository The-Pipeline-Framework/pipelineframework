package org.pipelineframework.awaitable.admission;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
        assertFalse(store.release(scope, owner, 2).toCompletableFuture().join());
    }
}
