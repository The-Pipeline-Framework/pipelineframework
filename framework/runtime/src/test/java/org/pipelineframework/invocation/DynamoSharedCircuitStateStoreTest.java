package org.pipelineframework.invocation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.pipelineframework.config.PipelineResilienceConfig;
import org.pipelineframework.runtime.core.resilience.CircuitIdentity;
import org.pipelineframework.runtime.core.resilience.CircuitPolicy;
import org.pipelineframework.runtime.core.resilience.CircuitScope;
import org.pipelineframework.runtime.core.resilience.SharedCircuitStatus;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;

class DynamoSharedCircuitStateStoreTest {
    private static final CircuitPolicy POLICY = new CircuitPolicy(
        CircuitScope.SHARED_DEPENDENCY, 2, Duration.ofMinutes(1), Duration.ofSeconds(30), 1,
        Duration.ofSeconds(1), Duration.ofSeconds(30));

    @Test
    void firstClosedStateWriteUsesOnlyTheInitialConditionPlaceholders() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        when(client.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().item(Map.of()).build());
        when(client.putItem(any(PutItemRequest.class))).thenReturn(PutItemResponse.builder().build());
        DynamoSharedCircuitStateStore store = new DynamoSharedCircuitStateStore(
            client, sharedConfig(), Clock.fixed(Instant.parse("2026-07-21T20:00:00Z"), ZoneOffset.UTC));

        assertEquals(SharedCircuitStatus.CLOSED, store.recordHealthFailures(
            new CircuitIdentity("pricing"), POLICY, 1, Instant.parse("2026-07-21T20:00:00Z"))
            .toCompletableFuture().join().status());

        ArgumentCaptor<PutItemRequest> request = ArgumentCaptor.forClass(PutItemRequest.class);
        verify(client).putItem(request.capture());
        assertEquals("attribute_not_exists(#pk)", request.getValue().conditionExpression());
        assertEquals(Map.of("#pk", "PK"), request.getValue().expressionAttributeNames());
        assertTrue(request.getValue().expressionAttributeValues().isEmpty());
    }

    private static PipelineResilienceConfig.SharedConfig sharedConfig() {
        return new PipelineResilienceConfig.SharedConfig() {
            @Override
            public Optional<String> dynamoTable() {
                return Optional.of("shared-circuits");
            }

            @Override
            public Duration maxStateStaleness() {
                return Duration.ofSeconds(1);
            }

            @Override
            public Duration backendRetryDelay() {
                return Duration.ofSeconds(1);
            }

            @Override
            public Optional<String> region() {
                return Optional.empty();
            }

            @Override
            public Optional<String> endpointOverride() {
                return Optional.empty();
            }
        };
    }
}
