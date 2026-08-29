package org.pipelineframework.query;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletionException;

import org.junit.jupiter.api.Test;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

class DynamoQueryCaptureStoreTest {
    @Test
    void surfacesConnectivityFailureAsTypedStoreFailure() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        when(client.query(any(QueryRequest.class))).thenThrow(
            DynamoDbException.builder().message("offline").build());
        DynamoQueryCaptureStore store = new DynamoQueryCaptureStore(client, "captures");

        CompletionException failure = assertThrows(CompletionException.class,
            () -> store.get("capture-key").toCompletableFuture().join());
        assertInstanceOf(QueryCaptureStoreException.class, failure.getCause());
    }

    @Test
    void rejectsOversizedCaptureBeforeWritingIt() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        when(client.query(any(QueryRequest.class))).thenReturn(
            QueryResponse.builder().items(List.of()).build());
        DynamoQueryCaptureStore store = new DynamoQueryCaptureStore(client, "captures");
        QueryCaptureRecord record = new QueryCaptureRecord(
            "tenant", "execution", 1, "large.find", "v1", "capture-key", "input",
            "{\"value\":\"" + "x".repeat(DynamoQueryCaptureStore.MAX_EVENT_BYTES) + "\"}",
            TestOutput.class.getName(), Instant.now(), QueryCaptureStatus.FOUND, "found");

        CompletionException failure = assertThrows(CompletionException.class,
            () -> store.putIfAbsent(record).toCompletableFuture().join());
        assertInstanceOf(QueryCaptureStoreException.class, failure.getCause());
        verify(client).query(any(QueryRequest.class));
        verify(client, never()).putItem(any(PutItemRequest.class));
    }

    @Test
    void invalidConfigurationDoesNotTouchDynamo() {
        DynamoDbClient client = mock(DynamoDbClient.class);

        assertThrows(IllegalArgumentException.class, () -> new DynamoQueryCaptureStore(client, " "));
        verifyNoInteractions(client);
    }

    record TestOutput(String value) {
    }
}
