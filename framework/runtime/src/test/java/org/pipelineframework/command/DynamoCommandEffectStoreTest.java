package org.pipelineframework.command;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.pipelineframework.execution.PipelineExecutionContext;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;

class DynamoCommandEffectStoreTest {

    @Test
    void surfacesConnectivityFailureAsStoreFailure() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        when(client.query(any(QueryRequest.class))).thenThrow(DynamoDbException.builder()
            .message("offline")
            .build());
        DynamoCommandEffectStore store = new DynamoCommandEffectStore(client, "effects");

        assertThrows(CommandEffectStoreException.class,
            () -> store.find("tenant", "command").await().atMost(Duration.ofSeconds(5)));
    }

    @Test
    void surfacesConditionalCreationRaceAsConflict() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        when(client.putItem(any(PutItemRequest.class))).thenThrow(
            ConditionalCheckFailedException.builder().message("lost race").build());
        DynamoCommandEffectStore store = new DynamoCommandEffectStore(client, "effects");

        assertThrows(CommandEffectConflictException.class,
            () -> store.createPending(request("attempt-1", "execution-1", new TestInput("small")), 1L)
                .await().atMost(Duration.ofSeconds(5)));
    }

    @Test
    void rejectsOversizedRecordBeforeCallingDynamo() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        DynamoCommandEffectStore store = new DynamoCommandEffectStore(client, "effects");
        TestInput input = new TestInput("x".repeat(DynamoCommandEffectStore.MAX_RECORD_BYTES + 1));

        assertThrows(CommandEffectStoreException.class,
            () -> store.createPending(request("attempt-1", "execution-1", input), 1L)
                .await().atMost(Duration.ofSeconds(5)));
        verifyNoInteractions(client);
    }

    static CommandRequest<TestInput> request(
        String attemptId,
        String executionId,
        TestInput input
    ) {
        CommandDescriptor descriptor = new CommandDescriptor(
            "WriteInvoice",
            "invoice.create",
            TestInput.class.getName(),
            TestOutput.class.getName(),
            "test.CommandIdGenerator",
            CommandDuplicatePolicy.RETURN_RECORDED,
            Map.of());
        return new CommandRequest<>(
            descriptor,
            "invoice-42",
            attemptId,
            input,
            new PipelineExecutionContext("tenant-a", executionId, 0),
            Map.of());
    }

    record TestInput(String value) {
    }

    record TestOutput(String value) {
    }
}
