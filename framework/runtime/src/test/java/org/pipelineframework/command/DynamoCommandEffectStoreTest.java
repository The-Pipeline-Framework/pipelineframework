package org.pipelineframework.command;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Map;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.pipelineframework.execution.PipelineExecutionContext;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

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

    @Test
    void reissueAppendsOneConditionalImmutableRevision() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        CommandEffectRecordCodec codec = new CommandEffectRecordCodec();
        DynamoCommandEffectStore store = new DynamoCommandEffectStore(client, "effects", codec);
        CommandRequest<TestInput> initial = request("attempt-1", "execution-1", new TestInput("small"));
        CommandEffectRecord succeeded = new CommandEffectRecord(
            "tenant-a", "execution-1", initial.descriptor().stepId(), initial.descriptor().command(),
            initial.commandId(), CommandEffectStatus.PENDING, initial.input(), null, null, null,
            Optional.empty(),
            List.of(new CommandEffectAttemptRecord(
                initial.attemptId(), initial.occurrenceId(), 1, "execution-1", CommandAttemptPurpose.INITIAL,
                CommandEffectStatus.PENDING, Optional.empty(), null, null, Optional.empty(),
                Optional.empty(), 1L, 1L)),
            1L, 1L)
            .dispatching(initial.attemptId(), 2L)
            .succeeded(initial.attemptId(), new TestOutput("created"), 3L);
        String encoded = codec.encode(
            succeeded, initial.descriptor().inputType(), initial.descriptor().outputType());
        when(client.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder().items(Map.of(
            DynamoCommandEffectStore.COMMAND_KEY, AttributeValue.builder().s("key").build(),
            DynamoCommandEffectStore.REVISION, AttributeValue.builder().n("4").build(),
            DynamoCommandEffectStore.SCHEMA_VERSION, AttributeValue.builder().n("2").build(),
            DynamoCommandEffectStore.RECORD_JSON, AttributeValue.builder().s(encoded).build())).build());
        when(client.putItem(any(PutItemRequest.class))).thenReturn(PutItemResponse.builder().build());
        CommandRequest<TestInput> reissue = new CommandRequest<>(
            initial.descriptor(), initial.commandId(), "occurrence-2", "attempt-2", initial.input(),
            new PipelineExecutionContext("tenant-a", "execution-2", 0), Map.of());

        CommandEffectRecord admitted = store.createAttempt(
                reissue, CommandAttemptAdmission.reissue("approved"), 4L)
            .await().atMost(Duration.ofSeconds(5));

        assertEquals(CommandAttemptPurpose.REISSUE, admitted.currentAttempt().purpose());
        assertEquals("occurrence-2", admitted.currentAttempt().occurrenceId());
        verify(client).putItem(argThat((PutItemRequest put) ->
            "5".equals(put.item().get(DynamoCommandEffectStore.REVISION).n())
                && put.conditionExpression().contains("attribute_not_exists")
                && "2".equals(put.item().get(DynamoCommandEffectStore.SCHEMA_VERSION).n())));
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
