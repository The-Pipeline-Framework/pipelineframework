package org.pipelineframework.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.StringValue;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.connector.CommandMachineConfirmation;
import org.pipelineframework.connector.CommandReference;
import org.pipelineframework.connector.CommandReferencePurpose;
import org.pipelineframework.connector.ConnectionRef;
import org.pipelineframework.connector.ConnectorConfigurationSnapshot;
import org.pipelineframework.connector.ConnectorOperationIdentity;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.ConnectorProviderId;

class CommandEffectRecordCodecTest {
    private final CommandEffectRecordCodec codec = new CommandEffectRecordCodec();

    @Test
    void roundTripsTypedRecordAndNativeOutcomeWithoutSchemaLoss() {
        CommandOutcomeSnapshot outcome = outcome(CommandEffectStatus.SUCCEEDED, "created");
        CommandEffectRecord record = new CommandEffectRecord(
            "tenant-a",
            "execution-1",
            "WriteInvoice",
            "invoice.create",
            "invoice-42",
            CommandEffectStatus.SUCCEEDED,
            new TestInput("invoice-42", 7),
            new TestOutput("provider-9", true),
            null,
            null,
            Optional.of(outcome),
            List.of(new CommandEffectAttemptRecord(
                "attempt-1",
                "invoice-42",
                1,
                "execution-1",
                CommandAttemptPurpose.INITIAL,
                CommandEffectStatus.SUCCEEDED,
                Optional.of(new TestOutput("provider-9", true)),
                null,
                null,
                Optional.of(outcome),
                Optional.empty(),
                10L,
                20L)),
            10L,
            20L);

        String encoded = codec.encode(record, TestInput.class.getName(), TestOutput.class.getName());
        CommandEffectRecordCodec.DecodedSnapshot decoded = codec.decode(encoded);

        assertEquals(record, decoded.record());
        assertInstanceOf(TestInput.class, decoded.record().input());
        assertInstanceOf(TestOutput.class, decoded.record().output());
        assertEquals(outcome, decoded.record().outcome().orElseThrow());
    }

    @Test
    void roundTripsProtobufValuesThroughGeneratedBuilder() {
        StringValue input = StringValue.of("input");
        StringValue output = StringValue.of("output");
        CommandEffectRecord record = new CommandEffectRecord(
            "tenant-a", "execution-1", "Write", "write", "command-1",
            CommandEffectStatus.SUCCEEDED, input, output, null, null,
            Optional.empty(),
            List.of(new CommandEffectAttemptRecord(
                "attempt-1", "command-1", 1, "execution-1", CommandAttemptPurpose.INITIAL,
                CommandEffectStatus.SUCCEEDED, Optional.of(output), null, null, Optional.empty(),
                Optional.empty(), 1L, 2L)),
            1L, 2L);

        CommandEffectRecord decoded = codec.decode(codec.encode(
            record, StringValue.class.getName(), StringValue.class.getName())).record();

        assertEquals(input, decoded.input());
        assertEquals(output, decoded.output());
    }

    @Test
    void rejectsUnknownSchemaVersion() {
        CommandEffectRecord record = pending(new TestInput("invoice-42", 7));
        String encoded = codec.encode(record, TestInput.class.getName(), TestOutput.class.getName())
            .replace("\"schemaVersion\":2", "\"schemaVersion\":99");

        CommandEffectStoreException failure = assertThrows(
            CommandEffectStoreException.class, () -> codec.decode(encoded));

        assertEquals("Unsupported durable Command effect schema version 99", failure.getMessage());
    }

    @Test
    void decodesV1ByInferringOriginalOccurrencePurposesAndCurrentOutput() throws Exception {
        TestOutput output = new TestOutput("provider-9", true);
        CommandEffectRecord record = new CommandEffectRecord(
            "tenant-a", "execution-2", "Write", "write", "command-1",
            CommandEffectStatus.SUCCEEDED, new TestInput("invoice-42", 7), output, null, null,
            Optional.empty(),
            List.of(
                new CommandEffectAttemptRecord(
                    "attempt-1", "command-1", 1, "execution-1", CommandAttemptPurpose.INITIAL,
                    CommandEffectStatus.FAILED_RETRYABLE, Optional.empty(), IllegalStateException.class.getName(),
                    "temporary", Optional.empty(), Optional.empty(), 1L, 2L),
                new CommandEffectAttemptRecord(
                    "attempt-2", "command-1", 2, "execution-2", CommandAttemptPurpose.RETRY,
                    CommandEffectStatus.SUCCEEDED, Optional.of(output), null, null, Optional.empty(),
                    Optional.empty(), 3L, 4L)),
            1L, 4L);
        ObjectNode root = (ObjectNode) PipelineJson.mapper().readTree(
            codec.encode(record, TestInput.class.getName(), TestOutput.class.getName()));
        root.put("schemaVersion", 1);
        for (JsonNode attempt : root.withArray("attempts")) {
            ObjectNode object = (ObjectNode) attempt;
            object.remove(List.of("occurrenceId", "purpose", "output", "reason"));
        }

        CommandEffectRecord decoded = codec.decode(PipelineJson.mapper().writeValueAsString(root)).record();

        assertEquals(List.of(CommandAttemptPurpose.INITIAL, CommandAttemptPurpose.RETRY),
            decoded.attempts().stream().map(CommandEffectAttemptRecord::purpose).toList());
        assertEquals(List.of("command-1", "command-1"),
            decoded.attempts().stream().map(CommandEffectAttemptRecord::occurrenceId).toList());
        assertTrue(decoded.attempts().get(0).output().isEmpty());
        assertEquals(output, decoded.attempts().get(1).output().orElseThrow());
        assertEquals(output, decoded.output());
    }

    @Test
    void rejectsRuntimeValueIncompatibleWithDeclaredType() {
        CommandEffectRecord record = pending(new TestInput("invoice-42", 7));

        assertThrows(CommandEffectStoreException.class,
            () -> codec.encode(record, String.class.getName(), TestOutput.class.getName()));
    }

    @Test
    void rejectsUnknownStoredProtobufFields() {
        FileDescriptorProto input = FileDescriptorProto.newBuilder().setName("input.proto").build();
        CommandEffectRecord record = pending(input)
            .dispatching("attempt-1", 2L)
            .succeeded("attempt-1", input, 3L);
        String encoded = codec.encode(
            record, FileDescriptorProto.class.getName(), FileDescriptorProto.class.getName())
            .replaceFirst(
                "\\\"name\\\":\\\"input.proto\\\"",
                "\\\"name\\\":\\\"input.proto\\\",\\\"unknownStoredField\\\":\\\"unexpected\\\"");

        assertThrows(CommandEffectStoreException.class, () -> codec.decode(encoded));
    }

    private static CommandEffectRecord pending(Object input) {
        return new CommandEffectRecord(
            "tenant-a", "execution-1", "Write", "write", "command-1",
            CommandEffectStatus.PENDING, input, null, null, null,
            Optional.empty(),
            List.of(new CommandEffectAttemptRecord(
                "attempt-1", "command-1", 1, "execution-1", CommandAttemptPurpose.INITIAL,
                CommandEffectStatus.PENDING, Optional.empty(), null, null, Optional.empty(),
                Optional.empty(), 1L, 1L)),
            1L, 1L);
    }

    private static CommandOutcomeSnapshot outcome(CommandEffectStatus status, String code) {
        return new CommandOutcomeSnapshot(
            new ConnectorOperationIdentity(
                ConnectorProviderId.of("test.provider"),
                "invoice.create",
                ConnectorOperationKind.COMMAND,
                1),
            1,
            new ConnectorConfigurationSnapshot(
                "test.config", 1, "digest", List.of(new ConnectionRef("invoice-primary"))),
            status,
            code,
            Set.of("durable"),
            CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED,
            false,
            List.of(new CommandReference(
                "invoice", "provider-9", CommandReferencePurpose.RECONCILIATION)));
    }

    record TestInput(String id, int quantity) {
    }

    record TestOutput(String providerId, boolean accepted) {
    }
}
