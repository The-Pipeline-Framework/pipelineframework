package org.pipelineframework.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.google.protobuf.StringValue;
import org.junit.jupiter.api.Test;
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
                1,
                "execution-1",
                CommandEffectStatus.SUCCEEDED,
                null,
                null,
                Optional.of(outcome),
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
                "attempt-1", 1, "execution-1", CommandEffectStatus.SUCCEEDED,
                null, null, Optional.empty(), 1L, 2L)),
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
            .replace("\"schemaVersion\":1", "\"schemaVersion\":99");

        CommandEffectStoreException failure = assertThrows(
            CommandEffectStoreException.class, () -> codec.decode(encoded));

        assertEquals("Unsupported durable Command effect schema version 99", failure.getMessage());
    }

    @Test
    void rejectsRuntimeValueIncompatibleWithDeclaredType() {
        CommandEffectRecord record = pending(new TestInput("invoice-42", 7));

        assertThrows(CommandEffectStoreException.class,
            () -> codec.encode(record, String.class.getName(), TestOutput.class.getName()));
    }

    private static CommandEffectRecord pending(Object input) {
        return new CommandEffectRecord(
            "tenant-a", "execution-1", "Write", "write", "command-1",
            CommandEffectStatus.PENDING, input, null, null, null,
            Optional.empty(),
            List.of(new CommandEffectAttemptRecord(
                "attempt-1", 1, "execution-1", CommandEffectStatus.PENDING,
                null, null, Optional.empty(), 1L, 1L)),
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
