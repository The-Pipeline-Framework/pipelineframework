package org.pipelineframework.orchestrator;

import java.util.List;
import java.util.Map;
import java.nio.file.Path;
import java.util.Set;

import com.google.protobuf.Int32Value;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TransitionCommandEnvelopeTest {

    private final JsonTransitionPayloadCodec payloadCodec = new JsonTransitionPayloadCodec();

    @Test
    void preservesTypedUniInputPayloadAcrossEnvelopeRoundTrip() {
        SamplePayload payload = new SamplePayload("order-1", 42);
        TransitionWorkerCommand command = command(new ExecutionInputSnapshot(ExecutionInputShape.UNI, payload));

        TransitionCommandEnvelope envelope = TransitionCommandEnvelope.from(
            command,
            "pipeline-1",
            "contract-1",
            "release-1",
            "trace-1",
            payloadCodec.encode(command.inputPayload()));
        TransitionWorkerCommand decoded = envelope.toCommand(payloadCodec);

        assertEquals("tenant-1", envelope.tenantId());
        assertEquals("exec-1", envelope.executionId());
        assertEquals("pipeline-1", envelope.pipelineId());
        assertEquals("contract-1", envelope.contractVersion());
        assertEquals("release-1", envelope.releaseVersion());
        assertEquals("trace-1", envelope.traceId());
        ExecutionInputSnapshot snapshot = assertInstanceOf(ExecutionInputSnapshot.class, decoded.inputPayload());
        SamplePayload decodedPayload = assertInstanceOf(SamplePayload.class, snapshot.payload());
        assertEquals("order-1", decodedPayload.id());
        assertEquals(42, decodedPayload.amount());
    }

    @Test
    void preservesTypedMultiInputPayloadAcrossEnvelopeRoundTrip() {
        TransitionWorkerCommand command = command(new ExecutionInputSnapshot(
            ExecutionInputShape.MULTI,
            List.of(new SamplePayload("a", 1), new SamplePayload("b", 2))));

        TransitionCommandEnvelope envelope = TransitionCommandEnvelope.from(
            command,
            "pipeline-1",
            "bundle-1",
            "trace-1",
            payloadCodec.encode(command.inputPayload()));
        TransitionWorkerCommand decoded = envelope.toCommand(payloadCodec);

        ExecutionInputSnapshot snapshot = assertInstanceOf(ExecutionInputSnapshot.class, decoded.inputPayload());
        List<?> decodedItems = assertInstanceOf(List.class, snapshot.payload());
        SamplePayload first = assertInstanceOf(SamplePayload.class, decodedItems.getFirst());
        assertEquals("a", first.id());
        assertEquals(1, first.amount());
    }

    @Test
    void preservesPathPayloadFieldsAcrossEnvelopeRoundTrip() {
        TransitionWorkerCommand command = command(new ExecutionInputSnapshot(
            ExecutionInputShape.UNI,
            new PathPayload(Path.of("/tmp/tpf/input.csv"))));

        TransitionCommandEnvelope envelope = TransitionCommandEnvelope.from(
            command,
            "pipeline-1",
            "bundle-1",
            "trace-1",
            payloadCodec.encode(command.inputPayload()));
        TransitionWorkerCommand decoded = envelope.toCommand(payloadCodec);

        ExecutionInputSnapshot snapshot = assertInstanceOf(ExecutionInputSnapshot.class, decoded.inputPayload());
        PathPayload decodedPayload = assertInstanceOf(PathPayload.class, snapshot.payload());
        assertEquals(Path.of("/tmp/tpf/input.csv"), decodedPayload.path());
    }

    @Test
    void preservesDirectPathPayloadAcrossEnvelopeRoundTrip() {
        TransitionWorkerCommand command = command(new ExecutionInputSnapshot(
            ExecutionInputShape.UNI,
            Path.of("/tmp/tpf/input.csv")));

        TransitionCommandEnvelope envelope = TransitionCommandEnvelope.from(
            command,
            "pipeline-1",
            "bundle-1",
            "trace-1",
            payloadCodec.encode(command.inputPayload()));
        TransitionWorkerCommand decoded = envelope.toCommand(payloadCodec);

        ExecutionInputSnapshot snapshot = assertInstanceOf(ExecutionInputSnapshot.class, decoded.inputPayload());
        assertEquals(Path.of("/tmp/tpf/input.csv"), snapshot.payload());
    }

    @Test
    void preservesProtobufPayloadAcrossEnvelopeRoundTrip() {
        JsonTransitionPayloadCodec configuredCodec = codecAllowingPrefixes("com.google.protobuf.");
        TransitionWorkerCommand command = command(new ExecutionInputSnapshot(
            ExecutionInputShape.UNI,
            Int32Value.of(42)));

        TransitionCommandEnvelope envelope = TransitionCommandEnvelope.from(
            command,
            "pipeline-1",
            "bundle-1",
            "trace-1",
            configuredCodec.encode(command.inputPayload()));
        TransitionWorkerCommand decoded = envelope.toCommand(configuredCodec);

        ExecutionInputSnapshot snapshot = assertInstanceOf(ExecutionInputSnapshot.class, decoded.inputPayload());
        Int32Value decodedPayload = assertInstanceOf(Int32Value.class, snapshot.payload());
        assertEquals(42, decodedPayload.getValue());
    }

    @Test
    void rejectsMalformedPayloadDuringDecode() {
        TransitionCommandEnvelope envelope = new TransitionCommandEnvelope(
            "tenant-1",
            "exec-1",
            "pipeline-1",
            "bundle-1",
            0,
            0,
            ExecutionResultShape.SINGLE,
            0L,
            "exec-1:0:0",
            "trace-1",
            SamplePayload.class.getName(),
            JsonTransitionPayloadCodec.ENCODING,
            "{not-json");

        assertThrows(IllegalArgumentException.class, () -> envelope.toCommand(payloadCodec));
    }

    @Test
    void rejectsUnsupportedPayloadTypeDuringDecode() {
        SerializedTransitionPayload payload = new SerializedTransitionPayload(
            java.io.File.class.getName(),
            JsonTransitionPayloadCodec.ENCODING,
            "\"/tmp/not-a-transition-payload\"");

        assertThrows(IllegalArgumentException.class, () -> payloadCodec.decode(payload));
    }

    @Test
    void rejectsUnsupportedPayloadTypeDuringEncode() {
        assertThrows(IllegalArgumentException.class, () -> payloadCodec.encode(new java.io.File("/tmp/data")));
    }

    @Test
    void inProcessResultCarriesDecodedOutputsWithoutSerializedPayloads() {
        Object output = new Object();
        TransitionResultEnvelope result = TransitionResultEnvelope.completedInProcess(List.of(output));

        assertEquals(TransitionWorkerOutcome.COMPLETED, result.outcome());
        assertEquals(List.of(), result.outputPayloads());
        assertEquals(output, result.decodeOutputItems(payloadCodec).getFirst());
    }

    @Test
    void rejectsMutuallyExclusiveTransitionResultStates() {
        TransitionFailureEnvelope failure = TransitionFailureEnvelope.from(new IllegalStateException("boom"));
        TransitionAwaitSuspension suspension = new TransitionAwaitSuspension("tenant-1", "exec-1", "unit-1", 0);

        assertThrows(IllegalArgumentException.class, () ->
            new TransitionResultEnvelope(TransitionWorkerOutcome.WAITING_EXTERNAL, List.of(), suspension, failure));
        assertThrows(IllegalArgumentException.class, () ->
            new TransitionResultEnvelope(TransitionWorkerOutcome.FAILED, List.of(), suspension, failure));
        assertThrows(IllegalArgumentException.class, () ->
            new TransitionResultEnvelope(
                TransitionWorkerOutcome.COMPLETED,
                List.of(payloadCodec.encode("encoded")),
                null,
                null,
                List.of("decoded")));
    }

    @Test
    void preservesContainerPayloadsAcrossCodecRoundTrip() {
        Object decodedMap = payloadCodec.decode(payloadCodec.encode(Map.of(
            "approved", true,
            "sample", new SamplePayload("order-1", 42))));
        Object decodedSet = payloadCodec.decode(payloadCodec.encode(Set.of("a", "b")));
        Object decodedArray = payloadCodec.decode(payloadCodec.encode(new int[] {1, 2, 3}));

        Map<?, ?> map = assertInstanceOf(Map.class, decodedMap);
        SamplePayload sample = assertInstanceOf(SamplePayload.class, map.get("sample"));
        assertEquals("order-1", sample.id());
        Set<?> set = assertInstanceOf(Set.class, decodedSet);
        assertEquals(Set.of("a", "b"), set);
        List<?> array = assertInstanceOf(List.class, decodedArray);
        assertEquals(List.of(1, 2, 3), array);
    }

    @Test
    void rejectsTransitionMapPayloadWithNonStringKey() {
        assertThrows(IllegalArgumentException.class, () -> payloadCodec.encode(Map.of(1, "value")));
    }

    @Test
    void configurablePayloadPrefixAllowsApplicationTypes() {
        JsonTransitionPayloadCodec configuredCodec = codecAllowingPrefixes("java.io.");
        java.io.File file = new java.io.File("/tmp/data");

        SerializedTransitionPayload encoded = configuredCodec.encode(file);
        Object decoded = configuredCodec.decode(encoded);

        java.io.File decodedFile = assertInstanceOf(java.io.File.class, decoded);
        assertEquals(file.getPath(), decodedFile.getPath());
    }

    @Test
    void rejectsNegativeAwaitSuspensionStepIndex() {
        assertThrows(IllegalArgumentException.class, () ->
            new TransitionAwaitSuspension("tenant-1", "exec-1", "unit-1", -1));
    }

    private TransitionWorkerCommand command(Object inputPayload) {
        return new TransitionWorkerCommand(
            "tenant-1",
            "exec-1",
            0,
            0,
            ExecutionResultShape.SINGLE,
            0L,
            "exec-1:0:0",
            inputPayload);
    }

    private JsonTransitionPayloadCodec codecAllowingPrefixes(String... prefixes) {
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        PipelineOrchestratorConfig.WorkerConfig workerConfig = mock(PipelineOrchestratorConfig.WorkerConfig.class);
        JsonTransitionPayloadCodec configuredCodec = new JsonTransitionPayloadCodec();
        configuredCodec.orchestratorConfig = config;
        when(config.worker()).thenReturn(workerConfig);
        when(workerConfig.allowedPayloadPrefixes()).thenReturn(List.of(prefixes));
        return configuredCodec;
    }

    public record SamplePayload(String id, int amount) {
    }

    public record PathPayload(Path path) {
    }
}
