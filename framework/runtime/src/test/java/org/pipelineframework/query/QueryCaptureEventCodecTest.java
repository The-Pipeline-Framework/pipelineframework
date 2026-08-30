package org.pipelineframework.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import com.google.protobuf.StringValue;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.connector.QueryObservation;
import org.pipelineframework.connector.QueryTokenUsage;

class QueryCaptureEventCodecTest {
    private final QueryCaptureEventCodec codec = new QueryCaptureEventCodec();

    @Test
    void durableRecordRedactsInputAndRoundTripsTypedOutput() {
        QueryCaptureRecord record = found("secret prompt", "{\"value\":\"safe\"}", TestOutput.class.getName());

        QueryCaptureRecord decoded = codec.toRecord(codec.decode(codec.encode(codec.unary(record))));

        assertEquals(new QueryCapturePayloadCodec(PipelineJson.mapper()).decode(
            decoded.outputJson(), TestOutput.class), new TestOutput("safe"));
        assertFalse(codec.encode(codec.unary(record)).contains("secret prompt"));
        assertEquals(71, decoded.inputJson().length());
        assertEquals("sha256:", decoded.inputJson().substring(0, 7));
    }

    @Test
    void rejectsUnknownSchemaAndOutputType() {
        assertThrows(QueryCaptureStoreException.class, () -> codec.decode("{\"schemaVersion\":99}"));
        assertThrows(QueryCaptureStoreException.class,
            () -> codec.unary(found("input", "{}", "missing.Type")));
    }

    @Test
    void rejectsCorruptOutputJsonWithoutInstantiatingAStoredType() throws Exception {
        String encoded = codec.encode(codec.unary(
            found("input", "{\"value\":\"safe\"}", TestOutput.class.getName())));
        com.fasterxml.jackson.databind.node.ObjectNode corrupted =
            (com.fasterxml.jackson.databind.node.ObjectNode) PipelineJson.mapper().readTree(encoded);
        corrupted.put("outputJson", "{");

        assertThrows(QueryCaptureStoreException.class,
            () -> codec.decode(PipelineJson.mapper().writeValueAsString(corrupted)));
    }

    @Test
    void rejectsUnavailableNotFoundOutputType() throws Exception {
        QueryCaptureRecord record = new QueryCaptureRecord(
            "tenant", "execution", 1, "customer.find", "v1", "capture-key",
            "input", "", "", Instant.ofEpochMilli(10),
            QueryCaptureStatus.NOT_FOUND, "missing");
        com.fasterxml.jackson.databind.node.ObjectNode corrupted =
            (com.fasterxml.jackson.databind.node.ObjectNode) PipelineJson.mapper().readTree(
                codec.encode(codec.unary(record)));
        corrupted.put("outputType", "missing.Type");
        corrupted.put("runtimeType", "missing.Type");

        assertThrows(QueryCaptureStoreException.class,
            () -> codec.decode(PipelineJson.mapper().writeValueAsString(corrupted)));
    }

    @Test
    void protobufParserRejectsUnknownFields() {
        QueryCapturePayloadCodec payloadCodec = new QueryCapturePayloadCodec(PipelineJson.mapper());

        assertThrows(QueryCaptureStoreException.class,
            () -> payloadCodec.decode("{\"value\":\"safe\",\"hidden\":\"no\"}", StringValue.class));
    }

    @Test
    void protobufPayloadRoundTripsThroughDurableSnapshot() {
        QueryCapturePayloadCodec payloadCodec = new QueryCapturePayloadCodec(PipelineJson.mapper());
        StringValue value = StringValue.of("typed");
        QueryCaptureRecord record = found(
            "input", payloadCodec.encode(value, StringValue.class), StringValue.class.getName());

        QueryCaptureRecord decoded = codec.toRecord(codec.decode(codec.encode(codec.unary(record))));

        assertEquals(value, payloadCodec.decode(decoded.outputJson(), StringValue.class));
    }

    @Test
    void durableUnaryEventPreservesProviderReportedObservation() {
        QueryObservation observation = QueryObservation.live(
            Optional.of(new QueryTokenUsage(
                OptionalLong.of(13), OptionalLong.empty(), OptionalLong.of(31))),
            Optional.of("provider-model"), Optional.of("length"));
        QueryCaptureRecord record = new QueryCaptureRecord(
            "tenant", "execution", 1, "customer.find", "v1", "capture-key", "input",
            "{\"value\":\"safe\"}", TestOutput.class.getName(), Instant.ofEpochMilli(10),
            QueryCaptureStatus.FOUND, "found", Optional.of(observation));

        QueryCaptureRecord decoded = codec.toRecord(codec.decode(codec.encode(codec.unary(record))));

        assertEquals(Optional.of(observation), decoded.observation());
        assertEquals(record.outputJson(), decoded.outputJson());
        assertEquals(record.outputType(), decoded.outputType());
    }

    @Test
    void decodesLegacyDurableEventWithoutObservationFields() throws Exception {
        com.fasterxml.jackson.databind.node.ObjectNode legacy =
            (com.fasterxml.jackson.databind.node.ObjectNode) PipelineJson.mapper().readTree(
                codec.encode(codec.unary(found("input", "{\"value\":\"safe\"}", TestOutput.class.getName()))));
        List.of(
            "observationPresent", "inputTokens", "outputTokens", "totalTokens", "responseModel", "finishReason")
            .forEach(legacy::remove);

        QueryCaptureRecord decoded = codec.toRecord(codec.decode(PipelineJson.mapper().writeValueAsString(legacy)));

        assertEquals(Optional.empty(), decoded.observation());
    }

    private static QueryCaptureRecord found(String input, String output, String outputType) {
        return new QueryCaptureRecord(
            "tenant", "execution", 1, "customer.find", "v1", "capture-key",
            input, output, outputType, Instant.ofEpochMilli(10), QueryCaptureStatus.FOUND, "found");
    }

    record TestOutput(String value) {
    }
}
