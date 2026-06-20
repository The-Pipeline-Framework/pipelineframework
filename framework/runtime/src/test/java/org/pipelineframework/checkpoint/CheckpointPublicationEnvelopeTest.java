package org.pipelineframework.checkpoint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.pipeline.PipelineJson;

class CheckpointPublicationEnvelopeTest {

    @Test
    void constructsWithAllFields() {
        ObjectNode payload = JsonNodeFactory.instance.objectNode().put("orderId", "o-1");
        CheckpointPublicationEnvelope envelope = new CheckpointPublicationEnvelope(
            "orders-ready", "tenant-1", "idem-1", payload, 1000L);

        assertEquals("orders-ready", envelope.publication());
        assertEquals("tenant-1", envelope.tenantId());
        assertEquals("idem-1", envelope.idempotencyKey());
        assertEquals("o-1", envelope.payload().get("orderId").asText());
        assertEquals(1000L, envelope.publishedAtEpochMs());
    }

    @Test
    void constructsWithNullOptionalFields() {
        ObjectNode payload = JsonNodeFactory.instance.objectNode().put("orderId", "o-1");
        CheckpointPublicationEnvelope envelope = new CheckpointPublicationEnvelope(
            "orders-ready", null, null, payload, 1L);

        assertEquals("orders-ready", envelope.publication());
        assertNull(envelope.tenantId());
        assertNull(envelope.idempotencyKey());
    }

    @Test
    void nullPublicationThrows() {
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        assertThrows(IllegalArgumentException.class, () ->
            new CheckpointPublicationEnvelope(null, "tenant-1", "idem-1", payload, 1L));
    }

    @Test
    void blankPublicationThrows() {
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        assertThrows(IllegalArgumentException.class, () ->
            new CheckpointPublicationEnvelope("   ", "tenant-1", "idem-1", payload, 1L));
    }

    @Test
    void emptyPublicationThrows() {
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        assertThrows(IllegalArgumentException.class, () ->
            new CheckpointPublicationEnvelope("", "tenant-1", "idem-1", payload, 1L));
    }

    @Test
    void nullPayloadThrows() {
        assertThrows(IllegalArgumentException.class, () ->
            new CheckpointPublicationEnvelope("orders-ready", "tenant-1", "idem-1", null, 1L));
    }

    @Test
    void jsonNullPayloadThrows() {
        assertThrows(IllegalArgumentException.class, () ->
            new CheckpointPublicationEnvelope(
                "orders-ready", "tenant-1", "idem-1",
                JsonNodeFactory.instance.nullNode(), 1L));
    }

    @Test
    void zeroTimestampThrows() {
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        assertThrows(IllegalArgumentException.class, () ->
            new CheckpointPublicationEnvelope("orders-ready", "tenant-1", "idem-1", payload, 0L));
    }

    @Test
    void negativeTimestampThrows() {
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        assertThrows(IllegalArgumentException.class, () ->
            new CheckpointPublicationEnvelope("orders-ready", "tenant-1", "idem-1", payload, -1L));
    }

    @Test
    void publicationIsTrimmerd() {
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        CheckpointPublicationEnvelope envelope = new CheckpointPublicationEnvelope(
            "  orders-ready  ", "tenant-1", "idem-1", payload, 1L);
        assertEquals("orders-ready", envelope.publication());
    }

    @Test
    void blankTenantIdNormalizesToNull() {
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        CheckpointPublicationEnvelope envelope = new CheckpointPublicationEnvelope(
            "orders-ready", "   ", "idem-1", payload, 1L);
        assertNull(envelope.tenantId());
    }

    @Test
    void blankIdempotencyKeyNormalizesToNull() {
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        CheckpointPublicationEnvelope envelope = new CheckpointPublicationEnvelope(
            "orders-ready", "tenant-1", "   ", payload, 1L);
        assertNull(envelope.idempotencyKey());
    }

    @Test
    void toRequestReturnsPublicationAndPayload() {
        ObjectNode payload = JsonNodeFactory.instance.objectNode().put("orderId", "o-1");
        CheckpointPublicationEnvelope envelope = new CheckpointPublicationEnvelope(
            "orders-ready", "tenant-1", "idem-1", payload, 1L);

        CheckpointPublicationRequest request = envelope.toRequest();

        assertEquals("orders-ready", request.publication());
        assertEquals("o-1", request.payload().get("orderId").asText());
    }

    @Test
    void roundTripsThroughJsonSerialization() throws Exception {
        ObjectNode payload = JsonNodeFactory.instance.objectNode().put("orderId", "o-1");
        CheckpointPublicationEnvelope original = new CheckpointPublicationEnvelope(
            "orders-ready", "tenant-1", "idem-1", payload, 1000L);

        String json = PipelineJson.mapper().writeValueAsString(original);
        CheckpointPublicationEnvelope deserialized = PipelineJson.mapper()
            .readValue(json, CheckpointPublicationEnvelope.class);

        assertEquals(original.publication(), deserialized.publication());
        assertEquals(original.tenantId(), deserialized.tenantId());
        assertEquals(original.idempotencyKey(), deserialized.idempotencyKey());
        assertEquals(original.publishedAtEpochMs(), deserialized.publishedAtEpochMs());
        assertEquals("o-1", deserialized.payload().get("orderId").asText());
    }
}