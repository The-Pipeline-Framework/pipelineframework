package org.pipelineframework.checkpoint.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class KafkaCheckpointPublicationRequestTest {

    @Test
    void constructsWithAllFields() {
        KafkaCheckpointPublicationRequest request = new KafkaCheckpointPublicationRequest(
            "checkout.orders.ready.v1", "idem-1", "{\"orderId\":\"o-1\"}");

        assertEquals("checkout.orders.ready.v1", request.topic());
        assertEquals("idem-1", request.key());
        assertEquals("{\"orderId\":\"o-1\"}", request.body());
    }

    @Test
    void constructsWithNullKey() {
        KafkaCheckpointPublicationRequest request = new KafkaCheckpointPublicationRequest(
            "checkout.orders.ready.v1", null, "{\"orderId\":\"o-1\"}");

        assertNull(request.key());
    }

    @Test
    void nullTopicThrows() {
        assertThrows(IllegalArgumentException.class, () ->
            new KafkaCheckpointPublicationRequest(null, "idem-1", "{\"orderId\":\"o-1\"}"));
    }

    @Test
    void blankTopicThrows() {
        assertThrows(IllegalArgumentException.class, () ->
            new KafkaCheckpointPublicationRequest("   ", "idem-1", "{\"orderId\":\"o-1\"}"));
    }

    @Test
    void emptyTopicThrows() {
        assertThrows(IllegalArgumentException.class, () ->
            new KafkaCheckpointPublicationRequest("", "idem-1", "{\"orderId\":\"o-1\"}"));
    }

    @Test
    void nullBodyThrows() {
        assertThrows(IllegalArgumentException.class, () ->
            new KafkaCheckpointPublicationRequest("checkout.orders.ready.v1", "idem-1", null));
    }

    @Test
    void blankBodyThrows() {
        assertThrows(IllegalArgumentException.class, () ->
            new KafkaCheckpointPublicationRequest("checkout.orders.ready.v1", "idem-1", "   "));
    }

    @Test
    void topicIsTrimmerd() {
        KafkaCheckpointPublicationRequest request = new KafkaCheckpointPublicationRequest(
            "  checkout.orders.ready.v1  ", "idem-1", "{}");

        assertEquals("checkout.orders.ready.v1", request.topic());
    }

    @Test
    void bodyIsTrimmerd() {
        KafkaCheckpointPublicationRequest request = new KafkaCheckpointPublicationRequest(
            "checkout.orders.ready.v1", "idem-1", "  {}  ");

        assertEquals("{}", request.body());
    }

    @Test
    void blankKeyNormalizesToNull() {
        KafkaCheckpointPublicationRequest request = new KafkaCheckpointPublicationRequest(
            "checkout.orders.ready.v1", "   ", "{}");

        assertNull(request.key());
    }

    @Test
    void keyIsTrimmerd() {
        KafkaCheckpointPublicationRequest request = new KafkaCheckpointPublicationRequest(
            "checkout.orders.ready.v1", "  idem-1  ", "{}");

        assertEquals("idem-1", request.key());
    }
}
