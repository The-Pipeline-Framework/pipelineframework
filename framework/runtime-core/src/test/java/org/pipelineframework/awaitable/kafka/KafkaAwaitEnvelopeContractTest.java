package org.pipelineframework.awaitable.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

class KafkaAwaitEnvelopeContractTest {

    @Test
    void completionEnvelopeNormalizesIdentifiers() {
        KafkaAwaitCompletionEnvelope envelope = new KafkaAwaitCompletionEnvelope(
            " tenant-1 ",
            " interaction-1 ",
            " ",
            " token ",
            " completion-1 ",
            Map.of("decision", "approved"),
            " actor ");

        assertEquals("tenant-1", envelope.tenantId());
        assertEquals("interaction-1", envelope.interactionId());
        assertEquals(null, envelope.correlationId());
        assertEquals("token", envelope.resumeToken());
        assertEquals("completion-1", envelope.idempotencyKey());
        assertEquals("actor", envelope.actor());
    }

    @Test
    void completionEnvelopeRequiresTenantAndInteractionIdentity() {
        assertThrows(IllegalArgumentException.class, () -> new KafkaAwaitCompletionEnvelope(
            " ",
            "interaction-1",
            null,
            null,
            null,
            Map.of(),
            null));

        assertThrows(IllegalArgumentException.class, () -> new KafkaAwaitCompletionEnvelope(
            "tenant-1",
            " ",
            null,
            null,
            null,
            Map.of(),
            null));
    }

    @Test
    void dispatchEnvelopeNormalizesAndDefensivelyCopiesMetadata() {
        KafkaAwaitDispatchEnvelope withoutMetadata = new KafkaAwaitDispatchEnvelope(
            "tenant-1",
            "execution-1",
            "interaction-1",
            "correlation-1",
            "step-1",
            123L,
            "input.Type",
            "output.Type",
            "token",
            Map.of("amount", 100),
            null);

        assertEquals(Map.of(), withoutMetadata.transportMetadata());

        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("topic", "requests");
        KafkaAwaitDispatchEnvelope envelope = new KafkaAwaitDispatchEnvelope(
            "tenant-1",
            "execution-1",
            "interaction-1",
            "correlation-1",
            "step-1",
            123L,
            "input.Type",
            "output.Type",
            "token",
            Map.of("amount", 100),
            metadata);

        metadata.put("topic", "changed");
        metadata.put("extra", "ignored");

        assertEquals(Map.of("topic", "requests"), envelope.transportMetadata());
        assertThrows(UnsupportedOperationException.class, () -> envelope.transportMetadata().put("extra", "value"));
    }

    @Test
    void publishRequestNormalizesTopicAndHeaders() {
        KafkaAwaitPublishRequest request = new KafkaAwaitPublishRequest(
            " requests ",
            "key-1",
            Map.of("x-source", "tpf"),
            "{}");

        assertEquals("requests", request.topic());
        assertEquals("tpf", request.headers().get("x-source"));
    }

    @Test
    void publishRequestRejectsBlankTopic() {
        assertThrows(IllegalArgumentException.class, () -> new KafkaAwaitPublishRequest(
            " ",
            "key-1",
            Map.of(),
            "{}"));
    }
}
