package org.pipelineframework.awaitable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.databind.JsonNode;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.kafka.KafkaAwaitPublishRequest;
import org.pipelineframework.awaitable.spi.AwaitTransportAdapter;
import org.pipelineframework.config.pipeline.PipelineJson;

class KafkaAwaitTransportAdapterTest {

    @Test
    void dispatchPublishesFrameworkEnvelope() throws Exception {
        AtomicReference<KafkaAwaitPublishRequest> publishRef = new AtomicReference<>();
        KafkaAwaitTransportAdapter adapter = adapter(request -> {
            publishRef.set(request);
            return Uni.createFrom().voidItem();
        });
        AwaitStepDescriptor descriptor = descriptor(Map.of(
            "request", Map.of(
                "topic", "fraud-check.requests",
                "key", "correlationId"),
            "response", Map.of("topic", "fraud-check.decisions"),
            "headers", Map.of("x-source", "tpf")));

        var result = adapter.dispatch(new AwaitTransportAdapter.AwaitDispatchRequest<>(
            descriptor,
            interaction(),
            Map.of("orderId", "o-1"))).await().atMost(Duration.ofSeconds(5));

        KafkaAwaitPublishRequest request = publishRef.get();
        assertEquals("fraud-check.requests", request.topic());
        assertEquals("corr-1", request.key());
        assertEquals("tpf", request.headers().get("x-source"));
        assertEquals("tenant-1", request.headers().get("tpf-tenant-id"));
        assertEquals("fraud-check.decisions", request.headers().get("tpf-response-topic"));
        JsonNode body = PipelineJson.mapper().readTree(request.body());
        assertEquals("tenant-1", body.get("tenantId").asText());
        assertEquals("exec-1", body.get("executionId").asText());
        assertEquals("interaction-1", body.get("interactionId").asText());
        assertEquals("corr-1", body.get("correlationId").asText());
        assertEquals("FraudCheck", body.get("stepId").asText());
        assertTrue(body.hasNonNull("resumeToken"));
        assertEquals("o-1", body.get("requestPayload").get("orderId").asText());
        assertEquals("fraud-check.requests", result.metadata().get("requestTopic"));
        assertEquals("fraud-check.decisions", result.metadata().get("responseTopic"));
        assertEquals("correlationId", result.metadata().get("keyStrategy"));
    }

    @Test
    void dispatchDefaultsKeyToInteractionId() {
        AtomicReference<KafkaAwaitPublishRequest> publishRef = new AtomicReference<>();
        KafkaAwaitTransportAdapter adapter = adapter(request -> {
            publishRef.set(request);
            return Uni.createFrom().voidItem();
        });
        AwaitStepDescriptor descriptor = descriptor(Map.of(
            "request", Map.of("topic", "requests"),
            "response", Map.of("topic", "responses")));

        adapter.dispatch(new AwaitTransportAdapter.AwaitDispatchRequest<>(
            descriptor,
            interaction(),
            Map.of())).await().atMost(Duration.ofSeconds(5));

        assertEquals("interaction-1", publishRef.get().key());
    }

    @Test
    void dispatchFailsWhenPublisherFails() {
        KafkaAwaitTransportAdapter adapter = adapter(request ->
            Uni.createFrom().failure(new IllegalStateException("broker unavailable")));
        AwaitStepDescriptor descriptor = descriptor(Map.of(
            "request", Map.of("topic", "requests"),
            "response", Map.of("topic", "responses")));

        IllegalStateException exception = assertThrows(IllegalStateException.class, () ->
            adapter.dispatch(new AwaitTransportAdapter.AwaitDispatchRequest<>(
                descriptor,
                interaction(),
                Map.of())).await().atMost(Duration.ofSeconds(5)));

        assertEquals("broker unavailable", exception.getMessage());
    }

    @Test
    void dispatchFailsWithoutPublisherProvider() {
        KafkaAwaitTransportAdapter adapter = new KafkaAwaitTransportAdapter();
        adapter.resumeTokenService = new AwaitResumeTokenService("secret-value-for-tests");
        AwaitStepDescriptor descriptor = descriptor(Map.of(
            "request", Map.of("topic", "requests"),
            "response", Map.of("topic", "responses")));

        IllegalStateException exception = assertThrows(IllegalStateException.class, () ->
            adapter.dispatch(new AwaitTransportAdapter.AwaitDispatchRequest<>(
                descriptor,
                interaction(),
                Map.of())).await().atMost(Duration.ofSeconds(5)));

        assertTrue(exception.getMessage().contains("KafkaAwaitPublisher"));
    }

    @Test
    void dispatchRejectsInvalidKeyStrategy() {
        KafkaAwaitTransportAdapter adapter = adapter(request -> Uni.createFrom().voidItem());
        AwaitStepDescriptor descriptor = descriptor(Map.of(
            "request", Map.of(
                "topic", "requests",
                "key", "orderId"),
            "response", Map.of("topic", "responses")));

        assertThrows(IllegalArgumentException.class, () ->
            adapter.dispatch(new AwaitTransportAdapter.AwaitDispatchRequest<>(
                descriptor,
                interaction(),
                Map.of())).await().atMost(Duration.ofSeconds(5)));
    }

    private static KafkaAwaitTransportAdapter adapter(org.pipelineframework.awaitable.kafka.KafkaAwaitPublisher publisher) {
        KafkaAwaitTransportAdapter adapter = new KafkaAwaitTransportAdapter(publisher);
        adapter.resumeTokenService = new AwaitResumeTokenService("secret-value-for-tests");
        return adapter;
    }

    private static AwaitStepDescriptor descriptor(Map<String, Object> config) {
        return new AwaitStepDescriptor(
            "FraudCheck",
            "com.example.Request",
            "com.example.Decision",
            Duration.ofMinutes(10),
            "signedResumeToken",
            "kafka",
            config,
            java.util.List.of("orderId"));
    }

    private static AwaitInteractionRecord interaction() {
        return new AwaitInteractionRecord(
            "tenant-1",
            "exec-1",
            "FraudCheck",
            1,
            "com.example.Decision",
            "interaction-1",
            "corr-1",
            "cause-1",
            "idem-1",
            1L,
            AwaitInteractionStatus.DISPATCHING,
            Map.of("orderId", "o-1"),
            null,
            null,
            null,
            null,
            "kafka",
            Map.of(),
            99_999_999_999L,
            1_000L,
            2_000L,
            99_999L);
    }
}
