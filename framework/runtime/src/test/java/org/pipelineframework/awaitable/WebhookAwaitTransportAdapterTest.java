package org.pipelineframework.awaitable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.spi.AwaitTransportAdapter;
import org.pipelineframework.config.pipeline.PipelineJson;

class WebhookAwaitTransportAdapterTest {

    private HttpServer server;

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void dispatchPostsSignedEnvelope() throws Exception {
        AtomicReference<String> bodyRef = new AtomicReference<>();
        server = startServer(202, bodyRef);
        WebhookAwaitTransportAdapter adapter = adapter();
        AwaitStepDescriptor descriptor = descriptor(Map.of(
            "request", Map.of("url", serverUrl()),
            "callback", Map.of("baseUrl", "https://orchestrator.example")));
        AwaitInteractionRecord interaction = interaction();

        var result = adapter.dispatch(new AwaitTransportAdapter.AwaitDispatchRequest<>(
            descriptor,
            interaction,
            Map.of("orderId", "o-1"))).await().atMost(Duration.ofSeconds(5));

        assertEquals("webhook", result.metadata().get("adapter"));
        assertEquals(202, result.metadata().get("statusCode"));
        JsonNode body = PipelineJson.mapper().readTree(bodyRef.get());
        assertEquals("tenant-1", body.get("tenantId").asText());
        assertEquals("interaction-1", body.get("interactionId").asText());
        assertEquals("corr-1", body.get("correlationId").asText());
        assertTrue(body.hasNonNull("resumeToken"));
        assertEquals("https://orchestrator.example/pipeline/interactions/complete",
            body.get("callback").get("completionUrl").asText());
    }

    @Test
    void dispatchFailsOnNonSuccessStatus() throws IOException {
        AtomicReference<String> bodyRef = new AtomicReference<>();
        server = startServer(503, bodyRef);
        WebhookAwaitTransportAdapter adapter = adapter();
        AwaitStepDescriptor descriptor = descriptor(Map.of("request", Map.of("url", serverUrl())));

        assertThrows(IllegalStateException.class, () -> adapter.dispatch(new AwaitTransportAdapter.AwaitDispatchRequest<>(
            descriptor,
            interaction(),
            Map.of("orderId", "o-1"))).await().atMost(Duration.ofSeconds(5)));
    }

    @Test
    void dispatchRequiresWebhookUrl() {
        WebhookAwaitTransportAdapter adapter = adapter();
        AwaitStepDescriptor descriptor = descriptor(Map.of());

        assertThrows(IllegalArgumentException.class, () -> adapter.dispatch(new AwaitTransportAdapter.AwaitDispatchRequest<>(
            descriptor,
            interaction(),
            Map.of())).await().indefinitely());
    }

    @Test
    void dispatchFailsOnInvalidUrl() {
        WebhookAwaitTransportAdapter adapter = adapter();
        AwaitStepDescriptor descriptor = descriptor(Map.of("request", Map.of("url", "http://[bad")));

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            adapter.dispatch(new AwaitTransportAdapter.AwaitDispatchRequest<>(
                descriptor,
                interaction(),
                Map.of())).await().indefinitely());

        assertTrue(exception.getMessage().contains("Invalid webhook await dispatch configuration"));
    }

    @Test
    void dispatchFailsOnInvalidTimeout() {
        WebhookAwaitTransportAdapter adapter = adapter();
        AwaitStepDescriptor descriptor = descriptor(Map.of(
            "request", Map.of("url", "https://partner.example/await"),
            "timeout", "not-a-duration"));

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            adapter.dispatch(new AwaitTransportAdapter.AwaitDispatchRequest<>(
                descriptor,
                interaction(),
                Map.of())).await().indefinitely());

        assertTrue(exception.getMessage().contains("Invalid webhook await dispatch configuration"));
    }

    @Test
    void dispatchUsesConfiguredCompletionPath() throws Exception {
        AtomicReference<String> bodyRef = new AtomicReference<>();
        server = startServer(202, bodyRef);
        WebhookAwaitTransportAdapter adapter = adapter();
        AwaitStepDescriptor descriptor = descriptor(Map.of(
            "request", Map.of("url", serverUrl()),
            "callback", Map.of(
                "baseUrl", "https://orchestrator.example/",
                "completionPath", "custom/complete")));

        adapter.dispatch(new AwaitTransportAdapter.AwaitDispatchRequest<>(
            descriptor,
            interaction(),
            Map.of("orderId", "o-1"))).await().atMost(Duration.ofSeconds(5));

        JsonNode body = PipelineJson.mapper().readTree(bodyRef.get());
        assertEquals("https://orchestrator.example/custom/complete",
            body.get("callback").get("completionUrl").asText());
    }

    private WebhookAwaitTransportAdapter adapter() {
        WebhookAwaitTransportAdapter adapter = new WebhookAwaitTransportAdapter();
        adapter.resumeTokenService = new AwaitResumeTokenService("secret-value-for-tests");
        return adapter;
    }

    private HttpServer startServer(int status, AtomicReference<String> bodyRef) throws IOException {
        HttpServer httpServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        httpServer.createContext("/await", exchange -> {
            bodyRef.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            byte[] body = "ok".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        httpServer.start();
        return httpServer;
    }

    private String serverUrl() {
        return "http://127.0.0.1:" + server.getAddress().getPort() + "/await";
    }

    private static AwaitStepDescriptor descriptor(Map<String, Object> config) {
        return new AwaitStepDescriptor(
            "FraudCheck",
            "com.example.Request",
            "com.example.Decision",
            Duration.ofMinutes(10),
            "signedResumeToken",
            "webhook",
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
            "webhook",
            Map.of(),
            99_999_999_999L,
            1_000L,
            2_000L,
            99_999L);
    }
}
