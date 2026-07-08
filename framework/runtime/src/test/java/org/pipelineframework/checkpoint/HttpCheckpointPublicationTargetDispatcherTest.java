package org.pipelineframework.checkpoint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.pipelineframework.checkpoint.grpc.CheckpointPublishRequest;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.transport.http.ProtobufHttpContentTypes;

class HttpCheckpointPublicationTargetDispatcherTest {

    @Test
    void resolveTargetBuildsFullUrlFromBaseUrlAndPath() {
        HttpCheckpointPublicationTargetDispatcher dispatcher = new HttpCheckpointPublicationTargetDispatcher();
        PipelineHandoffConfig.TargetConfig config = mock(PipelineHandoffConfig.TargetConfig.class);
        when(config.baseUrl()).thenReturn(Optional.of("http://localhost:8081"));
        when(config.path()).thenReturn(Optional.of("/pipeline/checkpoints/publish"));
        when(config.method()).thenReturn("POST");
        when(config.encoding()).thenReturn(Optional.of(PublicationEncoding.PROTO));
        when(config.contentType()).thenReturn(Optional.empty());
        when(config.idempotencyHeader()).thenReturn(Optional.empty());

        ResolvedCheckpointPublicationTarget target = dispatcher.resolveTarget("orders-ready", "deliver", config);

        assertEquals("orders-ready", target.publication());
        assertEquals("deliver", target.targetId());
        assertEquals(PublicationTargetKind.HTTP, target.kind());
        assertEquals(PublicationEncoding.PROTO, target.encoding());
        assertEquals("http://localhost:8081/pipeline/checkpoints/publish", target.endpoint());
        assertEquals("POST", target.method());
        assertEquals(ProtobufHttpContentTypes.APPLICATION_X_PROTOBUF, target.contentType());
        assertEquals("Idempotency-Key", target.idempotencyHeader());
    }

    @Test
    void resolveTargetStripsTrailingSlashFromBaseUrl() {
        HttpCheckpointPublicationTargetDispatcher dispatcher = new HttpCheckpointPublicationTargetDispatcher();
        PipelineHandoffConfig.TargetConfig config = mock(PipelineHandoffConfig.TargetConfig.class);
        when(config.baseUrl()).thenReturn(Optional.of("http://localhost:8081/"));
        when(config.path()).thenReturn(Optional.of("/pipeline/checkpoints/publish"));
        when(config.method()).thenReturn("POST");
        when(config.encoding()).thenReturn(Optional.of(PublicationEncoding.PROTO));
        when(config.contentType()).thenReturn(Optional.empty());
        when(config.idempotencyHeader()).thenReturn(Optional.empty());

        ResolvedCheckpointPublicationTarget target = dispatcher.resolveTarget("orders-ready", "deliver", config);

        assertEquals("http://localhost:8081/pipeline/checkpoints/publish", target.endpoint());
    }

    @Test
    void resolveTargetPrefixesPathWithSlashIfMissing() {
        HttpCheckpointPublicationTargetDispatcher dispatcher = new HttpCheckpointPublicationTargetDispatcher();
        PipelineHandoffConfig.TargetConfig config = mock(PipelineHandoffConfig.TargetConfig.class);
        when(config.baseUrl()).thenReturn(Optional.of("http://localhost:8081"));
        when(config.path()).thenReturn(Optional.of("pipeline/checkpoints/publish"));
        when(config.method()).thenReturn("POST");
        when(config.encoding()).thenReturn(Optional.of(PublicationEncoding.PROTO));
        when(config.contentType()).thenReturn(Optional.empty());
        when(config.idempotencyHeader()).thenReturn(Optional.empty());

        ResolvedCheckpointPublicationTarget target = dispatcher.resolveTarget("orders-ready", "deliver", config);

        assertEquals("http://localhost:8081/pipeline/checkpoints/publish", target.endpoint());
    }

    @Test
    void resolveTargetUsesJsonEncodingWithJsonContentType() {
        HttpCheckpointPublicationTargetDispatcher dispatcher = new HttpCheckpointPublicationTargetDispatcher();
        PipelineHandoffConfig.TargetConfig config = mock(PipelineHandoffConfig.TargetConfig.class);
        when(config.baseUrl()).thenReturn(Optional.of("http://localhost:8081"));
        when(config.path()).thenReturn(Optional.empty());
        when(config.method()).thenReturn("POST");
        when(config.encoding()).thenReturn(Optional.of(PublicationEncoding.JSON));
        when(config.contentType()).thenReturn(Optional.empty());
        when(config.idempotencyHeader()).thenReturn(Optional.empty());

        ResolvedCheckpointPublicationTarget target = dispatcher.resolveTarget("orders-ready", "deliver", config);

        assertEquals(PublicationEncoding.JSON, target.encoding());
        assertEquals(ProtobufHttpContentTypes.APPLICATION_JSON, target.contentType());
    }

    @Test
    void resolveTargetUsesExplicitContentTypeWhenProvided() {
        HttpCheckpointPublicationTargetDispatcher dispatcher = new HttpCheckpointPublicationTargetDispatcher();
        PipelineHandoffConfig.TargetConfig config = mock(PipelineHandoffConfig.TargetConfig.class);
        when(config.baseUrl()).thenReturn(Optional.of("http://localhost:8081"));
        when(config.path()).thenReturn(Optional.empty());
        when(config.method()).thenReturn("POST");
        when(config.encoding()).thenReturn(Optional.of(PublicationEncoding.PROTO));
        when(config.contentType()).thenReturn(Optional.of("application/x-custom-proto"));
        when(config.idempotencyHeader()).thenReturn(Optional.empty());

        ResolvedCheckpointPublicationTarget target = dispatcher.resolveTarget("orders-ready", "deliver", config);

        assertEquals("application/x-custom-proto", target.contentType());
    }

    @Test
    void resolveTargetUsesExplicitIdempotencyHeaderWhenProvided() {
        HttpCheckpointPublicationTargetDispatcher dispatcher = new HttpCheckpointPublicationTargetDispatcher();
        PipelineHandoffConfig.TargetConfig config = mock(PipelineHandoffConfig.TargetConfig.class);
        when(config.baseUrl()).thenReturn(Optional.of("http://localhost:8081"));
        when(config.path()).thenReturn(Optional.empty());
        when(config.method()).thenReturn("POST");
        when(config.encoding()).thenReturn(Optional.empty());
        when(config.contentType()).thenReturn(Optional.empty());
        when(config.idempotencyHeader()).thenReturn(Optional.of("X-Custom-Idempotency"));

        ResolvedCheckpointPublicationTarget target = dispatcher.resolveTarget("orders-ready", "deliver", config);

        assertEquals("X-Custom-Idempotency", target.idempotencyHeader());
    }

    @Test
    void resolveTargetDefaultsEncodingToProto() {
        HttpCheckpointPublicationTargetDispatcher dispatcher = new HttpCheckpointPublicationTargetDispatcher();
        PipelineHandoffConfig.TargetConfig config = mock(PipelineHandoffConfig.TargetConfig.class);
        when(config.baseUrl()).thenReturn(Optional.of("http://localhost:8081"));
        when(config.path()).thenReturn(Optional.empty());
        when(config.method()).thenReturn("POST");
        when(config.encoding()).thenReturn(Optional.empty());
        when(config.contentType()).thenReturn(Optional.empty());
        when(config.idempotencyHeader()).thenReturn(Optional.empty());

        ResolvedCheckpointPublicationTarget target = dispatcher.resolveTarget("orders-ready", "deliver", config);

        assertEquals(PublicationEncoding.PROTO, target.encoding());
    }

    @Test
    void resolveTargetFailsWhenBaseUrlAbsent() {
        HttpCheckpointPublicationTargetDispatcher dispatcher = new HttpCheckpointPublicationTargetDispatcher();
        PipelineHandoffConfig.TargetConfig config = mock(PipelineHandoffConfig.TargetConfig.class);
        when(config.baseUrl()).thenReturn(Optional.empty());
        when(config.method()).thenReturn("POST");

        IllegalStateException error = assertThrows(IllegalStateException.class,
            () -> dispatcher.resolveTarget("orders-ready", "deliver", config));
        assertEquals(
            "Checkpoint publication 'orders-ready' target 'deliver' requires base-url for HTTP delivery",
            error.getMessage());
    }

    @Test
    void resolveTargetFailsWhenBaseUrlBlank() {
        HttpCheckpointPublicationTargetDispatcher dispatcher = new HttpCheckpointPublicationTargetDispatcher();
        PipelineHandoffConfig.TargetConfig config = mock(PipelineHandoffConfig.TargetConfig.class);
        when(config.baseUrl()).thenReturn(Optional.of("   "));
        when(config.method()).thenReturn("POST");

        assertThrows(IllegalStateException.class,
            () -> dispatcher.resolveTarget("orders-ready", "deliver", config));
    }

    @Test
    void resolveTargetFailsForNonPostMethod() {
        HttpCheckpointPublicationTargetDispatcher dispatcher = new HttpCheckpointPublicationTargetDispatcher();
        PipelineHandoffConfig.TargetConfig config = mock(PipelineHandoffConfig.TargetConfig.class);
        when(config.baseUrl()).thenReturn(Optional.of("http://localhost:8081"));
        when(config.path()).thenReturn(Optional.empty());
        when(config.method()).thenReturn("PUT");

        IllegalStateException error = assertThrows(IllegalStateException.class,
            () -> dispatcher.resolveTarget("orders-ready", "deliver", config));
        assertEquals(
            "Checkpoint publication 'orders-ready' target 'deliver' only supports HTTP method POST",
            error.getMessage());
    }

    @Test
    void resolveTargetKindIsHttp() {
        HttpCheckpointPublicationTargetDispatcher dispatcher = new HttpCheckpointPublicationTargetDispatcher();
        assertEquals(PublicationTargetKind.HTTP, dispatcher.kind());
    }

    @Test
    void dispatchSendsProtobufWrappedCheckpointRequest() throws Exception {
        AtomicReference<String> contentType = new AtomicReference<>();
        AtomicReference<String> accept = new AtomicReference<>();
        AtomicReference<byte[]> body = new AtomicReference<>();

        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/pipeline/checkpoints/publish", exchange -> respond(exchange, contentType, accept, body));
        server.start();
        try {
            int port = server.getAddress().getPort();
            HttpCheckpointPublicationTargetDispatcher dispatcher = new HttpCheckpointPublicationTargetDispatcher();
            ResolvedCheckpointPublicationTarget target = new ResolvedCheckpointPublicationTarget(
                "orders-ready",
                "deliver",
                PublicationTargetKind.HTTP,
                PublicationEncoding.PROTO,
                ProtobufHttpContentTypes.APPLICATION_X_PROTOBUF,
                "Idempotency-Key",
                "http://localhost:" + port + "/pipeline/checkpoints/publish",
                "POST");

            dispatcher.dispatch(
                target,
                new CheckpointPublicationRequest(
                    "orders-ready",
                    PipelineJson.mapper().valueToTree(new PublishedOrder("o-1", "c-1"))),
                "tenant-1",
                "idem-1").await().indefinitely();

            assertEquals(ProtobufHttpContentTypes.APPLICATION_X_PROTOBUF, contentType.get());
            assertEquals(ProtobufHttpContentTypes.APPLICATION_JSON, accept.get());

            CheckpointPublishRequest request = CheckpointPublishRequest.parseFrom(body.get());
            assertEquals("orders-ready", request.getPublication());
            assertEquals("tenant-1", request.getTenantId());
            assertEquals("idem-1", request.getIdempotencyKey());
            JsonNode payload = PipelineJson.mapper().readTree(request.getPayloadJson().toStringUtf8());
            assertEquals("o-1", payload.get("orderId").asText());
        } finally {
            server.stop(0);
        }
    }

    private void respond(
        HttpExchange exchange,
        AtomicReference<String> contentType,
        AtomicReference<String> accept,
        AtomicReference<byte[]> body
    ) throws IOException {
        contentType.set(exchange.getRequestHeaders().getFirst("Content-Type"));
        accept.set(exchange.getRequestHeaders().getFirst("Accept"));
        body.set(exchange.getRequestBody().readAllBytes());
        byte[] response = "{\"executionId\":\"exec-1\"}".getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", ProtobufHttpContentTypes.APPLICATION_JSON);
        exchange.sendResponseHeaders(202, response.length);
        exchange.getResponseBody().write(response);
        exchange.close();
    }

    private record PublishedOrder(String orderId, String customerId) {
    }
}
