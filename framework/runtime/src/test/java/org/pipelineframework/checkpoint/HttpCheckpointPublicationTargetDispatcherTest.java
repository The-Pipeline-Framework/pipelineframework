package org.pipelineframework.checkpoint;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.pipelineframework.checkpoint.grpc.CheckpointPublishRequest;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.transport.http.ProtobufHttpContentTypes;

class HttpCheckpointPublicationTargetDispatcherTest {

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
