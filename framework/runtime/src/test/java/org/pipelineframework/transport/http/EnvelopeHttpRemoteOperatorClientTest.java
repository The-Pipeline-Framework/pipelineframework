/*
 * Copyright (c) 2023-2026 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework.transport.http;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;
import org.pipelineframework.context.TransportDispatchMetadata;
import org.pipelineframework.context.TransportDispatchMetadataHolder;
import org.pipelineframework.envelope.TpfEnvelope;
import org.pipelineframework.envelope.TpfEnvelopeCodec;
import org.pipelineframework.step.NonRetryableException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EnvelopeHttpRemoteOperatorClientTest {

    private final TpfEnvelopeCodec codec = new TpfEnvelopeCodec();

    @AfterEach
    void tearDown() {
        PipelineContextHolder.clear();
        TransportDispatchMetadataHolder.clear();
    }

    @Test
    void postsStrictEnvelopeAndDecodesJsonPayloadResponse() throws Exception {
        AtomicReference<String> contentType = new AtomicReference<>();
        AtomicReference<String> correlationHeader = new AtomicReference<>();
        AtomicReference<TpfEnvelope> requestEnvelope = new AtomicReference<>();

        try (ServerHandle server = startServer(exchange -> {
            contentType.set(exchange.getRequestHeaders().getFirst("Content-Type"));
            correlationHeader.set(exchange.getRequestHeaders().getFirst("x-tpf-correlation-id"));
            requestEnvelope.set(codec.read(exchange.getRequestBody().readAllBytes()));
            TpfEnvelope response = codec.envelope(requestEnvelope.get().control(),
                codec.jsonPayload(new DemoResponse("ok"), DemoResponse.class.getName()));
            byte[] body = codec.write(response);
            exchange.getResponseHeaders().add("Content-Type", ProtobufHttpContentTypes.APPLICATION_TPF_ENVELOPE_JSON);
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        })) {
            PipelineContextHolder.set(new PipelineContext("pipeline-v1", "replay", "prefer-cache"));
            TransportDispatchMetadataHolder.set(new TransportDispatchMetadata(
                "corr-1", "exec-1", "idem-1", 3, 2_000_000_000_000L, 1_900_000_000_000L, "parent-1"));

            EnvelopeHttpRemoteOperatorClient client = new EnvelopeHttpRemoteOperatorClient();
            DemoResponse response = client.invoke(
                    server.url(), "Chunk", "chunker", new DemoRequest("doc-1"), DemoResponse.class, 3000)
                .toCompletableFuture()
                .get(2, TimeUnit.SECONDS);

            assertEquals("ok", response.status());
            assertEquals(ProtobufHttpContentTypes.APPLICATION_TPF_ENVELOPE_JSON, contentType.get());
            assertEquals("corr-1", correlationHeader.get());
            assertEquals("Chunk", requestEnvelope.get().control().step());
            assertEquals("chunker", requestEnvelope.get().control().operatorId());
            assertEquals("corr-1", requestEnvelope.get().control().context().get("correlationId"));
            assertEquals("doc-1", requestEnvelope.get().payload().data().path("id").asText());
        }
    }

    @Test
    void mapsClientErrorsToNonRetryableException() throws Exception {
        try (ServerHandle server = startServer(exchange -> respond(exchange, 400, "bad request"))) {
            ExecutionException error = invokeExpectingFailure(server.url());

            assertEquals(NonRetryableException.class, error.getCause().getClass());
        }
    }

    @Test
    void mapsServerErrorsToRetryableException() throws Exception {
        try (ServerHandle server = startServer(exchange -> respond(exchange, 503, "try later"))) {
            ExecutionException error = invokeExpectingFailure(server.url());

            assertEquals(IllegalStateException.class, error.getCause().getClass());
            assertTrue(error.getCause().getMessage().contains("HTTP 503"));
        }
    }

    @Test
    void mapsTimeoutAndRateLimitStatusesToRetryableException() throws Exception {
        try (ServerHandle timeout = startServer(exchange -> respond(exchange, 408, "timeout"))) {
            ExecutionException error = invokeExpectingFailure(timeout.url());
            assertEquals(IllegalStateException.class, error.getCause().getClass());
        }
        try (ServerHandle rateLimit = startServer(exchange -> respond(exchange, 429, "rate limit"))) {
            ExecutionException error = invokeExpectingFailure(rateLimit.url());
            assertEquals(IllegalStateException.class, error.getCause().getClass());
        }
    }

    @Test
    void omitsBlankOptionalContextValuesFromEnvelopeControl() throws Exception {
        AtomicReference<TpfEnvelope> requestEnvelope = new AtomicReference<>();

        try (ServerHandle server = startServer(exchange -> {
            requestEnvelope.set(codec.read(exchange.getRequestBody().readAllBytes()));
            TpfEnvelope response = codec.envelope(requestEnvelope.get().control(),
                codec.jsonPayload(new DemoResponse("ok"), DemoResponse.class.getName()));
            byte[] body = codec.write(response);
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        })) {
            TransportDispatchMetadataHolder.set(new TransportDispatchMetadata(
                " ", "", " idem-1 ", 0, 2_000_000_000_000L, 1_900_000_000_000L, "   "));

            EnvelopeHttpRemoteOperatorClient client = new EnvelopeHttpRemoteOperatorClient();
            client.invoke(server.url(), "Chunk", "chunker", new DemoRequest("doc-1"), DemoResponse.class, 3000)
                .toCompletableFuture()
                .get(2, TimeUnit.SECONDS);

            assertFalse(requestEnvelope.get().control().context().containsKey("correlationId"));
            assertFalse(requestEnvelope.get().control().context().containsKey("executionId"));
            assertEquals("idem-1", requestEnvelope.get().control().context().get("idempotencyKey"));
            assertFalse(requestEnvelope.get().control().context().containsKey("parentItemId"));
        }
    }

    @Test
    void rejectsExpiredDeadlineBeforeDispatch() {
        TransportDispatchMetadataHolder.set(new TransportDispatchMetadata(
            "corr-1", "exec-1", "idem-1", 0, System.currentTimeMillis() - 1000, System.currentTimeMillis(), "parent-1"));
        EnvelopeHttpRemoteOperatorClient client = new EnvelopeHttpRemoteOperatorClient();

        ExecutionException error = assertThrows(ExecutionException.class, () -> client.invoke(
                "http://127.0.0.1:1/", "Chunk", "chunker", new DemoRequest("doc-1"), DemoResponse.class, 3000)
            .toCompletableFuture()
            .get(2, TimeUnit.SECONDS));

        assertEquals(NonRetryableException.class, error.getCause().getClass());
        assertTrue(error.getCause().getMessage().contains("deadline exceeded"));
    }

    private ExecutionException invokeExpectingFailure(String url) {
        EnvelopeHttpRemoteOperatorClient client = new EnvelopeHttpRemoteOperatorClient();
        return assertThrows(ExecutionException.class, () -> client.invoke(
                url, "Chunk", "chunker", new DemoRequest("doc-1"), DemoResponse.class, 3000)
            .toCompletableFuture()
            .get(2, TimeUnit.SECONDS));
    }

    private void respond(HttpExchange exchange, int status, String message) throws IOException {
        byte[] body = message.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, body.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
        }
    }

    private ServerHandle startServer(HttpHandler handler) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", handler);
        server.start();
        return new ServerHandle(server);
    }

    private record ServerHandle(HttpServer server) implements AutoCloseable {
        String url() {
            return "http://127.0.0.1:" + server.getAddress().getPort() + "/";
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }

    private record DemoRequest(String id) {
    }

    private record DemoResponse(String status) {
    }
}
