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
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import com.google.rpc.Code;
import com.google.rpc.Status;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;
import org.pipelineframework.context.TransportDispatchMetadata;
import org.pipelineframework.context.TransportDispatchMetadataHolder;
import org.pipelineframework.step.NonRetryableException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProtobufHttpRemoteOperatorClientTest {

    @AfterEach
    void tearDown() {
        PipelineContextHolder.clear();
        TransportDispatchMetadataHolder.clear();
    }

    @Test
    void propagatesCanonicalHeadersAndReturnsRawResponseBytes() throws Exception {
        AtomicReference<String> versionHeader = new AtomicReference<>();
        AtomicReference<String> correlationHeader = new AtomicReference<>();
        AtomicReference<String> executionHeader = new AtomicReference<>();
        AtomicReference<String> idempotencyHeader = new AtomicReference<>();
        AtomicReference<String> retryHeader = new AtomicReference<>();
        AtomicReference<String> deadlineHeader = new AtomicReference<>();
        AtomicReference<String> dispatchHeader = new AtomicReference<>();
        AtomicReference<String> parentHeader = new AtomicReference<>();
        AtomicReference<byte[]> requestBody = new AtomicReference<>();

        byte[] responseBody = new byte[] {1, 2, 3, 4};
        try (ServerHandle server = startServer(exchange -> {
            versionHeader.set(exchange.getRequestHeaders().getFirst("x-pipeline-version"));
            correlationHeader.set(exchange.getRequestHeaders().getFirst("x-tpf-correlation-id"));
            executionHeader.set(exchange.getRequestHeaders().getFirst("x-tpf-execution-id"));
            idempotencyHeader.set(exchange.getRequestHeaders().getFirst("x-tpf-idempotency-key"));
            retryHeader.set(exchange.getRequestHeaders().getFirst("x-tpf-retry-attempt"));
            deadlineHeader.set(exchange.getRequestHeaders().getFirst("x-tpf-deadline-epoch-ms"));
            dispatchHeader.set(exchange.getRequestHeaders().getFirst("x-tpf-dispatch-ts-epoch-ms"));
            parentHeader.set(exchange.getRequestHeaders().getFirst("x-tpf-parent-item-id"));
            requestBody.set(exchange.getRequestBody().readAllBytes());
            exchange.getResponseHeaders().add("Content-Type", ProtobufHttpContentTypes.APPLICATION_X_PROTOBUF);
            exchange.sendResponseHeaders(200, responseBody.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(responseBody);
            }
        })) {
            PipelineContextHolder.set(new PipelineContext("v1", "replay", "prefer-cache"));
            TransportDispatchMetadataHolder.set(new TransportDispatchMetadata(
                "corr-1",
                "exec-1",
                "idem-1",
                2,
                2_000_000_000_000L,
                1_900_000_000_000L,
                "parent-1"));

            ProtobufHttpRemoteOperatorClient client = new ProtobufHttpRemoteOperatorClient();
            byte[] result = client.invoke(server.url(), "charge-card", new byte[] {9, 8, 7}, 3000)
                .await().atMost(Duration.ofSeconds(2));

            assertArrayEquals(new byte[] {9, 8, 7}, requestBody.get());
            assertArrayEquals(responseBody, result);
            assertEquals("v1", versionHeader.get());
            assertEquals("corr-1", correlationHeader.get());
            assertEquals("exec-1", executionHeader.get());
            assertEquals("idem-1", idempotencyHeader.get());
            assertEquals("2", retryHeader.get());
            assertEquals("2000000000000", deadlineHeader.get());
            assertEquals("1900000000000", dispatchHeader.get());
            assertEquals("parent-1", parentHeader.get());
        }
    }

    @Test
    void rejectsExpiredDeadlineBeforeSending() {
        PipelineContextHolder.set(new PipelineContext("v1", null, null));
        TransportDispatchMetadataHolder.set(new TransportDispatchMetadata(
            "corr-1",
            "exec-1",
            "idem-1",
            0,
            System.currentTimeMillis() - 1000,
            System.currentTimeMillis() - 2000,
            null));

        ProtobufHttpRemoteOperatorClient client = new ProtobufHttpRemoteOperatorClient();
        NonRetryableException ex = assertThrows(
            NonRetryableException.class,
            () -> client.invoke("http://localhost:1/ignored", "charge-card", new byte[] {1}, 3000)
                .await().atMost(Duration.ofSeconds(2)));
        assertTrue(ex.getMessage().contains("deadline exceeded"));
    }

    @Test
    void mapsNonRetryableStatusEnvelopeToNonRetryableException() throws Exception {
        Status status = Status.newBuilder()
            .setCode(Code.INVALID_ARGUMENT_VALUE)
            .setMessage("invalid payload")
            .build();

        try (ServerHandle server = startServer(exchange -> respondStatus(exchange, 400, status))) {
            PipelineContextHolder.set(new PipelineContext("v1", null, null));
            TransportDispatchMetadataHolder.set(new TransportDispatchMetadata(
                "corr-1", "exec-1", "idem-1", 0, null, System.currentTimeMillis(), null));
            ProtobufHttpRemoteOperatorClient client = new ProtobufHttpRemoteOperatorClient();
            NonRetryableException ex = assertThrows(
                NonRetryableException.class,
                () -> client.invoke(server.url(), "charge-card", new byte[] {1}, 3000)
                    .await().atMost(Duration.ofSeconds(2)));
            assertTrue(ex.getMessage().contains("INVALID_ARGUMENT"));
            assertTrue(ex.getMessage().contains("invalid payload"));
        }
    }

    @Test
    void mapsRetryableStatusEnvelopeToIllegalStateException() throws Exception {
        Status status = Status.newBuilder()
            .setCode(Code.UNAVAILABLE_VALUE)
            .setMessage("try again")
            .build();

        try (ServerHandle server = startServer(exchange -> respondStatus(exchange, 503, status))) {
            PipelineContextHolder.set(new PipelineContext("v1", null, null));
            TransportDispatchMetadataHolder.set(new TransportDispatchMetadata(
                "corr-1", "exec-1", "idem-1", 0, null, System.currentTimeMillis(), null));
            ProtobufHttpRemoteOperatorClient client = new ProtobufHttpRemoteOperatorClient();
            IllegalStateException ex = assertThrows(
                IllegalStateException.class,
                () -> client.invoke(server.url(), "charge-card", new byte[] {1}, 3000)
                    .await().atMost(Duration.ofSeconds(2)));
            assertTrue(ex.getMessage().contains("UNAVAILABLE"));
            assertTrue(ex.getMessage().contains("try again"));
        }
    }

    private static void respondStatus(HttpExchange exchange, int statusCode, Status status) throws IOException {
        byte[] response = status.toByteArray();
        exchange.getResponseHeaders().add("Content-Type", ProtobufHttpContentTypes.APPLICATION_X_PROTOBUF);
        exchange.sendResponseHeaders(statusCode, response.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(response);
        }
    }

    private static ServerHandle startServer(HttpHandler handler) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/", handler);
        server.start();
        return new ServerHandle(server);
    }

    private record ServerHandle(HttpServer server) implements AutoCloseable {
        String url() {
            return "http://localhost:" + server.getAddress().getPort() + "/";
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }
}
