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

package org.pipelineframework.transport.function;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.smallrye.mutiny.Multi;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class HttpRemoteFunctionInvokeAdapterTest {
    private static final ObjectMapper MAPPER = new ObjectMapper().findAndRegisterModules();

    @Test
    void invokesOneToOneOverHttp() throws Exception {
        TraceEnvelope<Integer> responseEnvelope = TraceEnvelope.root(
            "trace-remote", "item-remote", "search.out", "v1", "idem-remote", 99);
        try (ServerHandle server = startServer(exchange -> respondJson(exchange, MAPPER.writeValueAsString(responseEnvelope)))) {
            HttpRemoteFunctionInvokeAdapter<Integer, Integer> adapter = new HttpRemoteFunctionInvokeAdapter<>();
            FunctionTransportContext context = FunctionTransportContext.of(
                "req-remote-1",
                "handler",
                "invoke",
                Map.of(FunctionTransportContext.ATTR_TARGET_URL, server.url()));

            TraceEnvelope<Integer> input = TraceEnvelope.root(
                "trace-local", "item-local", "search.in", "v1", "idem-local", 7);
            TraceEnvelope<Integer> output = adapter.invokeOneToOne(input, context)
                .await().atMost(Duration.ofSeconds(2));

            assertEquals(99, output.payload());
            assertEquals("trace-remote", output.traceId());
        }
    }

    @Test
    void invokesManyToManyOverHttp() throws Exception {
        List<TraceEnvelope<Integer>> responseEnvelopes = List.of(
            TraceEnvelope.root("trace-remote", "item-r1", "search.out", "v1", "idem-r1", 10),
            TraceEnvelope.root("trace-remote", "item-r2", "search.out", "v1", "idem-r2", 20));
        AtomicReference<Throwable> serverFailure = new AtomicReference<>();
        CountDownLatch handledRequest = new CountDownLatch(1);

        try (ServerHandle server = startServer(exchange -> {
            try {
                JsonNode request = MAPPER.readTree(exchange.getRequestBody());
                assertTrue(request.isArray());
                respondJson(exchange, MAPPER.writeValueAsString(responseEnvelopes));
            } catch (Throwable t) {
                serverFailure.set(t);
                throw t;
            } finally {
                handledRequest.countDown();
            }
        })) {
            HttpRemoteFunctionInvokeAdapter<Integer, Integer> adapter = new HttpRemoteFunctionInvokeAdapter<>();
            FunctionTransportContext context = FunctionTransportContext.of(
                "req-remote-2",
                "handler",
                "invoke",
                Map.of(FunctionTransportContext.ATTR_TARGET_URL, server.url()));

            List<TraceEnvelope<Integer>> output = adapter.invokeManyToMany(
                    Multi.createFrom().items(
                        TraceEnvelope.root("trace-local", "item-1", "search.in", "v1", "idem-1", 1),
                        TraceEnvelope.root("trace-local", "item-2", "search.in", "v1", "idem-2", 2)),
                    context)
                .collect().asList().await().atMost(Duration.ofSeconds(2));

            assertEquals(2, output.size());
            assertEquals(List.of(10, 20), output.stream().map(TraceEnvelope::payload).toList());
            assertTrue(handledRequest.await(2, TimeUnit.SECONDS), "server did not handle request in time");
            if (serverFailure.get() != null) {
                fail("server-side assertion failed", serverFailure.get());
            }
        }
    }

    @Test
    void failsWhenTargetUrlIsMissing() {
        HttpRemoteFunctionInvokeAdapter<Integer, Integer> adapter = new HttpRemoteFunctionInvokeAdapter<>();
        FunctionTransportContext context = FunctionTransportContext.of("req-remote-3", "handler", "invoke");
        TraceEnvelope<Integer> input = TraceEnvelope.root("trace-local", "item-local", "search.in", "v1", "idem-local", 7);

        IllegalStateException ex = assertThrows(
            IllegalStateException.class,
            () -> adapter.invokeOneToOne(input, context).await().atMost(Duration.ofSeconds(2)));
        assertTrue(ex.getMessage().contains(FunctionTransportContext.ATTR_TARGET_URL));
    }

    @Test
    void resolvesTargetUrlFromHandlerMetadataUsingRestClientConfig() throws Exception {
        TraceEnvelope<Integer> responseEnvelope = TraceEnvelope.root(
            "trace-remote-h", "item-remote-h", "search.out", "v1", "idem-remote-h", 123);
        try (ServerHandle server = startServer(exchange -> respondJson(exchange, MAPPER.writeValueAsString(responseEnvelope)))) {
            String key = "quarkus.rest-client.process-index-document.url";
            String previous = System.getProperty(key);
            System.setProperty(key, server.url());
            try {
                HttpRemoteFunctionInvokeAdapter<Integer, Integer> adapter = new HttpRemoteFunctionInvokeAdapter<>();
                FunctionTransportContext context = FunctionTransportContext.of(
                    "req-remote-4",
                    "handler",
                    "invoke",
                    Map.of(
                        FunctionTransportContext.ATTR_TARGET_RUNTIME, "pipeline",
                        FunctionTransportContext.ATTR_TARGET_MODULE, "index-document-svc",
                        FunctionTransportContext.ATTR_TARGET_HANDLER, "ProcessIndexDocumentFunctionHandler"));

                TraceEnvelope<Integer> input = TraceEnvelope.root(
                    "trace-local-4", "item-local-4", "search.in", "v1", "idem-local-4", 5);
                TraceEnvelope<Integer> output = adapter.invokeOneToOne(input, context)
                    .await().atMost(Duration.ofSeconds(2));

                assertEquals(123, output.payload());
            } finally {
                if (previous == null) {
                    System.clearProperty(key);
                } else {
                    System.setProperty(key, previous);
                }
            }
        }
    }

    @Test
    void resolvesTargetUrlFromModuleMetadataUsingRestClientConfig() throws Exception {
        TraceEnvelope<Integer> responseEnvelope = TraceEnvelope.root(
            "trace-remote-m", "item-remote-m", "search.out", "v1", "idem-remote-m", 321);
        try (ServerHandle server = startServer(exchange -> respondJson(exchange, MAPPER.writeValueAsString(responseEnvelope)))) {
            String key = "quarkus.rest-client.index-document-svc.url";
            String previous = System.getProperty(key);
            System.setProperty(key, server.url());
            try {
                HttpRemoteFunctionInvokeAdapter<Integer, Integer> adapter = new HttpRemoteFunctionInvokeAdapter<>();
                FunctionTransportContext context = FunctionTransportContext.of(
                    "req-remote-5",
                    "handler",
                    "invoke",
                    Map.of(
                        FunctionTransportContext.ATTR_TARGET_RUNTIME, "pipeline",
                        FunctionTransportContext.ATTR_TARGET_MODULE, "index-document-svc"));

                TraceEnvelope<Integer> input = TraceEnvelope.root(
                    "trace-local-5", "item-local-5", "search.in", "v1", "idem-local-5", 6);
                TraceEnvelope<Integer> output = adapter.invokeOneToOne(input, context)
                    .await().atMost(Duration.ofSeconds(2));

                assertEquals(321, output.payload());
            } finally {
                if (previous == null) {
                    System.clearProperty(key);
                } else {
                    System.setProperty(key, previous);
                }
            }
        }
    }

    private static ServerHandle startServer(HttpHandler handler) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/", handler);
        server.start();
        return new ServerHandle(server);
    }

    private static void respondJson(HttpExchange exchange, String body) throws IOException {
        byte[] bytes = body.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private record ServerHandle(HttpServer server) implements AutoCloseable {
        private String url() {
            return "http://127.0.0.1:" + server.getAddress().getPort() + "/";
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }
}
