package org.pipelineframework.connector.llm.langchain4j;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.sun.net.httpserver.HttpServer;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.pipelineframework.connector.ConnectorExecutionContext;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.llm.LlmProviderConfiguration;
import org.pipelineframework.connector.llm.LlmToolDefinition;
import org.pipelineframework.connector.llm.LlmTurnRequest;
import org.pipelineframework.connector.llm.StructuredOutputSchemaMode;

import static org.junit.jupiter.api.Assertions.assertEquals;

@QuarkusTest
class LangChain4jAugmentationTest {
    @Inject
    LangChain4jOllamaQueryConnector connector;

    @Test
    void augmentedConnectorReadsADeterministicStreamingProviderResponse() throws Exception {
        var requests = new AtomicInteger();
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/chat", exchange -> {
            exchange.getRequestBody().readAllBytes();
            requests.incrementAndGet();
            byte[] response = """
                {"model":"migration-test","message":{"role":"assistant","content":"{\\"value\\":\\"ready\\"}"},"done":false}
                {"model":"migration-test","message":{"role":"assistant","content":""},"done":true,"done_reason":"stop","prompt_eval_count":3,"eval_count":2}
                """.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/x-ndjson");
            exchange.sendResponseHeaders(200, response.length);
            try (var output = exchange.getResponseBody()) {
                output.write(response);
            } finally {
                exchange.close();
            }
        });
        server.start();
        try {
            String endpoint = "http://127.0.0.1:" + server.getAddress().getPort();
            var client = connector.createClientResolver(
                    new LlmProviderConfiguration("migration-test", Optional.of(endpoint)),
                    ConnectorRuntimeContext.empty())
                .resolve(ConnectorExecutionContext.empty()).toCompletableFuture().get(10, TimeUnit.SECONDS);
            var request = new LlmTurnRequest("Return the requested value.", "{}", List.of(
                new LlmToolDefinition("complete", "Complete", """
                    {"type":"object","properties":{"value":{"type":"string"}},"required":["value"],"additionalProperties":false}
                    """)), StructuredOutputSchemaMode.REQUIRED);
            var decision = client.decide(request).toCompletableFuture().get(10, TimeUnit.SECONDS);
            assertEquals("complete", decision.proposal().alias());
            assertEquals("{\"value\":\"ready\"}", decision.proposal().argumentsJson());
            assertEquals(1, requests.get());
        } finally {
            server.stop(0);
        }
    }
}
