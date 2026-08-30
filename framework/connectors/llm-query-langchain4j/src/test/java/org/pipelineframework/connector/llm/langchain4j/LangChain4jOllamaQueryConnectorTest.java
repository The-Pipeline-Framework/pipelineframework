package org.pipelineframework.connector.llm.langchain4j;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import dev.langchain4j.agent.tool.ToolExecutionRequest;
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.ImageContent;
import dev.langchain4j.data.message.PdfFileContent;
import dev.langchain4j.data.message.TextContent;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.request.ResponseFormatType;
import dev.langchain4j.model.chat.request.json.JsonObjectSchema;
import dev.langchain4j.model.chat.response.ChatResponse;
import dev.langchain4j.model.output.FinishReason;
import dev.langchain4j.model.output.TokenUsage;
import org.pipelineframework.connector.llm.LlmToolDefinition;
import org.pipelineframework.connector.llm.LlmProviderConfiguration;
import org.pipelineframework.connector.llm.LlmTurnRequest;
import org.pipelineframework.connector.ConnectorExecutionContext;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.MaterializedPayload;
import org.pipelineframework.connector.QueryObservationOrigin;
import org.pipelineframework.repository.PayloadReference;

class LangChain4jOllamaQueryConnectorTest {
    @Test
    void rejectsNonPositiveRequestTimeouts() {
        assertThrows(IllegalArgumentException.class,
            () -> new LangChain4jOllamaQueryConnector.OllamaRuntimeSettings(Duration.ZERO, false));
    }

    @Test
    void configuresTheOllamaModelWithAnExplicitBoundedTimeout() {
        AtomicReference<String> baseUrl = new AtomicReference<>();
        AtomicReference<String> modelName = new AtomicReference<>();
        AtomicReference<Duration> timeout = new AtomicReference<>();
        AtomicReference<Boolean> thinking = new AtomicReference<>();
        AtomicInteger maxRetries = new AtomicInteger(-1);
        ChatModel model = model(AiMessage.from("unused"));
        var connector = new LangChain4jOllamaQueryConnector((configuredBaseUrl, configuredModel, configuredTimeout,
                                                              configuredThinking, configuredMaxRetries) -> {
            baseUrl.set(configuredBaseUrl);
            modelName.set(configuredModel);
            timeout.set(configuredTimeout);
            thinking.set(configuredThinking);
            maxRetries.set(configuredMaxRetries);
            return model;
        }, new LangChain4jOllamaQueryConnector.OllamaRuntimeSettings(Duration.ofSeconds(90), false));

        assertInstanceOf(LangChain4jOllamaQueryConnector.LangChain4jDecisionClient.class,
            connector.createClientResolver(
                new LlmProviderConfiguration("qwen3", Optional.of("http://ollama.internal:11434")),
                ConnectorRuntimeContext.empty())
                .resolve(ConnectorExecutionContext.empty()).toCompletableFuture().join());
        assertEquals("http://ollama.internal:11434", baseUrl.get());
        assertEquals("qwen3", modelName.get());
        assertEquals(Duration.ofSeconds(90), timeout.get());
        assertEquals(false, thinking.get());
        assertEquals(0, maxRetries.get());
    }

    @Test
    void retainsCompatibleTimeoutAndThinkingDefaultsWithoutInternalRetries() {
        AtomicReference<Duration> timeout = new AtomicReference<>();
        AtomicReference<Boolean> thinking = new AtomicReference<>();
        AtomicInteger maxRetries = new AtomicInteger(-1);
        var connector = new LangChain4jOllamaQueryConnector((baseUrl, modelName, configuredTimeout,
                                                              configuredThinking, configuredMaxRetries) -> {
            timeout.set(configuredTimeout);
            thinking.set(configuredThinking);
            maxRetries.set(configuredMaxRetries);
            return model(AiMessage.from("unused"));
        });

        connector.createClientResolver(new LlmProviderConfiguration("qwen3", Optional.empty()),
                ConnectorRuntimeContext.empty())
            .resolve(ConnectorExecutionContext.empty()).toCompletableFuture().join();

        assertEquals(Duration.ofSeconds(30), timeout.get());
        assertEquals(true, thinking.get());
        assertEquals(0, maxRetries.get());
    }

    @Test
    void performsOneLowLevelChatCallAndReturnsTheProposalWithoutExecutingIt() {
        AtomicInteger calls = new AtomicInteger();
        ChatModel model = new ChatModel() {
            @Override
            public ChatResponse doChat(ChatRequest request) {
                calls.incrementAndGet();
                assertEquals(List.of("charge"),
                    request.toolSpecifications().stream().map(specification -> specification.name()).toList());
                return ChatResponse.builder()
                    .aiMessage(AiMessage.from(ToolExecutionRequest.builder()
                        .name("charge")
                        .arguments("{\"amount\":42}")
                        .build()))
                    .tokenUsage(new TokenUsage(10, 5, 99))
                    .modelName("provider-model-v2")
                    .finishReason(FinishReason.TOOL_EXECUTION)
                    .build();
            }
        };
        var client = new LangChain4jOllamaQueryConnector.LangChain4jDecisionClient(model, Runnable::run);

        var proposal = client.decide(new LlmTurnRequest(
            "Decide once.",
            "{\"invoiceId\":\"7\"}",
            List.of(new LlmToolDefinition(
                "charge", "Propose a charge", """
                    {"type":"object","properties":{"amount":{"type":"integer"}},
                     "required":["amount"],"additionalProperties":false,
                     "$defs":{"Arguments":{"type":"object","properties":{"amount":{"type":"integer"}},
                     "required":["amount"],"additionalProperties":false}}}
                    """)),
            org.pipelineframework.connector.llm.StructuredOutputSchemaMode.OPTIONAL))
            .toCompletableFuture().join();

        assertEquals("charge", proposal.proposal().alias());
        assertEquals("{\"amount\":42}", proposal.proposal().argumentsJson());
        var observation = proposal.observation().orElseThrow();
        assertEquals(QueryObservationOrigin.LIVE_PROVIDER, observation.origin());
        assertEquals(OptionalLong.of(10), observation.tokenUsage().orElseThrow().inputTokens());
        assertEquals(OptionalLong.of(5), observation.tokenUsage().orElseThrow().outputTokens());
        assertEquals(OptionalLong.of(99), observation.tokenUsage().orElseThrow().totalTokens());
        assertEquals(Optional.of("provider-model-v2"), observation.responseModel());
        assertEquals(Optional.of("tool_execution"), observation.finishReason());
        assertEquals(1, calls.get());
    }

    @Test
    void preservesPartialUsageWithoutDerivingMissingCounts() {
        ChatModel model = new ChatModel() {
            @Override
            public ChatResponse doChat(ChatRequest request) {
                return ChatResponse.builder()
                    .aiMessage(AiMessage.from(ToolExecutionRequest.builder()
                        .name("complete")
                        .arguments("{}")
                        .build()))
                    .tokenUsage(new TokenUsage(null, 7, null))
                    .build();
            }
        };
        var client = new LangChain4jOllamaQueryConnector.LangChain4jDecisionClient(model, Runnable::run);

        var observation = client.decide(optionalCompletionRequest()).toCompletableFuture().join()
            .observation().orElseThrow();

        assertEquals(OptionalLong.empty(), observation.tokenUsage().orElseThrow().inputTokens());
        assertEquals(OptionalLong.of(7), observation.tokenUsage().orElseThrow().outputTokens());
        assertEquals(OptionalLong.empty(), observation.tokenUsage().orElseThrow().totalTokens());
        assertTrue(observation.responseModel().isEmpty());
        assertTrue(observation.finishReason().isEmpty());
    }

    @Test
    void returnsAnObservationWhenProviderMetadataIsAbsent() {
        var client = new LangChain4jOllamaQueryConnector.LangChain4jDecisionClient(
            model(AiMessage.from(ToolExecutionRequest.builder().name("complete").arguments("{}").build())),
            Runnable::run);

        var observation = client.decide(optionalCompletionRequest()).toCompletableFuture().join()
            .observation().orElseThrow();

        assertTrue(observation.tokenUsage().isEmpty());
        assertTrue(observation.responseModel().isEmpty());
        assertTrue(observation.finishReason().isEmpty());
    }

    @Test
    void ignoresInvalidMetadataWithoutLosingTheApplicationDecision() {
        ChatModel model = new ChatModel() {
            @Override
            public ChatResponse doChat(ChatRequest request) {
                return ChatResponse.builder()
                    .aiMessage(AiMessage.from(ToolExecutionRequest.builder()
                        .name("complete")
                        .arguments("{\"status\":\"ok\"}")
                        .build()))
                    .tokenUsage(new TokenUsage(-1, -2, -3))
                    .modelName("m".repeat(300))
                    .finishReason(FinishReason.STOP)
                    .build();
            }
        };
        var client = new LangChain4jOllamaQueryConnector.LangChain4jDecisionClient(model, Runnable::run);

        var decision = client.decide(optionalCompletionRequest()).toCompletableFuture().join();

        assertEquals("complete", decision.proposal().alias());
        assertEquals("{\"status\":\"ok\"}", decision.proposal().argumentsJson());
        var observation = decision.observation().orElseThrow();
        assertTrue(observation.tokenUsage().isEmpty());
        assertTrue(observation.responseModel().isEmpty());
        assertEquals(Optional.of("stop"), observation.finishReason());
    }

    private static LlmTurnRequest optionalCompletionRequest() {
        return new LlmTurnRequest("Decide once.", "{}", List.of(
            new LlmToolDefinition("complete", "Complete", """
                {"type":"object","properties":{},"required":[],"additionalProperties":false}
                """)), org.pipelineframework.connector.llm.StructuredOutputSchemaMode.OPTIONAL);
    }

    @Test
    void usesNativeJsonSchemaForRequiredDirectCompletion() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        ChatModel model = new ChatModel() {
            @Override
            public ChatResponse doChat(ChatRequest request) {
                calls.incrementAndGet();
                assertTrue(request.toolSpecifications().isEmpty());
                assertEquals(ResponseFormatType.JSON, request.responseFormat().type());
                JsonObjectSchema schema = assertInstanceOf(
                    JsonObjectSchema.class, request.responseFormat().jsonSchema().rootElement());
                assertEquals(List.of("facts", "recommendation", "note"), schema.required());
                JsonObjectSchema facts = assertInstanceOf(
                    JsonObjectSchema.class, schema.definitions().get("InvoiceFacts"));
                assertEquals(List.of("supplier", "invoiceNumber", "totalAmount", "diagnostic"), facts.required());
                JsonObjectSchema recommendation = assertInstanceOf(
                    JsonObjectSchema.class, schema.definitions().get("PropertyRecommendation"));
                assertEquals(List.of("propertyId", "explanation"), recommendation.required());
                return ChatResponse.builder().aiMessage(AiMessage.from("""
                    {"facts":{"supplier":"Acme","invoiceNumber":"INV-1","totalAmount":12.34,
                    "diagnostic":"Header text."},"recommendation":{"propertyId":"home",
                    "explanation":"Address match."},"note":"Friday callout."}
                    """)).build();
            }
        };
        var client = new LangChain4jOllamaQueryConnector.LangChain4jDecisionClient(model, Runnable::run);
        LlmToolDefinition completion = new LlmToolDefinition("complete", "Complete", """
            {"type":"object","properties":{
              "facts":{"$ref":"#/$defs/InvoiceFacts"},
              "recommendation":{"$ref":"#/$defs/PropertyRecommendation"},
              "note":{"type":"string"}},
             "required":["facts","recommendation","note"],"additionalProperties":false,
             "$defs":{
               "InvoiceFacts":{"type":"object","properties":{
                 "supplier":{"type":"string"},"invoiceNumber":{"type":"string"},
                 "totalAmount":{"type":"number"},"diagnostic":{"type":"string"}},
                 "required":["supplier","invoiceNumber","totalAmount","diagnostic"],
                 "additionalProperties":false},
               "PropertyRecommendation":{"type":"object","properties":{
                 "propertyId":{"type":"string"},"explanation":{"type":"string"}},
                 "required":["propertyId","explanation"],"additionalProperties":false}}}
            """);

        assertTrue(client.supportsNativeStructuredOutput(List.of(completion)));
        assertFalse(client.supportsNativeStructuredOutput(List.of(
            completion, new LlmToolDefinition("charge", "Charge", "{\"type\":\"object\"}"))));
        var proposal = client.decide(new LlmTurnRequest("Analyse once.", "{}", List.of(completion)))
            .toCompletableFuture().join();

        assertEquals("complete", proposal.proposal().alias());
        assertEquals(new ObjectMapper().readTree("""
            {"facts":{"supplier":"Acme","invoiceNumber":"INV-1","totalAmount":12.34,
            "diagnostic":"Header text."},"recommendation":{"propertyId":"home",
            "explanation":"Address match."},"note":"Friday callout."}
            """), new ObjectMapper().readTree(proposal.proposal().argumentsJson()));
        assertEquals(1, calls.get());
    }

    @Test
    void normalizesMissingToolRequestsToAnEmptyProposal() {
        var proposal = decide(AiMessage.builder().toolExecutionRequests(null).build());

        assertEquals("", proposal.alias());
        assertEquals("{}", proposal.argumentsJson());
    }

    @Test
    void normalizesNullToolNameAndArgumentsToAnEmptyProposal() {
        var proposal = decide(AiMessage.from(ToolExecutionRequest.builder().build()));

        assertEquals("", proposal.alias());
        assertEquals("{}", proposal.argumentsJson());
    }

    @Test
    void mapsMaterializedPdfToLangChain4jMediaWithoutAnotherModelCall() {
        AtomicReference<ChatRequest> observed = new AtomicReference<>();
        AtomicInteger calls = new AtomicInteger();
        ChatModel model = new ChatModel() {
            @Override
            public ChatResponse doChat(ChatRequest request) {
                calls.incrementAndGet();
                observed.set(request);
                return ChatResponse.builder().aiMessage(AiMessage.from("{}")).build();
            }
        };
        PayloadReference reference = new PayloadReference(
            "test", "invoices", "invoice.pdf", "application/pdf", null, "sha256:test", 4,
            null, java.util.Map.of(), Optional.empty());
        var client = new LangChain4jOllamaQueryConnector.LangChain4jDecisionClient(model, Runnable::run);

        client.decide(new LlmTurnRequest(
            "Analyse once.",
            "{}",
            List.of(new MaterializedPayload(
                reference, new byte[]{1, 2, 3, 4}, "application/pdf", null, "sha256:test")),
            List.of(new LlmToolDefinition("complete", "Complete", """
                {"type":"object","properties":{},"required":[],"additionalProperties":false}
                """)),
            org.pipelineframework.connector.llm.StructuredOutputSchemaMode.REQUIRED))
            .toCompletableFuture().join();

        UserMessage user = assertInstanceOf(UserMessage.class, observed.get().messages().get(1));
        assertInstanceOf(TextContent.class, user.contents().get(0));
        PdfFileContent pdf = assertInstanceOf(PdfFileContent.class, user.contents().get(1));
        assertEquals("AQIDBA==", pdf.pdfFile().base64Data());
        assertEquals("application/pdf", pdf.pdfFile().mimeType());
        assertEquals(1, calls.get());
    }

    @Test
    void mapsMaterializedPngToLangChain4jVisionContent() {
        AtomicReference<ChatRequest> observed = new AtomicReference<>();
        AtomicInteger calls = new AtomicInteger();
        ChatModel model = new ChatModel() {
            @Override
            public ChatResponse doChat(ChatRequest request) {
                calls.incrementAndGet();
                observed.set(request);
                return ChatResponse.builder().aiMessage(AiMessage.from("{}")).build();
            }
        };
        PayloadReference reference = new PayloadReference(
            "test", "invoices", "invoice.png", "image/png", null, "sha256:test", 4,
            null, java.util.Map.of(), Optional.empty());
        var client = new LangChain4jOllamaQueryConnector.LangChain4jDecisionClient(model, Runnable::run);

        client.decide(new LlmTurnRequest(
            "Analyse once.",
            "{}",
            List.of(new MaterializedPayload(
                reference, new byte[]{1, 2, 3, 4}, "image/png", null, "sha256:test")),
            List.of(new LlmToolDefinition("complete", "Complete", """
                {"type":"object","properties":{},"required":[],"additionalProperties":false}
                """)),
            org.pipelineframework.connector.llm.StructuredOutputSchemaMode.REQUIRED))
            .toCompletableFuture().join();

        UserMessage user = assertInstanceOf(UserMessage.class, observed.get().messages().get(1));
        ImageContent image = assertInstanceOf(ImageContent.class, user.contents().get(1));
        assertEquals("AQIDBA==", image.image().base64Data());
        assertEquals("image/png", image.image().mimeType());
        assertEquals(1, calls.get());
    }

    private static org.pipelineframework.connector.llm.LlmToolProposal decide(AiMessage message) {
        var client = new LangChain4jOllamaQueryConnector.LangChain4jDecisionClient(model(message), Runnable::run);
        return client.decide(new LlmTurnRequest("Decide once.", "{}", List.of(
            new LlmToolDefinition("complete", "Complete", """
                {"type":"object","properties":{},"required":[],"additionalProperties":false}
                """)), org.pipelineframework.connector.llm.StructuredOutputSchemaMode.OPTIONAL))
            .toCompletableFuture().join().proposal();
    }

    private static ChatModel model(AiMessage message) {
        return new ChatModel() {
            @Override
            public ChatResponse doChat(ChatRequest request) {
                return ChatResponse.builder().aiMessage(message).build();
            }
        };
    }
}
