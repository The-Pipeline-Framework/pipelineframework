package org.pipelineframework.connector.llm.langchain4j;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import dev.langchain4j.agent.tool.ToolExecutionRequest;
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.ImageContent;
import dev.langchain4j.data.message.PdfFileContent;
import dev.langchain4j.data.message.TextContent;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.response.ChatResponse;
import org.pipelineframework.connector.llm.LlmToolDefinition;
import org.pipelineframework.connector.llm.LlmProviderConfiguration;
import org.pipelineframework.connector.llm.LlmTurnRequest;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.MaterializedPayload;
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
            connector.createClient(
                new LlmProviderConfiguration("qwen3", Optional.of("http://ollama.internal:11434")),
                ConnectorRuntimeContext.empty()));
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

        connector.createClient(new LlmProviderConfiguration("qwen3", Optional.empty()),
            ConnectorRuntimeContext.empty());

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
                    """))))
            .toCompletableFuture().join();

        assertEquals("charge", proposal.alias());
        assertEquals("{\"amount\":42}", proposal.argumentsJson());
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
                return ChatResponse.builder().aiMessage(AiMessage.from(ToolExecutionRequest.builder()
                    .name("complete").arguments("{}").build())).build();
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
                return ChatResponse.builder().aiMessage(AiMessage.from(ToolExecutionRequest.builder()
                    .name("complete").arguments("{}").build())).build();
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
                """))))
            .toCompletableFuture().join();
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
