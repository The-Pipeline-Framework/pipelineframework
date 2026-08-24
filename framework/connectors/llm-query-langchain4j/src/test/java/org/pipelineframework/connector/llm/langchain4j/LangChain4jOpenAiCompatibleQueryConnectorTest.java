package org.pipelineframework.connector.llm.langchain4j;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.response.ChatResponse;
import org.junit.jupiter.api.Test;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.llm.LlmProviderConfiguration;

class LangChain4jOpenAiCompatibleQueryConnectorTest {
    @Test
    void hasADistinctProviderIdentityFromTheOllamaAdapter() {
        assertEquals("llm.query.openai.compatible",
            new LangChain4jOpenAiCompatibleQueryConnector().id().value());
        assertNotEquals(
            new LangChain4jOllamaQueryConnector().id(),
            new LangChain4jOpenAiCompatibleQueryConnector().id());
    }

    @Test
    void rejectsNonPositiveRequestTimeouts() {
        assertThrows(IllegalArgumentException.class,
            () -> new LangChain4jOpenAiCompatibleQueryConnector.RuntimeSettings(
                Optional.of("secret"), Duration.ZERO));
    }

    @Test
    void configuresModelIdentityFromTheBindingAndSecretFromRuntimeConfiguration() {
        AtomicReference<String> baseUrl = new AtomicReference<>();
        AtomicReference<String> modelName = new AtomicReference<>();
        AtomicReference<String> apiKey = new AtomicReference<>();
        AtomicReference<Duration> timeout = new AtomicReference<>();
        AtomicInteger maxRetries = new AtomicInteger(-1);
        ChatModel model = model();
        var connector = new LangChain4jOpenAiCompatibleQueryConnector(
            (configuredBaseUrl, configuredModel, configuredApiKey, configuredTimeout, configuredMaxRetries) -> {
                baseUrl.set(configuredBaseUrl);
                modelName.set(configuredModel);
                apiKey.set(configuredApiKey);
                timeout.set(configuredTimeout);
                maxRetries.set(configuredMaxRetries);
                return model;
            },
            new LangChain4jOpenAiCompatibleQueryConnector.RuntimeSettings(
                Optional.of("openrouter-secret"), Duration.ofSeconds(75)));

        assertInstanceOf(LangChain4jOllamaQueryConnector.LangChain4jDecisionClient.class,
            connector.createClient(
                new LlmProviderConfiguration(
                    "google/gemini-3.1-flash-lite",
                    Optional.of("https://openrouter.ai/api/v1")),
                ConnectorRuntimeContext.empty()));
        assertEquals("https://openrouter.ai/api/v1", baseUrl.get());
        assertEquals("google/gemini-3.1-flash-lite", modelName.get());
        assertEquals("openrouter-secret", apiKey.get());
        assertEquals(Duration.ofSeconds(75), timeout.get());
        assertEquals(0, maxRetries.get());
    }

    @Test
    void rejectsStartupWithoutARuntimeApiKey() {
        var connector = new LangChain4jOpenAiCompatibleQueryConnector(
            (baseUrl, modelName, apiKey, timeout, maxRetries) -> model(),
            LangChain4jOpenAiCompatibleQueryConnector.RuntimeSettings.defaults());

        IllegalStateException failure = assertThrows(IllegalStateException.class,
            () -> connector.createClient(
                new LlmProviderConfiguration(
                    "google/gemini-3.1-flash-lite",
                    Optional.of("https://openrouter.ai/api/v1")),
                ConnectorRuntimeContext.empty()));

        assertTrue(failure.getMessage().contains("openai-compatible.api-key"));
    }

    private static ChatModel model() {
        return new ChatModel() {
            @Override
            public ChatResponse doChat(ChatRequest request) {
                return ChatResponse.builder().aiMessage(AiMessage.from("{}")).build();
            }
        };
    }
}
