package org.pipelineframework.connector.llm.langchain4j;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.exception.HttpException;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.StreamingChatModel;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.request.ResponseFormatType;
import dev.langchain4j.model.chat.request.json.JsonRawSchema;
import dev.langchain4j.model.chat.response.ChatResponse;
import dev.langchain4j.model.chat.response.StreamingChatResponseHandler;
import dev.langchain4j.model.openai.OpenAiStreamingChatModel;
import org.junit.jupiter.api.Test;
import org.pipelineframework.connector.ConnectionRef;
import org.pipelineframework.connector.ConnectionResolutionException;
import org.pipelineframework.connector.ConnectionResolutionRequest;
import org.pipelineframework.connector.ConnectionResolver;
import org.pipelineframework.connector.ConnectorExecutionContext;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.ResolvedConnection;
import org.pipelineframework.connector.llm.LlmDecisionClient;
import org.pipelineframework.connector.llm.LlmDecisionClientResolver;
import org.pipelineframework.connector.llm.LlmProviderConfiguration;
import org.pipelineframework.connector.llm.LlmProviderFailureException;
import org.pipelineframework.connector.llm.LlmToolDefinition;
import org.pipelineframework.connector.llm.LlmTurnRequest;
import org.pipelineframework.connector.llm.StructuredOutputSchemaMode;

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
            () -> new LangChain4jOpenAiCompatibleQueryConnector.RuntimeSettings(Duration.ZERO, "blocking"));
    }

    @Test
    void rejectsUnknownClientImplementations() {
        assertThrows(IllegalArgumentException.class,
            () -> new LangChain4jOpenAiCompatibleQueryConnector.RuntimeSettings(
                Duration.ofSeconds(60), "unknown"));
    }

    @Test
    void resolvesTenantAwareAuthenticatedConnectionsForEveryInvocation() {
        List<ConnectionResolutionRequest<?>> requests = new ArrayList<>();
        List<ModelSettings> models = new ArrayList<>();
        AtomicInteger generations = new AtomicInteger();
        ConnectionResolver resolver = new ConnectionResolver() {
            @Override
            public <C extends ResolvedConnection> CompletionStage<C> resolve(ConnectionResolutionRequest<C> request) {
                requests.add(request);
                int generation = generations.incrementAndGet();
                AuthenticatedOpenAiCompatibleConnection connection =
                    new AuthenticatedOpenAiCompatibleConnection(modelConfiguration -> {
                        models.add(new ModelSettings(modelConfiguration, generation));
                        return model();
                    });
                return CompletableFuture.completedStage(request.connectionType().cast(connection));
            }
        };
        var connector = new LangChain4jOpenAiCompatibleQueryConnector(
            new LangChain4jOpenAiCompatibleQueryConnector.RuntimeSettings(Duration.ofSeconds(75), "blocking"),
            OpenAiCompatibleClientManager.defaults());
        LlmProviderConfiguration configuration = new LlmProviderConfiguration(
            "google/gemini-3.1-flash-lite",
            Optional.of("https://openrouter.ai/api/v1"),
            Optional.of(new ConnectionRef("hosted-llm.primary")));
        LlmDecisionClientResolver clientResolver = connector.createClientResolver(
            configuration, runtimeContext(Optional.of(resolver)));

        ConnectorExecutionContext firstInvocation = invocation("tenant-a");
        ConnectorExecutionContext secondInvocation = invocation("tenant-b");
        LlmDecisionClient first = clientResolver.resolve(firstInvocation).toCompletableFuture().join();
        LlmDecisionClient second = clientResolver.resolve(secondInvocation).toCompletableFuture().join();

        assertInstanceOf(LangChain4jOllamaQueryConnector.LangChain4jDecisionClient.class, first);
        assertInstanceOf(LangChain4jOllamaQueryConnector.LangChain4jDecisionClient.class, second);
        assertNotSame(first, second);
        assertEquals(2, requests.size());
        assertEquals(List.of(firstInvocation, secondInvocation), requests.stream()
            .map(ConnectionResolutionRequest::invocationContext).toList());
        assertTrue(requests.stream().allMatch(request ->
            request.reference().equals(new ConnectionRef("hosted-llm.primary"))
                && request.connectionType() == AuthenticatedOpenAiCompatibleConnection.class));
        assertEquals(List.of(
            new ModelSettings(
                new AuthenticatedOpenAiCompatibleConnection.ModelConfiguration(
                    "https://openrouter.ai/api/v1", "google/gemini-3.1-flash-lite",
                    Duration.ofSeconds(75), 0, true),
                1),
            new ModelSettings(
                new AuthenticatedOpenAiCompatibleConnection.ModelConfiguration(
                    "https://openrouter.ai/api/v1", "google/gemini-3.1-flash-lite",
                    Duration.ofSeconds(75), 0, true),
                2)), models);
    }

    @Test
    void selectsTheReactiveImplementationByDefaultWithoutChangingProviderIdentity() {
        List<ConnectionResolutionRequest<?>> requests = new ArrayList<>();
        ConnectionResolver resolver = new ConnectionResolver() {
            @Override
            public <C extends ResolvedConnection> CompletionStage<C> resolve(ConnectionResolutionRequest<C> request) {
                requests.add(request);
                AuthenticatedOpenAiCompatibleReactiveConnection connection =
                    new AuthenticatedOpenAiCompatibleReactiveConnection(ignored -> streamingModel());
                return CompletableFuture.completedStage(request.connectionType().cast(connection));
            }
        };
        var connector = new LangChain4jOpenAiCompatibleQueryConnector();

        LlmDecisionClient client = connector.createClientResolver(
            new LlmProviderConfiguration(
                "provider-model", Optional.empty(), Optional.of(new ConnectionRef("hosted-llm.primary"))),
            runtimeContext(Optional.of(resolver)))
            .resolve(invocation("tenant-a")).toCompletableFuture().join();

        assertEquals("llm.query.openai.compatible", connector.id().value());
        assertInstanceOf(LangChain4jOllamaQueryConnector.LangChain4jDecisionClient.class, client);
        assertEquals(AuthenticatedOpenAiCompatibleReactiveConnection.class, requests.getFirst().connectionType());
    }

    @Test
    void usesTheQuarkusOpenAiStreamingModelBuilder() {
        assertTrue(OpenAiStreamingChatModel.builder().getClass().getName()
            .startsWith("io.quarkiverse.langchain4j.openai."));
    }

    @Test
    void classifiesHttpRequestTimeoutAsTimeout() {
        LlmProviderFailureException classified = LangChain4jOpenAiSupport.classifyProviderFailure(
            new HttpException(408, "timeout-body-must-not-leak"));

        assertEquals(LlmProviderFailureException.Kind.TEMPORARILY_UNAVAILABLE, classified.kind());
        assertEquals(LlmProviderFailureException.CODE_TIMEOUT, classified.outcomeCode());
        assertTrue(!classified.getMessage().contains("timeout-body-must-not-leak"));
    }

    @Test
    void classifiesReactiveProviderFailuresWithoutExposingProviderBodies() {
        ConnectionResolver resolver = new ConnectionResolver() {
            @Override
            public <C extends ResolvedConnection> CompletionStage<C> resolve(ConnectionResolutionRequest<C> request) {
                AuthenticatedOpenAiCompatibleReactiveConnection connection =
                    new AuthenticatedOpenAiCompatibleReactiveConnection(ignored -> new StreamingChatModel() {
                        @Override
                        public void doChat(ChatRequest chatRequest, StreamingChatResponseHandler handler) {
                            handler.onError(new HttpException(402, "credit-body-must-not-leak"));
                        }
                    });
                return CompletableFuture.completedStage(request.connectionType().cast(connection));
            }
        };
        var connector = new LangChain4jOpenAiCompatibleQueryConnector(
            new LangChain4jOpenAiCompatibleQueryConnector.RuntimeSettings(Duration.ofSeconds(75), "reactive"),
            OpenAiCompatibleClientManager.defaults());
        LlmDecisionClient client = connector.createClientResolver(
            new LlmProviderConfiguration(
                "provider-model", Optional.empty(), Optional.of(new ConnectionRef("hosted-llm.primary"))),
            runtimeContext(Optional.of(resolver)))
            .resolve(invocation("tenant-a")).toCompletableFuture().join();

        RuntimeException failure = assertThrows(RuntimeException.class, () -> client.decide(new LlmTurnRequest(
            "Answer once.", "{}", List.of(new LlmToolDefinition("complete", "Complete", "{}")),
            StructuredOutputSchemaMode.REQUIRED)).toCompletableFuture().join());

        LlmProviderFailureException classified = assertInstanceOf(
            LlmProviderFailureException.class, failure.getCause());
        assertEquals(LlmProviderFailureException.Kind.TERMINAL, classified.kind());
        assertEquals("llm-provider-quota-exhausted", classified.outcomeCode());
        assertTrue(!classified.getMessage().contains("credit-body-must-not-leak"));
    }

    @Test
    void classifiesBlockingProviderFailuresWithoutExposingProviderBodies() {
        ConnectionResolver resolver = new ConnectionResolver() {
            @Override
            public <C extends ResolvedConnection> CompletionStage<C> resolve(ConnectionResolutionRequest<C> request) {
                AuthenticatedOpenAiCompatibleConnection connection =
                    new AuthenticatedOpenAiCompatibleConnection(ignored -> new ChatModel() {
                        @Override
                        public ChatResponse doChat(ChatRequest chatRequest) {
                            throw new HttpException(429, "rate-limit-body-must-not-leak");
                        }
                    });
                return CompletableFuture.completedStage(request.connectionType().cast(connection));
            }
        };
        LlmDecisionClient client = blockingConnector()
            .createClientResolver(
                new LlmProviderConfiguration(
                    "provider-model", Optional.empty(), Optional.of(new ConnectionRef("hosted-llm.primary"))),
                runtimeContext(Optional.of(resolver)))
            .resolve(invocation("tenant-a")).toCompletableFuture().join();

        RuntimeException failure = assertThrows(RuntimeException.class, () -> client.decide(new LlmTurnRequest(
            "Answer once.", "{}", List.of(new LlmToolDefinition("complete", "Complete", "{}")),
            StructuredOutputSchemaMode.REQUIRED)).toCompletableFuture().join());

        LlmProviderFailureException classified = assertInstanceOf(
            LlmProviderFailureException.class, failure.getCause());
        assertEquals(LlmProviderFailureException.Kind.TEMPORARILY_UNAVAILABLE, classified.kind());
        assertEquals("llm-provider-rate-limited", classified.outcomeCode());
        assertTrue(!classified.getMessage().contains("rate-limit-body-must-not-leak"));
    }

    @Test
    void blockingExecutorRejectionSettlesTheDecisionStage() {
        ConnectionResolver resolver = new ConnectionResolver() {
            @Override
            public <C extends ResolvedConnection> CompletionStage<C> resolve(ConnectionResolutionRequest<C> request) {
                AuthenticatedOpenAiCompatibleConnection connection =
                    new AuthenticatedOpenAiCompatibleConnection(ignored -> model());
                return CompletableFuture.completedStage(request.connectionType().cast(connection));
            }
        };
        ConnectorRuntimeContext rejectingContext = ConnectorRuntimeContext.of(
            "test",
            ignored -> {
                throw new RejectedExecutionException("executor detail must not leak");
            },
            Clock.systemUTC(),
            Optional.of(resolver));
        LlmDecisionClient client = blockingConnector()
            .createClientResolver(
                new LlmProviderConfiguration(
                    "provider-model", Optional.empty(), Optional.of(new ConnectionRef("hosted-llm.primary"))),
                rejectingContext)
            .resolve(invocation("tenant-a")).toCompletableFuture().join();

        CompletionStage<?> decision = client.decide(new LlmTurnRequest(
            "Answer once.", "{}", List.of(new LlmToolDefinition("complete", "Complete", "{}")),
            StructuredOutputSchemaMode.REQUIRED));
        RuntimeException failure = assertThrows(RuntimeException.class, () -> decision.toCompletableFuture().join());

        LlmProviderFailureException classified = assertInstanceOf(
            LlmProviderFailureException.class, failure.getCause());
        assertEquals("llm-provider-failed", classified.outcomeCode());
        assertTrue(!classified.getMessage().contains("executor detail must not leak"));
    }

    @Test
    void preservesRequiredStructuredOutputOnTheOpenAiRequest() {
        AtomicReference<ChatRequest> observed = new AtomicReference<>();
        ConnectionResolver resolver = new ConnectionResolver() {
            @Override
            public <C extends ResolvedConnection> CompletionStage<C> resolve(ConnectionResolutionRequest<C> request) {
                AuthenticatedOpenAiCompatibleConnection connection = new AuthenticatedOpenAiCompatibleConnection(
                    ignored -> new ChatModel() {
                        @Override
                        public ChatResponse doChat(ChatRequest chatRequest) {
                            observed.set(chatRequest);
                            return ChatResponse.builder().aiMessage(AiMessage.from("{}")).build();
                        }
                    });
                return CompletableFuture.completedStage(request.connectionType().cast(connection));
            }
        };
        var connector = blockingConnector();
        LlmDecisionClient client = connector.createClientResolver(
            new LlmProviderConfiguration(
                "provider-model", Optional.empty(), Optional.of(new ConnectionRef("hosted-llm.primary"))),
            runtimeContext(Optional.of(resolver)))
            .resolve(invocation("tenant-a")).toCompletableFuture().join();
        String schema = """
            {"type":"object","properties":{"answer":{"type":"string"}},
             "required":["answer"],"additionalProperties":false}
            """;

        client.decide(new LlmTurnRequest("Answer once.", "{}", List.of(
            new LlmToolDefinition("complete", "Complete", schema)), StructuredOutputSchemaMode.REQUIRED))
            .toCompletableFuture().join();

        assertEquals(ResponseFormatType.JSON, observed.get().responseFormat().type());
        JsonRawSchema rawSchema = assertInstanceOf(
            JsonRawSchema.class, observed.get().responseFormat().jsonSchema().rootElement());
        assertEquals(schema, rawSchema.schema());
    }

    @Test
    void rejectsMissingConnectionConfigurationOrHostResolverBeforeInference() {
        var connector = new LangChain4jOpenAiCompatibleQueryConnector();

        ConnectionResolutionException missingReference = assertThrows(ConnectionResolutionException.class, () ->
            connector.createClientResolver(
                new LlmProviderConfiguration("provider-model", Optional.empty()),
                runtimeContext(Optional.of(failingResolver()))));
        ConnectionResolutionException missingResolver = assertThrows(ConnectionResolutionException.class, () ->
            connector.createClientResolver(
                new LlmProviderConfiguration(
                    "provider-model", Optional.empty(), Optional.of(new ConnectionRef("hosted-llm.primary"))),
                runtimeContext(Optional.empty())));

        assertTrue(missingReference.getMessage().contains("connection reference"));
        assertTrue(missingResolver.getMessage().contains("ConnectionResolver"));
    }

    @Test
    void rejectsInvocationWithoutTenantContextBeforeResolution() {
        AtomicInteger resolutions = new AtomicInteger();
        ConnectionResolver resolver = new ConnectionResolver() {
            @Override
            public <C extends ResolvedConnection> CompletionStage<C> resolve(ConnectionResolutionRequest<C> request) {
                resolutions.incrementAndGet();
                return CompletableFuture.failedStage(new AssertionError("resolver must not be called"));
            }
        };
        var connector = new LangChain4jOpenAiCompatibleQueryConnector();
        LlmDecisionClientResolver clientResolver = connector.createClientResolver(
            new LlmProviderConfiguration(
                "provider-model", Optional.empty(), Optional.of(new ConnectionRef("hosted-llm.primary"))),
            runtimeContext(Optional.of(resolver)));

        RuntimeException failure = assertThrows(RuntimeException.class, () ->
            clientResolver.resolve(ConnectorExecutionContext.empty()).toCompletableFuture().join());

        assertInstanceOf(ConnectionResolutionException.class, failure.getCause());
        assertEquals(0, resolutions.get());
    }

    private static ConnectorRuntimeContext runtimeContext(Optional<ConnectionResolver> resolver) {
        return ConnectorRuntimeContext.of("test", Runnable::run, Clock.systemUTC(), resolver);
    }

    private static LangChain4jOpenAiCompatibleQueryConnector blockingConnector() {
        return new LangChain4jOpenAiCompatibleQueryConnector(
            new LangChain4jOpenAiCompatibleQueryConnector.RuntimeSettings(Duration.ofSeconds(60), "blocking"),
            OpenAiCompatibleClientManager.defaults());
    }

    private static ConnectionResolver failingResolver() {
        return new ConnectionResolver() {
            @Override
            public <C extends ResolvedConnection> CompletionStage<C> resolve(ConnectionResolutionRequest<C> request) {
                return CompletableFuture.failedStage(new AssertionError("resolver must not be called"));
            }
        };
    }

    private static ConnectorExecutionContext invocation(String tenantId) {
        return new ConnectorExecutionContext(
            Optional.of(tenantId), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
            Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    private static ChatModel model() {
        return new ChatModel() {
            @Override
            public ChatResponse doChat(ChatRequest request) {
                return ChatResponse.builder().aiMessage(AiMessage.from("{}")).build();
            }
        };
    }

    private static StreamingChatModel streamingModel() {
        return new StreamingChatModel() {
            @Override
            public void doChat(ChatRequest request, StreamingChatResponseHandler handler) {
                handler.onCompleteResponse(ChatResponse.builder().aiMessage(AiMessage.from("{}")).build());
            }
        };
    }

    private record ModelSettings(
        AuthenticatedOpenAiCompatibleConnection.ModelConfiguration configuration,
        int generation
    ) {
    }
}
