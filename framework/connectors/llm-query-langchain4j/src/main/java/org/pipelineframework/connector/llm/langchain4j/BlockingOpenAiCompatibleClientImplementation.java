package org.pipelineframework.connector.llm.langchain4j;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.response.ChatResponse;
import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.connector.ConnectionRef;
import org.pipelineframework.connector.ConnectionResolutionRequest;
import org.pipelineframework.connector.ConnectionResolver;
import org.pipelineframework.connector.ConnectorExecutionContext;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.llm.LlmDecisionClient;
import org.pipelineframework.connector.llm.LlmProviderConfiguration;

@ApplicationScoped
final class BlockingOpenAiCompatibleClientImplementation implements OpenAiCompatibleClientImplementation {
    static final String ID = "blocking";
    private static final String DEFAULT_BASE_URL = "https://api.openai.com/v1";

    @Override
    public String id() {
        return ID;
    }

    @Override
    public CompletionStage<LlmDecisionClient> resolve(
        ConnectionResolver resolver,
        ConnectionRef reference,
        LlmProviderConfiguration configuration,
        ConnectorRuntimeContext runtimeContext,
        ConnectorExecutionContext executionContext,
        Duration requestTimeout
    ) {
        CompletionStage<AuthenticatedOpenAiCompatibleConnection> stage = resolver.resolve(
            new ConnectionResolutionRequest<>(
                reference,
                AuthenticatedOpenAiCompatibleConnection.class,
                executionContext));
        return Objects.requireNonNull(stage, "host ConnectionResolver returned a null stage")
            .thenApply(connection -> decisionClient(connection, configuration, runtimeContext, requestTimeout));
    }

    private static LlmDecisionClient decisionClient(
        AuthenticatedOpenAiCompatibleConnection connection,
        LlmProviderConfiguration configuration,
        ConnectorRuntimeContext runtimeContext,
        Duration requestTimeout
    ) {
        ChatModel model = connection.createModel(
            configuration.baseUrl().orElse(DEFAULT_BASE_URL),
            configuration.model(),
            requestTimeout,
            0);
        Objects.requireNonNull(model, "OpenAI-compatible model must not be null");
        return new LangChain4jOllamaQueryConnector.LangChain4jDecisionClient(
            (request, responseSchema) -> chat(
                model,
                LangChain4jOpenAiSupport.withResponseSchema(request, responseSchema),
                runtimeContext));
    }

    private static CompletionStage<ChatResponse> chat(
        ChatModel model,
        ChatRequest request,
        ConnectorRuntimeContext runtimeContext
    ) {
        CompletableFuture<ChatResponse> response = new CompletableFuture<>();
        try {
            runtimeContext.executor().execute(() -> {
                try {
                    response.complete(model.chat(request));
                } catch (Throwable failure) {
                    response.completeExceptionally(LangChain4jOpenAiSupport.classifyProviderFailure(failure));
                }
            });
        } catch (RuntimeException rejection) {
            response.completeExceptionally(LangChain4jOpenAiSupport.classifyProviderFailure(rejection));
        }
        return response;
    }
}
