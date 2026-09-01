package org.pipelineframework.connector.llm.langchain4j;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import dev.langchain4j.model.chat.StreamingChatModel;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.response.ChatResponse;
import dev.langchain4j.model.chat.response.StreamingChatResponseHandler;
import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.connector.ConnectionRef;
import org.pipelineframework.connector.ConnectionResolutionRequest;
import org.pipelineframework.connector.ConnectionResolver;
import org.pipelineframework.connector.ConnectorExecutionContext;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.llm.LlmDecisionClient;
import org.pipelineframework.connector.llm.LlmProviderConfiguration;

@ApplicationScoped
final class ReactiveOpenAiCompatibleClientImplementation implements OpenAiCompatibleClientImplementation {
    static final String ID = "reactive";
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
        CompletionStage<AuthenticatedOpenAiCompatibleReactiveConnection> stage = resolver.resolve(
            new ConnectionResolutionRequest<>(
                reference,
                AuthenticatedOpenAiCompatibleReactiveConnection.class,
                executionContext));
        return Objects.requireNonNull(stage, "host ConnectionResolver returned a null stage")
            .thenApply(connection -> decisionClient(connection, configuration, requestTimeout));
    }

    private static LlmDecisionClient decisionClient(
        AuthenticatedOpenAiCompatibleReactiveConnection connection,
        LlmProviderConfiguration configuration,
        Duration requestTimeout
    ) {
        StreamingChatModel model = connection.createModel(
            configuration.baseUrl().orElse(DEFAULT_BASE_URL),
            configuration.model(),
            requestTimeout);
        Objects.requireNonNull(model, "OpenAI-compatible reactive model must not be null");
        return new LangChain4jOllamaQueryConnector.LangChain4jDecisionClient(
            (request, responseSchema) -> chat(
                model,
                LangChain4jOpenAiSupport.withResponseSchema(request, responseSchema)));
    }

    private static CompletionStage<ChatResponse> chat(StreamingChatModel model, ChatRequest request) {
        CompletableFuture<ChatResponse> response = new CompletableFuture<>();
        try {
            model.chat(request, new StreamingChatResponseHandler() {
                @Override
                public void onPartialResponse(String partialResponse) {
                    // The provider client aggregates partials into the final ChatResponse.
                }

                @Override
                public void onCompleteResponse(ChatResponse completeResponse) {
                    response.complete(completeResponse);
                }

                @Override
                public void onError(Throwable error) {
                    response.completeExceptionally(LangChain4jOpenAiSupport.classifyProviderFailure(error));
                }
            });
        } catch (RuntimeException failure) {
            response.completeExceptionally(LangChain4jOpenAiSupport.classifyProviderFailure(failure));
        }
        return response;
    }
}
