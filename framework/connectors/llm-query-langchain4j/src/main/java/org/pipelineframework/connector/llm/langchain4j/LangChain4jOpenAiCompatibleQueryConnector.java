package org.pipelineframework.connector.llm.langchain4j;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.request.ResponseFormat;
import dev.langchain4j.model.chat.request.ResponseFormatType;
import dev.langchain4j.model.chat.request.json.JsonRawSchema;
import dev.langchain4j.model.chat.request.json.JsonSchema;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.pipelineframework.connector.ConnectionResolutionException;
import org.pipelineframework.connector.ConnectionResolutionRequest;
import org.pipelineframework.connector.ConnectionResolver;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.llm.LlmDecisionClient;
import org.pipelineframework.connector.llm.LlmDecisionClientResolver;
import org.pipelineframework.connector.llm.LlmProviderConfiguration;
import org.pipelineframework.connector.llm.LlmQueryConnectorProvider;

/** OpenAI-compatible LangChain4j adapter for hosted chat-completion providers. */
@ApplicationScoped
public final class LangChain4jOpenAiCompatibleQueryConnector extends LlmQueryConnectorProvider {
    public static final ConnectorProviderId PROVIDER_ID = ConnectorProviderId.of("llm.query.openai.compatible");
    private static final String DEFAULT_BASE_URL = "https://api.openai.com/v1";
    private final RuntimeSettings runtimeSettings;

    public LangChain4jOpenAiCompatibleQueryConnector() {
        this(RuntimeSettings.defaults());
    }

    @Inject
    LangChain4jOpenAiCompatibleQueryConnector(RuntimeConfiguration configuration) {
        this(new RuntimeSettings(configuration.requestTimeout()));
    }

    LangChain4jOpenAiCompatibleQueryConnector(RuntimeSettings runtimeSettings) {
        super(PROVIDER_ID);
        this.runtimeSettings = Objects.requireNonNull(
            runtimeSettings, "OpenAI-compatible runtime settings must not be null");
    }

    @Override
    protected LlmDecisionClientResolver createClientResolver(
        LlmProviderConfiguration configuration,
        ConnectorRuntimeContext context
    ) {
        var reference = configuration.connection().orElseThrow(() ->
            new ConnectionResolutionException(
                "OpenAI-compatible LLM binding requires a deployment-owned connection reference"));
        ConnectionResolver resolver = context.connectionResolver().orElseThrow(() ->
            new ConnectionResolutionException(
                "No host ConnectionResolver is configured for the OpenAI-compatible LLM binding"));
        return executionContext -> {
            if (executionContext.tenantId().isEmpty()) {
                return CompletableFuture.failedStage(new ConnectionResolutionException(
                    "OpenAI-compatible LLM connection resolution requires a tenant-aware invocation context"));
            }
            CompletionStage<AuthenticatedOpenAiCompatibleConnection> stage = resolver.resolve(
                new ConnectionResolutionRequest<>(
                    reference,
                    AuthenticatedOpenAiCompatibleConnection.class,
                    executionContext));
            return Objects.requireNonNull(stage, "host ConnectionResolver returned a null stage")
                .thenApply(connection -> decisionClient(connection, configuration, context));
        };
    }

    private LlmDecisionClient decisionClient(
        AuthenticatedOpenAiCompatibleConnection connection,
        LlmProviderConfiguration configuration,
        ConnectorRuntimeContext context
    ) {
        ChatModel model = connection.createModel(
            configuration.baseUrl().orElse(DEFAULT_BASE_URL),
            configuration.model(),
            runtimeSettings.requestTimeout(),
            0);
        Objects.requireNonNull(model, "OpenAI-compatible model must not be null");
        return new LangChain4jOllamaQueryConnector.LangChain4jDecisionClient(
            (request, responseSchema) -> CompletableFuture.supplyAsync(
                () -> model.chat(withResponseSchema(request, responseSchema)), context.executor()));
    }

    private static ChatRequest withResponseSchema(ChatRequest request, Optional<String> responseSchema) {
        if (responseSchema.isEmpty()) {
            return request;
        }
        ResponseFormat format = ResponseFormat.builder()
            .type(ResponseFormatType.JSON)
            .jsonSchema(JsonSchema.builder()
                .name("tpf_completion")
                .rootElement(JsonRawSchema.from(responseSchema.orElseThrow()))
                .build())
            .build();
        return new ChatRequest.Builder(request).responseFormat(format).build();
    }

    @ConfigMapping(prefix = "pipeline.llm.langchain4j.openai-compatible")
    interface RuntimeConfiguration {
        /** Maximum wall-clock time for one provider HTTP request. */
        @WithDefault("PT60S")
        Duration requestTimeout();
    }

    record RuntimeSettings(Duration requestTimeout) {
        RuntimeSettings {
            Objects.requireNonNull(requestTimeout, "OpenAI-compatible request timeout must not be null");
            if (requestTimeout.isZero() || requestTimeout.isNegative()) {
                throw new IllegalArgumentException("OpenAI-compatible request timeout must be positive");
            }
        }

        static RuntimeSettings defaults() {
            return new RuntimeSettings(Duration.ofSeconds(60));
        }
    }
}
