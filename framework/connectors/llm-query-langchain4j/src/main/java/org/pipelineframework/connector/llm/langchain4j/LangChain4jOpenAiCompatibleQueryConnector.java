package org.pipelineframework.connector.llm.langchain4j;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

import dev.langchain4j.model.chat.Capability;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.openai.OpenAiChatModel;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.llm.LlmDecisionClient;
import org.pipelineframework.connector.llm.LlmProviderConfiguration;
import org.pipelineframework.connector.llm.LlmQueryConnectorProvider;

/** OpenAI-compatible LangChain4j adapter for hosted chat-completion providers. */
@ApplicationScoped
public final class LangChain4jOpenAiCompatibleQueryConnector extends LlmQueryConnectorProvider {
    public static final ConnectorProviderId PROVIDER_ID = ConnectorProviderId.of("llm.query.openai.compatible");
    private static final String DEFAULT_BASE_URL = "https://api.openai.com/v1";
    private final OpenAiModelFactory modelFactory;
    private final RuntimeSettings runtimeSettings;

    public LangChain4jOpenAiCompatibleQueryConnector() {
        this(defaultModelFactory(), RuntimeSettings.defaults());
    }

    @Inject
    LangChain4jOpenAiCompatibleQueryConnector(RuntimeConfiguration configuration) {
        this(defaultModelFactory(), new RuntimeSettings(configuration.apiKey(), configuration.requestTimeout()));
    }

    LangChain4jOpenAiCompatibleQueryConnector(
        OpenAiModelFactory modelFactory,
        RuntimeSettings runtimeSettings
    ) {
        super(PROVIDER_ID);
        this.modelFactory = Objects.requireNonNull(
            modelFactory, "OpenAI-compatible model factory must not be null");
        this.runtimeSettings = Objects.requireNonNull(
            runtimeSettings, "OpenAI-compatible runtime settings must not be null");
    }

    private static OpenAiModelFactory defaultModelFactory() {
        return (baseUrl, modelName, apiKey, timeout, maxRetries) -> OpenAiChatModel.builder()
            .baseUrl(baseUrl)
            .modelName(modelName)
            .apiKey(apiKey)
            .timeout(timeout)
            .maxRetries(maxRetries)
            .strictJsonSchema(true)
            .supportedCapabilities(Capability.RESPONSE_FORMAT_JSON_SCHEMA)
            .build();
    }

    @Override
    protected LlmDecisionClient createClient(
        LlmProviderConfiguration configuration,
        ConnectorRuntimeContext context
    ) {
        ChatModel model = modelFactory.create(
            configuration.baseUrl().orElse(DEFAULT_BASE_URL),
            configuration.model(),
            runtimeSettings.requiredApiKey(),
            runtimeSettings.requestTimeout(),
            0);
        return new LangChain4jOllamaQueryConnector.LangChain4jDecisionClient(model, context.executor());
    }

    @FunctionalInterface
    interface OpenAiModelFactory {
        ChatModel create(
            String baseUrl,
            String modelName,
            String apiKey,
            Duration timeout,
            int maxRetries);
    }

    @ConfigMapping(prefix = "pipeline.llm.langchain4j.openai-compatible")
    interface RuntimeConfiguration {
        /** Provider API key. Keep this value in runtime configuration, never in a pipeline binding. */
        Optional<String> apiKey();

        /** Maximum wall-clock time for one provider HTTP request. */
        @WithDefault("PT60S")
        Duration requestTimeout();
    }

    record RuntimeSettings(Optional<String> apiKey, Duration requestTimeout) {
        RuntimeSettings {
            apiKey = Objects.requireNonNull(apiKey, "OpenAI-compatible API key must not be null")
                .map(String::trim).filter(value -> !value.isEmpty());
            Objects.requireNonNull(requestTimeout, "OpenAI-compatible request timeout must not be null");
            if (requestTimeout.isZero() || requestTimeout.isNegative()) {
                throw new IllegalArgumentException("OpenAI-compatible request timeout must be positive");
            }
        }

        static RuntimeSettings defaults() {
            return new RuntimeSettings(Optional.empty(), Duration.ofSeconds(60));
        }

        String requiredApiKey() {
            return apiKey.orElseThrow(() -> new IllegalStateException(
                "OpenAI-compatible LLM adapter requires "
                    + "pipeline.llm.langchain4j.openai-compatible.api-key"));
        }
    }
}
