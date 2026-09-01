package org.pipelineframework.connector.llm.langchain4j;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.pipelineframework.connector.ConnectionResolutionException;
import org.pipelineframework.connector.ConnectionResolver;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.llm.LlmDecisionClientResolver;
import org.pipelineframework.connector.llm.LlmProviderConfiguration;
import org.pipelineframework.connector.llm.LlmQueryConnectorProvider;

/** OpenAI-compatible LangChain4j adapter with host-selectable blocking and reactive clients. */
@ApplicationScoped
public final class LangChain4jOpenAiCompatibleQueryConnector extends LlmQueryConnectorProvider {
    public static final ConnectorProviderId PROVIDER_ID = ConnectorProviderId.of("llm.query.openai.compatible");
    private final RuntimeSettings runtimeSettings;
    private final OpenAiCompatibleClientManager clientManager;

    public LangChain4jOpenAiCompatibleQueryConnector() {
        this(RuntimeSettings.defaults(), OpenAiCompatibleClientManager.defaults());
    }

    @Inject
    LangChain4jOpenAiCompatibleQueryConnector(
        RuntimeConfiguration configuration,
        OpenAiCompatibleClientManager clientManager
    ) {
        this(
            new RuntimeSettings(configuration.requestTimeout(), configuration.clientImplementation()),
            clientManager);
    }

    LangChain4jOpenAiCompatibleQueryConnector(
        RuntimeSettings runtimeSettings,
        OpenAiCompatibleClientManager clientManager
    ) {
        super(PROVIDER_ID);
        this.runtimeSettings = Objects.requireNonNull(
            runtimeSettings, "OpenAI-compatible runtime settings must not be null");
        this.clientManager = Objects.requireNonNull(
            clientManager, "OpenAI-compatible client manager must not be null");
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
            return clientManager.resolve(
                runtimeSettings.clientImplementation(),
                resolver,
                reference,
                configuration,
                context,
                executionContext,
                runtimeSettings.requestTimeout());
        };
    }

    @ConfigMapping(prefix = "pipeline.llm.langchain4j.openai-compatible")
    interface RuntimeConfiguration {
        /** Maximum wall-clock time for one provider HTTP request. */
        @WithDefault("PT60S")
        Duration requestTimeout();

        /** Runtime client implementation: blocking or reactive. */
        @WithDefault("reactive")
        String clientImplementation();
    }

    record RuntimeSettings(Duration requestTimeout, String clientImplementation) {
        RuntimeSettings {
            Objects.requireNonNull(requestTimeout, "OpenAI-compatible request timeout must not be null");
            if (requestTimeout.isZero() || requestTimeout.isNegative()) {
                throw new IllegalArgumentException("OpenAI-compatible request timeout must be positive");
            }
            clientImplementation = Objects.requireNonNull(
                clientImplementation, "OpenAI-compatible client implementation must not be null");
            if (!clientImplementation.equals(BlockingOpenAiCompatibleClientImplementation.ID)
                && !clientImplementation.equals(ReactiveOpenAiCompatibleClientImplementation.ID)) {
                throw new IllegalArgumentException(
                    "OpenAI-compatible client implementation must be blocking or reactive");
            }
        }

        static RuntimeSettings defaults() {
            return new RuntimeSettings(
                Duration.ofSeconds(60),
                ReactiveOpenAiCompatibleClientImplementation.ID);
        }
    }
}
