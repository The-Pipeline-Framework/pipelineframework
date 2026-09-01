package org.pipelineframework.connector.embedding.langchain4j;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

import dev.langchain4j.http.client.jdk.JdkHttpClient;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.ollama.OllamaEmbeddingModel;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.pipelineframework.connector.ConnectionResolutionException;
import org.pipelineframework.connector.ConnectionResolutionRequest;
import org.pipelineframework.connector.ConnectionResolver;
import org.pipelineframework.connector.ConnectorConfigSchema;
import org.pipelineframework.connector.ConnectorExecutionContext;
import org.pipelineframework.connector.ConnectorOperation;
import org.pipelineframework.connector.ConnectorProvider;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorProviderVersion;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.QueryInvocation;
import org.pipelineframework.connector.QueryObservation;
import org.pipelineframework.connector.QueryOutcome;
import org.pipelineframework.connector.embedding.EmbeddingProviderConfiguration;
import org.pipelineframework.connector.embedding.EmbeddingQueryOperation;
import org.pipelineframework.connector.embedding.EmbeddingRequest;
import org.pipelineframework.connector.embedding.EmbeddingResult;

/** Production Ollama adapter for one portable embedding Query. */
@ApplicationScoped
public final class LangChain4jOllamaEmbeddingConnector
    implements ConnectorProvider<EmbeddingProviderConfiguration> {
    public static final ConnectorProviderId PROVIDER_ID = ConnectorProviderId.of("embedding.query.ollama");
    private static final ConnectorConfigSchema<EmbeddingProviderConfiguration> CONFIGURATION =
        ConnectorConfigSchema.record(EmbeddingProviderConfiguration.class, "embedding.query.ollama.provider", 1);

    private final ModelFactory modelFactory;
    private final RuntimeSettings runtimeSettings;
    private final AtomicReference<ClientResolver> resolver = new AtomicReference<>();
    private final AtomicReference<Executor> executor = new AtomicReference<>();
    private final AtomicReference<Optional<Integer>> configuredDimensions = new AtomicReference<>(Optional.empty());
    private final AtomicReference<String> configuredModel = new AtomicReference<>();
    private final EmbeddingQueryOperation operation = new Operation();

    public LangChain4jOllamaEmbeddingConnector() {
        this(defaultModelFactory(), RuntimeSettings.defaults());
    }

    @Inject
    LangChain4jOllamaEmbeddingConnector(OllamaEmbeddingRuntimeConfiguration configuration) {
        this(defaultModelFactory(), new RuntimeSettings(configuration.baseUrl(), configuration.requestTimeout()));
    }

    LangChain4jOllamaEmbeddingConnector(ModelFactory modelFactory, RuntimeSettings runtimeSettings) {
        this.modelFactory = Objects.requireNonNull(modelFactory, "embedding model factory must not be null");
        this.runtimeSettings = Objects.requireNonNull(runtimeSettings, "embedding runtime settings must not be null");
    }

    @Override public ConnectorProviderId id() { return PROVIDER_ID; }
    @Override public ConnectorProviderVersion version() { return new ConnectorProviderVersion(1, 0); }
    @Override public Optional<ConnectorConfigSchema<EmbeddingProviderConfiguration>> configurationSchema() {
        return Optional.of(CONFIGURATION);
    }
    @Override public Collection<? extends ConnectorOperation> operations() { return List.of(operation); }

    @Override
    public CompletionStage<Void> start(ConnectorRuntimeContext context, EmbeddingProviderConfiguration configuration) {
        try {
            executor.set(context.executor());
            configuredDimensions.set(configuration.dimensions());
            configuredModel.set(configuration.model());
            resolver.set(clientResolver(context, configuration));
            return CompletableFuture.completedStage(null);
        } catch (RuntimeException failure) {
            return CompletableFuture.failedStage(failure);
        }
    }

    @Override
    public CompletionStage<Void> stop(ConnectorRuntimeContext context) {
        resolver.set(null);
        executor.set(null);
        configuredDimensions.set(Optional.empty());
        configuredModel.set(null);
        return CompletableFuture.completedStage(null);
    }

    private ClientResolver clientResolver(
        ConnectorRuntimeContext context,
        EmbeddingProviderConfiguration configuration
    ) {
        if (configuration.connection().isEmpty()) {
            EmbeddingModel model = modelFactory.create(modelConfiguration(configuration));
            return ignored -> CompletableFuture.completedStage(model);
        }
        ConnectionResolver connectionResolver = context.connectionResolver().orElseThrow(() ->
            new ConnectionResolutionException(
                "Ollama embedding binding has a connection reference but no host ConnectionResolver"));
        return executionContext -> {
            if (executionContext.tenantId().isEmpty()) {
                return CompletableFuture.failedStage(new ConnectionResolutionException(
                    "Ollama embedding connection resolution requires a tenant-aware invocation context"));
            }
            return connectionResolver.resolve(new ConnectionResolutionRequest<>(
                configuration.connection().orElseThrow(),
                AuthenticatedOllamaEmbeddingConnection.class,
                executionContext)).thenApply(connection -> connection.createModel(modelConfiguration(configuration)));
        };
    }

    private AuthenticatedOllamaEmbeddingConnection.ModelConfiguration modelConfiguration(
        EmbeddingProviderConfiguration configuration
    ) {
        return new AuthenticatedOllamaEmbeddingConnection.ModelConfiguration(
            runtimeSettings.baseUrl(), configuration.model(), configuration.dimensions(), runtimeSettings.requestTimeout());
    }

    private static ModelFactory defaultModelFactory() {
        return configuration -> {
            var builder = OllamaEmbeddingModel.builder()
                .baseUrl(configuration.baseUrl())
                .modelName(configuration.model())
                .timeout(configuration.requestTimeout())
                .httpClientBuilder(JdkHttpClient.builder())
                .maxRetries(0);
            configuration.dimensions().ifPresent(builder::dimensions);
            return builder.build();
        };
    }

    private final class Operation implements EmbeddingQueryOperation {
        @Override
        public CompletionStage<QueryOutcome<EmbeddingResult>> query(
            QueryInvocation<EmbeddingRequest, org.pipelineframework.connector.ConnectorConfigurationDocument,
                EmbeddingResult> invocation
        ) {
            ClientResolver active = resolver.get();
            Executor activeExecutor = executor.get();
            if (active == null || activeExecutor == null) {
                return CompletableFuture.failedStage(new IllegalStateException("Ollama embedding binding is not active"));
            }
            return active.resolve(invocation.executionContext()).thenApplyAsync(model -> {
                List<Float> values = List.copyOf(model.embed(invocation.input().text()).content().vectorAsList());
                validateVector(values);
                return new QueryOutcome.Found<>(new EmbeddingResult(
                    invocation.input().itemId(), invocation.input().text(), values),
                    Optional.of(QueryObservation.live(
                        Optional.empty(), Optional.of(configuredModel.get()), Optional.empty())));
            }, activeExecutor);
        }

        private void validateVector(List<Float> values) {
            if (values.isEmpty()) throw new IllegalArgumentException("Ollama embedding vector must not be empty");
            for (Float value : values) {
                if (value == null || !Float.isFinite(value)) {
                    throw new IllegalArgumentException("Ollama embedding vector must contain only finite values");
                }
            }
            resolverDimensions().ifPresent(expected -> {
                if (values.size() != expected) {
                    throw new IllegalArgumentException(
                        "Ollama embedding dimensions must be " + expected + " but were " + values.size());
                }
            });
        }
    }

    private Optional<Integer> resolverDimensions() {
        return configuredDimensions.get();
    }

    @FunctionalInterface
    interface ClientResolver {
        CompletionStage<EmbeddingModel> resolve(ConnectorExecutionContext executionContext);
    }

    @FunctionalInterface
    interface ModelFactory {
        EmbeddingModel create(AuthenticatedOllamaEmbeddingConnection.ModelConfiguration configuration);
    }

    record RuntimeSettings(String baseUrl, Duration requestTimeout) {
        RuntimeSettings {
            baseUrl = Objects.requireNonNull(baseUrl, "Ollama embedding base URL must not be null").trim();
            if (baseUrl.isEmpty()) throw new IllegalArgumentException("Ollama embedding base URL must not be blank");
            requestTimeout = Objects.requireNonNull(requestTimeout,
                "Ollama embedding request timeout must not be null");
            if (requestTimeout.isZero() || requestTimeout.isNegative()) {
                throw new IllegalArgumentException("Ollama embedding request timeout must be positive");
            }
        }

        static RuntimeSettings defaults() {
            return new RuntimeSettings("http://localhost:11434", Duration.ofSeconds(30));
        }
    }
}
