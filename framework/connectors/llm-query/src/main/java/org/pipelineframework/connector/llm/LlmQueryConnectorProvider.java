package org.pipelineframework.connector.llm;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

import org.pipelineframework.connector.ConnectorConfigSchema;
import org.pipelineframework.connector.ConnectorOperation;
import org.pipelineframework.connector.ConnectorProvider;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorProviderVersion;
import org.pipelineframework.connector.ConnectorRuntimeContext;

/** Stable TPF provider surface shared by low-level LLM framework adapters. */
public abstract class LlmQueryConnectorProvider implements ConnectorProvider<LlmProviderConfiguration> {
    public static final ConnectorProviderId PROVIDER_ID = ConnectorProviderId.of("llm.query");
    private static final ConnectorConfigSchema<LlmProviderConfiguration> PROVIDER_SCHEMA =
        ConnectorConfigSchema.record(LlmProviderConfiguration.class, "llm.query.provider", 2);

    private final ConnectorProviderId providerId;
    private final AtomicReference<LlmDecisionClientResolver> clientResolver = new AtomicReference<>();
    private final LlmQueryOperation operation = new LlmQueryOperation(this::resolveClient);

    protected LlmQueryConnectorProvider() {
        this(PROVIDER_ID);
    }

    protected LlmQueryConnectorProvider(ConnectorProviderId providerId) {
        this.providerId = java.util.Objects.requireNonNull(providerId, "LLM provider ID must not be null");
    }

    @Override
    public final ConnectorProviderId id() {
        return providerId;
    }

    @Override
    public final ConnectorProviderVersion version() {
        return new ConnectorProviderVersion(1, 0);
    }

    @Override
    public final Optional<ConnectorConfigSchema<LlmProviderConfiguration>> configurationSchema() {
        return Optional.of(PROVIDER_SCHEMA);
    }

    @Override
    public final Collection<? extends ConnectorOperation> operations() {
        return List.of(operation);
    }

    @Override
    public final CompletionStage<Void> start(ConnectorRuntimeContext context, LlmProviderConfiguration configuration) {
        try {
            clientResolver.set(java.util.Objects.requireNonNull(
                createClientResolver(configuration, context),
                "LLM decision client resolver must not be null"));
            return CompletableFuture.completedFuture(null);
        } catch (RuntimeException failure) {
            return CompletableFuture.failedStage(failure);
        }
    }

    @Override
    public final CompletionStage<Void> stop(ConnectorRuntimeContext context) {
        clientResolver.set(null);
        return CompletableFuture.completedFuture(null);
    }

    /** Construct the adapter-specific client resolver for this configured binding. */
    protected abstract LlmDecisionClientResolver createClientResolver(
        LlmProviderConfiguration configuration,
        ConnectorRuntimeContext context
    );

    private CompletionStage<LlmDecisionClient> resolveClient(
        org.pipelineframework.connector.ConnectorExecutionContext executionContext
    ) {
        LlmDecisionClientResolver active = clientResolver.get();
        if (active == null) {
            return CompletableFuture.failedStage(new IllegalStateException("LLM Query binding is not active"));
        }
        try {
            return java.util.Objects.requireNonNull(
                active.resolve(executionContext), "LLM decision client resolver returned a null stage");
        } catch (RuntimeException failure) {
            return CompletableFuture.failedStage(failure);
        }
    }
}
