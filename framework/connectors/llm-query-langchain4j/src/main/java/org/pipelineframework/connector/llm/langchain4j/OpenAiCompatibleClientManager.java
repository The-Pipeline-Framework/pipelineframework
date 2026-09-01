package org.pipelineframework.connector.llm.langchain4j;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.pipelineframework.connector.ConnectionRef;
import org.pipelineframework.connector.ConnectionResolver;
import org.pipelineframework.connector.ConnectorExecutionContext;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.llm.LlmDecisionClient;
import org.pipelineframework.connector.llm.LlmProviderConfiguration;

/** Selects one runtime implementation without changing the connector or pipeline contract. */
@ApplicationScoped
final class OpenAiCompatibleClientManager {
    private final Map<String, OpenAiCompatibleClientImplementation> implementations;

    @Inject
    OpenAiCompatibleClientManager(Instance<OpenAiCompatibleClientImplementation> implementations) {
        this(implementations.stream().toList());
    }

    OpenAiCompatibleClientManager(List<OpenAiCompatibleClientImplementation> implementations) {
        Objects.requireNonNull(implementations, "OpenAI-compatible client implementations must not be null");
        Map<String, OpenAiCompatibleClientImplementation> discovered = new LinkedHashMap<>();
        for (OpenAiCompatibleClientImplementation implementation : implementations) {
            OpenAiCompatibleClientImplementation previous = discovered.putIfAbsent(
                Objects.requireNonNull(implementation.id(), "client implementation id must not be null"),
                implementation);
            if (previous != null) {
                throw new IllegalStateException(
                    "Duplicate OpenAI-compatible client implementation: " + implementation.id());
            }
        }
        this.implementations = Map.copyOf(discovered);
    }

    static OpenAiCompatibleClientManager defaults() {
        return new OpenAiCompatibleClientManager(List.of(
            new BlockingOpenAiCompatibleClientImplementation(),
            new ReactiveOpenAiCompatibleClientImplementation()));
    }

    CompletionStage<LlmDecisionClient> resolve(
        String implementationId,
        ConnectionResolver resolver,
        ConnectionRef reference,
        LlmProviderConfiguration configuration,
        ConnectorRuntimeContext runtimeContext,
        ConnectorExecutionContext executionContext,
        Duration requestTimeout
    ) {
        OpenAiCompatibleClientImplementation implementation = implementations.get(implementationId);
        if (implementation == null) {
            throw new IllegalStateException(
                "OpenAI-compatible client implementation is unavailable: " + implementationId);
        }
        return implementation.resolve(
            resolver,
            reference,
            configuration,
            runtimeContext,
            executionContext,
            requestTimeout);
    }
}
