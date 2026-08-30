package org.pipelineframework.connector.llm;

import java.util.Objects;
import java.util.Optional;

import org.pipelineframework.connector.ConnectionRef;

/** Binding-owned, adapter-neutral model selection. */
public record LlmProviderConfiguration(
    String model,
    Optional<String> baseUrl,
    Optional<ConnectionRef> connection
) {
    public LlmProviderConfiguration {
        model = Objects.requireNonNull(model, "LLM model must not be null").trim();
        if (model.isEmpty()) {
            throw new IllegalArgumentException("LLM model must not be blank");
        }
        baseUrl = Objects.requireNonNull(baseUrl, "LLM base URL must not be null")
            .map(String::trim).filter(value -> !value.isEmpty());
        connection = Objects.requireNonNull(connection, "LLM connection reference must not be null");
    }

    public LlmProviderConfiguration(String model, Optional<String> baseUrl) {
        this(model, baseUrl, Optional.empty());
    }
}
