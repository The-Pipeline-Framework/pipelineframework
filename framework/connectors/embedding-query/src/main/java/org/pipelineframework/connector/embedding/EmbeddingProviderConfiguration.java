package org.pipelineframework.connector.embedding;

import java.util.Objects;
import java.util.Optional;

import org.pipelineframework.connector.ConnectionRef;

/** Binding-owned model identity and portable dimension override. */
public record EmbeddingProviderConfiguration(
    String model,
    Optional<Integer> dimensions,
    Optional<ConnectionRef> connection
) {
    public EmbeddingProviderConfiguration {
        model = EmbeddingValues.requireText(model, "embedding model");
        dimensions = Objects.requireNonNull(dimensions, "embedding dimensions must not be null");
        dimensions.ifPresent(value -> {
            if (value <= 0) {
                throw new IllegalArgumentException("embedding dimensions must be positive");
            }
        });
        connection = Objects.requireNonNull(connection, "embedding connection must not be null");
    }
}
