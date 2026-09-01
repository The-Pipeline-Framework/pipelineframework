package org.pipelineframework.connector.embedding.langchain4j;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

import dev.langchain4j.model.embedding.EmbeddingModel;
import org.pipelineframework.connector.ResolvedConnection;

/** Runtime-only authenticated Ollama embedding client factory. */
public final class AuthenticatedOllamaEmbeddingConnection implements ResolvedConnection {
    private final ModelFactory modelFactory;

    public AuthenticatedOllamaEmbeddingConnection(ModelFactory modelFactory) {
        this.modelFactory = Objects.requireNonNull(modelFactory, "Ollama embedding model factory must not be null");
    }

    EmbeddingModel createModel(ModelConfiguration configuration) {
        return Objects.requireNonNull(modelFactory.create(configuration),
            "authenticated Ollama embedding model factory returned null");
    }

    public record ModelConfiguration(
        String baseUrl,
        String model,
        Optional<Integer> dimensions,
        Duration requestTimeout
    ) {
        public ModelConfiguration {
            baseUrl = requireText(baseUrl, "Ollama embedding base URL");
            model = requireText(model, "Ollama embedding model");
            dimensions = Objects.requireNonNull(dimensions, "Ollama embedding dimensions must not be null");
            requestTimeout = Objects.requireNonNull(requestTimeout,
                "Ollama embedding request timeout must not be null");
        }

        private static String requireText(String value, String name) {
            String normalized = Objects.requireNonNull(value, name + " must not be null").trim();
            if (normalized.isEmpty()) throw new IllegalArgumentException(name + " must not be blank");
            return normalized;
        }
    }

    @FunctionalInterface
    public interface ModelFactory {
        EmbeddingModel create(ModelConfiguration configuration);
    }
}
