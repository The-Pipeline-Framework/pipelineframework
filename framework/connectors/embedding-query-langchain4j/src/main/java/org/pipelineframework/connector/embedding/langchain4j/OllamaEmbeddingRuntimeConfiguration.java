package org.pipelineframework.connector.embedding.langchain4j;

import java.time.Duration;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/** Runtime-owned endpoint and client tuning for Ollama embedding access. */
@ConfigMapping(prefix = "pipeline.embedding.langchain4j.ollama")
public interface OllamaEmbeddingRuntimeConfiguration {
    /** Ollama HTTP endpoint resolved by the deployment host. */
    @WithDefault("http://localhost:11434")
    String baseUrl();

    /** Maximum duration allowed for one embedding request. */
    @WithDefault("PT30S")
    Duration requestTimeout();
}
