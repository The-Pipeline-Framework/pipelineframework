package org.pipelineframework.connector.llm.langchain4j;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.time.Duration;

/** Runtime-owned Ollama tuning supplied through ordinary application properties. */
@ConfigMapping(prefix = "pipeline.llm.langchain4j.ollama")
public interface OllamaRuntimeConfiguration {
    /** Maximum wall-clock time for one Ollama HTTP request. */
    @WithDefault("PT30S")
    Duration requestTimeout();

    /** Whether Ollama should enable model thinking/reasoning output. */
    @WithDefault("true")
    boolean thinking();
}
