package org.pipelineframework.connector.llm.langchain4j;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.junit.jupiter.api.Test;

class OllamaRuntimeConfigurationTest {
    @Test
    void bindsConfiguredRuntimeControls() {
        OllamaRuntimeConfiguration configuration = configuration(Map.of(
            "pipeline.llm.langchain4j.ollama.request-timeout", "PT1M30S",
            "pipeline.llm.langchain4j.ollama.thinking", "false"));

        assertEquals(Duration.ofSeconds(90), configuration.requestTimeout());
        assertFalse(configuration.thinking());
    }

    @Test
    void retainsRuntimeControlDefaults() {
        OllamaRuntimeConfiguration configuration = configuration(Map.of());

        assertEquals(Duration.ofSeconds(30), configuration.requestTimeout());
        assertTrue(configuration.thinking());
    }

    private static OllamaRuntimeConfiguration configuration(Map<String, String> properties) {
        SmallRyeConfig config = new SmallRyeConfigBuilder()
            .withMapping(OllamaRuntimeConfiguration.class)
            .withSources(new MapConfigSource(properties))
            .build();
        return config.getConfigMapping(OllamaRuntimeConfiguration.class);
    }

    private record MapConfigSource(Map<String, String> values) implements ConfigSource {
        @Override
        public Map<String, String> getProperties() {
            return values;
        }

        @Override
        public Set<String> getPropertyNames() {
            return values.keySet();
        }

        @Override
        public String getValue(String propertyName) {
            return values.get(propertyName);
        }

        @Override
        public String getName() {
            return "test-ollama-runtime-config";
        }
    }
}
