package org.pipelineframework.config;

import java.util.Map;
import java.util.Set;
import io.smallrye.config.ConfigValidationException;
import io.smallrye.config.SmallRyeConfigBuilder;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class CallbackPolicyConfigTest {
    @Test
    void defaultsToHttpsAndAcceptsTheExplicitLocalPolicyUnderStrictMapping() {
        assertFalse(mapping(Map.of()).callback().allowHttp());
        assertTrue(mapping(Map.of("pipeline.callback.allow-http", "true")).callback().allowHttp());
    }

    @Test
    void misspelledCallbackPolicyStillFailsStrictValidation() {
        assertThrows(ConfigValidationException.class,
            () -> mapping(Map.of("pipeline.callback.allow-htp", "true")));
    }

    private PipelineStepConfig mapping(Map<String, String> values) {
        return new SmallRyeConfigBuilder().withMapping(PipelineStepConfig.class)
            .withSources(new Properties(values)).build().getConfigMapping(PipelineStepConfig.class);
    }

    private record Properties(Map<String, String> values) implements ConfigSource {
        public Map<String, String> getProperties() { return values; }
        public Set<String> getPropertyNames() { return values.keySet(); }
        public String getValue(String name) { return values.get(name); }
        public String getName() { return "callback-policy-test"; }
    }
}
