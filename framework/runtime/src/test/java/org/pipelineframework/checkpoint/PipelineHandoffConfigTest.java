package org.pipelineframework.checkpoint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import java.util.Map;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.junit.jupiter.api.Test;

class PipelineHandoffConfigTest {

    @Test
    void configReadsHttpBinding() {
        PipelineHandoffConfig config = buildConfig(Map.of(
            "pipeline.handoff.bindings.orders-ready.targets.deliver.kind", "HTTP",
            "pipeline.handoff.bindings.orders-ready.targets.deliver.base-url", "http://localhost:8081",
            "pipeline.handoff.bindings.orders-ready.targets.deliver.path", "/pipeline/checkpoints/publish",
            "pipeline.handoff.bindings.orders-ready.targets.deliver.method", "POST",
            "pipeline.handoff.bindings.orders-ready.targets.deliver.content-type", "application/json"));

        PipelineHandoffConfig.TargetConfig target = config.bindings().get("orders-ready").targets().get("deliver");
        assertEquals(PublicationTargetKind.HTTP, target.kind());
        assertEquals("http://localhost:8081", target.baseUrl().orElseThrow());
        assertEquals("/pipeline/checkpoints/publish", target.path().orElseThrow());
        assertEquals("POST", target.method());
        assertEquals("application/json", target.contentType().orElseThrow());
        assertFalse(target.encoding().isPresent());
    }

    @Test
    void configReadsGrpcBindingShape() {
        PipelineHandoffConfig config = buildConfig(Map.of(
            "pipeline.handoff.bindings.orders-ready.targets.deliver.kind", "GRPC",
            "pipeline.handoff.bindings.orders-ready.targets.deliver.host", "localhost",
            "pipeline.handoff.bindings.orders-ready.targets.deliver.port", "9000",
            "pipeline.handoff.bindings.orders-ready.targets.deliver.plaintext", "true"));

        PipelineHandoffConfig.TargetConfig target = config.bindings().get("orders-ready").targets().get("deliver");
        assertEquals(PublicationTargetKind.GRPC, target.kind());
        assertEquals("localhost", target.host().orElseThrow());
        assertEquals(9000, target.port().orElseThrow());
        assertTrue(target.plaintext());
    }

    private PipelineHandoffConfig buildConfig(Map<String, String> properties) {
        SmallRyeConfigBuilder builder = new SmallRyeConfigBuilder();
        builder.withMapping(PipelineHandoffConfig.class);
        builder.withSources(new MapConfigSource(properties));
        SmallRyeConfig config = builder.build();
        return config.getConfigMapping(PipelineHandoffConfig.class);
    }

    private record MapConfigSource(Map<String, String> values) implements ConfigSource {
        @Override
        public Map<String, String> getProperties() {
            return values;
        }

        @Override
        public java.util.Set<String> getPropertyNames() {
            return values.keySet();
        }

        @Override
        public String getValue(String propertyName) {
            return values.get(propertyName);
        }

        @Override
        public String getName() {
            return "test-handoff-config";
        }
    }
}
