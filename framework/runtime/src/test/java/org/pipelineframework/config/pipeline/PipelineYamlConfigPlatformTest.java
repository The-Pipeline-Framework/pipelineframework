package org.pipelineframework.config.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.boundary.PipelineCheckpointConfig;
import org.pipelineframework.config.boundary.PipelineInputBoundaryConfig;
import org.pipelineframework.config.boundary.PipelineOutputBoundaryConfig;
import org.pipelineframework.config.boundary.PipelineSubscriptionConfig;

class PipelineYamlConfigPlatformTest {

    @Test
    void canonicalizesLegacyPlatformAliases() {
        PipelineYamlConfig fromLegacy = new PipelineYamlConfig(
            "org.example",
            "REST",
            "LAMBDA",
            List.of(),
            List.of());
        PipelineYamlConfig fromCanonical = new PipelineYamlConfig(
            "org.example",
            "REST",
            "FUNCTION",
            List.of(),
            List.of());
        PipelineYamlConfig fromStandard = new PipelineYamlConfig(
            "org.example",
            "REST",
            "STANDARD",
            List.of(),
            List.of());

        assertEquals("FUNCTION", fromLegacy.platform());
        assertEquals("FUNCTION", fromCanonical.platform());
        assertEquals("COMPUTE", fromStandard.platform());
    }

    @Test
    void defaultsToComputeWhenPlatformMissing() {
        PipelineYamlConfig config = new PipelineYamlConfig(
            "org.example",
            "REST",
            null,
            List.of(),
            List.of());
        assertEquals("COMPUTE", config.platform());
    }

    @Test
    void withTransportPreservesCheckpointBoundaries() {
        PipelineYamlConfig config = new PipelineYamlConfig(
            "org.example",
            "REST",
            "FUNCTION",
            List.of(),
            List.of(),
            new PipelineInputBoundaryConfig(new PipelineSubscriptionConfig("orders-ready", "com.example.Mapper")),
            new PipelineOutputBoundaryConfig(new PipelineCheckpointConfig("orders-dispatched", List.of("orderId"))));

        PipelineYamlConfig updated = config.withTransport("GRPC");

        assertEquals("GRPC", updated.transport());
        assertEquals(config.input(), updated.input());
        assertEquals(config.output(), updated.output());
    }
}
