package org.pipelineframework.config.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.StringReader;
import java.util.List;
import org.junit.jupiter.api.Test;

class PipelineYamlConfigLoaderTest {

    @Test
    void loadsCheckpointBoundaryDeclarations() {
        PipelineYamlConfig config = new PipelineYamlConfigLoader().load(new StringReader("""
            basePackage: "com.example"
            transport: "GRPC"
            platform: "COMPUTE"
            steps: []
            input:
              subscription:
                publication: "orders-ready"
                mapper: "com.example.bridge.ReadyOrderMapper"
            output:
              checkpoint:
                publication: "orders-dispatched"
                idempotencyKeyFields: ["orderId", "customerId"]
            """));

        assertNotNull(config.input());
        assertEquals("orders-ready", config.input().subscription().publication());
        assertEquals("com.example.bridge.ReadyOrderMapper", config.input().subscription().mapper());
        assertNotNull(config.output());
        assertEquals("orders-dispatched", config.output().checkpoint().publication());
        assertEquals(List.of("orderId", "customerId"), config.output().checkpoint().idempotencyKeyFields());
    }

    @Test
    void loadsConfigWithoutCheckpointBoundaries() {
        PipelineYamlConfig config = new PipelineYamlConfigLoader().load(new StringReader("""
            basePackage: "com.example"
            transport: "GRPC"
            platform: "COMPUTE"
            steps: []
            """));

        assertNull(config.input());
        assertNull(config.output());
    }

    @Test
    void rejectsLegacyConnectorSection() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlConfigLoader().load(new StringReader("""
                basePackage: "com.example"
                transport: "GRPC"
                platform: "COMPUTE"
                steps: []
                connectors: []
                """)));

        assertEquals(
            "Top-level connectors are no longer supported; use input.subscription and output.checkpoint",
            exception.getMessage());
    }

    @Test
    void rejectsMalformedCheckpointBoundaryBlocks() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlConfigLoader().load(new StringReader("""
                basePackage: "com.example"
                transport: "GRPC"
                platform: "COMPUTE"
                steps: []
                output:
                  checkpoint: "not-a-map"
                """)));

        assertEquals("output.checkpoint must be defined as a map", exception.getMessage());
    }

    @Test
    void rejectsNonListIdempotencyKeyFieldsInCheckpoint() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlConfigLoader().load(new StringReader("""
                basePackage: "com.example"
                transport: "GRPC"
                platform: "COMPUTE"
                steps: []
                output:
                  checkpoint:
                    publication: "orders-dispatched"
                    idempotencyKeyFields: "orderId"
                """)));

        assertEquals("output.checkpoint.idempotencyKeyFields must be defined as a list", exception.getMessage());
    }

    @Test
    void rejectsMalformedSubscriptionBlock() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlConfigLoader().load(new StringReader("""
                basePackage: "com.example"
                transport: "GRPC"
                platform: "COMPUTE"
                steps: []
                input:
                  subscription: "not-a-map"
                """)));

        assertEquals("input.subscription must be defined as a map", exception.getMessage());
    }

    @Test
    void rejectsBlankPublicationInSubscription() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlConfigLoader().load(new StringReader("""
                basePackage: "com.example"
                transport: "GRPC"
                platform: "COMPUTE"
                steps: []
                input:
                  subscription:
                    publication: "  "
                """)));

        assertEquals("input.subscription.publication must not be blank", exception.getMessage());
    }
}
