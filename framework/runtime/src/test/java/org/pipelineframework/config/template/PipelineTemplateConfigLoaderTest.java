package org.pipelineframework.config.template;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PipelineTemplateConfigLoaderTest {

    @TempDir
    Path tempDir;

    @Test
    void loadsTemplateConfigWithDefaults() throws Exception {
        String yaml = """
            appName: "Test App"
            basePackage: "com.example.test"
            transport: "GRPC"
            aspects:
              persistence:
                enabled: true
                scope: "GLOBAL"
                position: "AFTER_STEP"
                order: 5
                config:
                  enabledTargets:
                    - "GRPC_SERVICE"
            steps:
              - name: "Process Foo"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "FooInput"
                inputFields:
                  - name: "id"
                    type: "UUID"
                    protoType: "string"
                outputTypeName: "FooOutput"
                outputFields:
                  - name: "status"
                    type: "String"
                    protoType: "string"
            """;
        Path configPath = tempDir.resolve("pipeline-config.yaml");
        Files.writeString(configPath, yaml);

        PipelineTemplateConfig config = new PipelineTemplateConfigLoader().load(configPath);

        assertEquals("Test App", config.appName());
        assertEquals("com.example.test", config.basePackage());
        assertEquals("GRPC", config.transport());
        assertEquals(PipelinePlatform.COMPUTE, config.platform());
        assertNull(config.input());
        assertNull(config.output());
        assertEquals(1, config.steps().size());

        PipelineTemplateStep step = config.steps().getFirst();
        assertEquals("Process Foo", step.name());
        assertEquals("ONE_TO_ONE", step.cardinality());
        assertEquals("FooInput", step.inputTypeName());
        assertEquals("FooOutput", step.outputTypeName());

        Map<String, PipelineTemplateAspect> aspects = config.aspects();
        assertNotNull(aspects);
        assertTrue(aspects.containsKey("persistence"));
    }

    @Test
    void loadsCheckpointBoundaryDeclarations() throws Exception {
        String yaml = """
            appName: "Test App"
            basePackage: "com.example.test"
            transport: "GRPC"
            steps:
              - name: "Process Foo"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "FooInput"
                outputTypeName: "FooOutput"
            input:
              subscription:
                publication: "orders-ready"
                mapper: "com.example.test.mapper.ReadyOrderMapper"
            output:
              checkpoint:
                publication: "orders-processed"
                idempotencyKeyFields: ["orderId", "customerId"]
            """;
        Path configPath = tempDir.resolve("pipeline-config-boundaries.yaml");
        Files.writeString(configPath, yaml);

        PipelineTemplateConfig config = new PipelineTemplateConfigLoader().load(configPath);

        assertNotNull(config.input());
        assertEquals("orders-ready", config.input().subscription().publication());
        assertEquals("com.example.test.mapper.ReadyOrderMapper", config.input().subscription().mapper());
        assertNotNull(config.output());
        assertEquals("orders-processed", config.output().checkpoint().publication());
        assertEquals(List.of("orderId", "customerId"), config.output().checkpoint().idempotencyKeyFields());
    }

    @Test
    void rejectsLegacyConnectorSection() throws Exception {
        String yaml = """
            appName: "Test App"
            basePackage: "com.example.test"
            transport: "GRPC"
            steps:
              - name: "Process Foo"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "FooInput"
                outputTypeName: "FooOutput"
            connectors: []
            """;
        Path configPath = tempDir.resolve("pipeline-config-connectors.yaml");
        Files.writeString(configPath, yaml);

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> new PipelineTemplateConfigLoader().load(configPath));

        assertEquals(
            "Top-level connectors are no longer supported; use input.subscription and output.checkpoint",
            exception.getMessage());
    }

    @Test
    void rejectsMalformedCheckpointBoundaryBlock() throws Exception {
        String yaml = """
            appName: "Test App"
            basePackage: "com.example.test"
            transport: "GRPC"
            steps:
              - name: "Process Foo"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "FooInput"
                outputTypeName: "FooOutput"
            input:
              subscription: "not-a-map"
            """;
        Path configPath = tempDir.resolve("pipeline-config-bad-input.yaml");
        Files.writeString(configPath, yaml);

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> new PipelineTemplateConfigLoader().load(configPath));

        assertEquals("input.subscription must be declared as a YAML map", exception.getMessage());
    }

    @Test
    void platformCanBeOverriddenViaSystemProperty() throws Exception {
        String yaml = """
            appName: "Test App"
            basePackage: "com.example.test"
            transport: "REST"
            platform: "COMPUTE"
            steps: []
            """;
        Path configPath = tempDir.resolve("pipeline-config.yaml");
        Files.writeString(configPath, yaml);

        Function<String, String> propertyLookup = key -> "pipeline.platform".equals(key) ? "LAMBDA" : null;
        PipelineTemplateConfigLoader loader = new PipelineTemplateConfigLoader(propertyLookup, key -> null);
        PipelineTemplateConfig config = loader.load(configPath);
        assertEquals(PipelinePlatform.FUNCTION, config.platform());
        assertEquals("REST", config.transport());
    }
}
