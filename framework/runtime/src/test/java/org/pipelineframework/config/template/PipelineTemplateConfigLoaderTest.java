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
            version: 2
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
            messages:
              FooInput:
                fields:
                  - number: 1
                    name: "id"
                    type: "uuid"
              FooOutput:
                fields:
                  - number: 1
                    name: "status"
                    type: "string"
            steps:
              - name: "Process Foo"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "FooInput"
                inboundMapper: "com.example.test.mapper.FooInputMapper"
                outputTypeName: "FooOutput"
                outboundMapper: "com.example.test.mapper.FooOutputMapper"
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
        assertEquals("com.example.test.mapper.FooInputMapper", step.inboundMapper());
        assertEquals("com.example.test.mapper.FooOutputMapper", step.outboundMapper());

        Map<String, PipelineTemplateAspect> aspects = config.aspects();
        assertNotNull(aspects);
        assertTrue(aspects.containsKey("persistence"));
    }

    @Test
    void loadsUnionDefinitions() throws Exception {
        String yaml = """
            version: 2
            appName: "Union Test App"
            basePackage: "com.example.test"
            transport: "GRPC"
            messages:
              PaymentRequest:
                fields:
                  - number: 1
                    name: "orderId"
                    type: "uuid"
              PaymentCaptured:
                fields:
                  - number: 1
                    name: "orderId"
                    type: "uuid"
              PaymentRejected:
                fields:
                  - number: 1
                    name: "orderId"
                    type: "uuid"
            unions:
              PaymentOutcome:
                variants:
                  captured:
                    type: "PaymentCaptured"
                    number: 1
                  rejected:
                    type: "PaymentRejected"
                    number: 2
            steps:
              - name: "Capture Payment"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "PaymentRequest"
                outputTypeName: "PaymentOutcome"
            """;
        Path configPath = tempDir.resolve("pipeline-config-union.yaml");
        Files.writeString(configPath, yaml);

        PipelineTemplateConfig config = new PipelineTemplateConfigLoader().load(configPath);

        assertTrue(config.unions().containsKey("PaymentOutcome"));
        PipelineTemplateUnion union = config.unions().get("PaymentOutcome");
        assertEquals("PaymentCaptured", union.variants().get("captured").type());
        assertEquals(2, union.variants().get("rejected").number());
        assertEquals("PaymentOutcome", config.steps().getFirst().outputTypeName());
        assertTrue(config.steps().getFirst().outputFields().isEmpty());
    }

    @Test
    void rejectsUnionVariantWithUnknownMessage() throws Exception {
        String yaml = """
            version: 2
            appName: "Union Test App"
            basePackage: "com.example.test"
            transport: "GRPC"
            messages:
              PaymentRequest:
                fields:
                  - number: 1
                    name: "orderId"
                    type: "uuid"
            unions:
              PaymentOutcome:
                variants:
                  captured:
                    type: "PaymentCaptured"
                    number: 1
            steps:
              - name: "Capture Payment"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "PaymentRequest"
                outputTypeName: "PaymentOutcome"
            """;
        Path configPath = tempDir.resolve("pipeline-config-unknown-union-message.yaml");
        Files.writeString(configPath, yaml);

        IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> new PipelineTemplateConfigLoader().load(configPath));

        assertTrue(exception.getMessage().contains("references unknown message 'PaymentCaptured'"));
    }

    @Test
    void rejectsDuplicateUnionVariantNumbers() throws Exception {
        String yaml = """
            version: 2
            appName: "Union Test App"
            basePackage: "com.example.test"
            transport: "GRPC"
            messages:
              PaymentRequest:
                fields:
                  - number: 1
                    name: "orderId"
                    type: "uuid"
              PaymentCaptured:
                fields:
                  - number: 1
                    name: "orderId"
                    type: "uuid"
              PaymentRejected:
                fields:
                  - number: 1
                    name: "orderId"
                    type: "uuid"
            unions:
              PaymentOutcome:
                variants:
                  captured:
                    type: "PaymentCaptured"
                    number: 1
                  rejected:
                    type: "PaymentRejected"
                    number: 1
            steps:
              - name: "Capture Payment"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "PaymentRequest"
                outputTypeName: "PaymentOutcome"
            """;
        Path configPath = tempDir.resolve("pipeline-config-duplicate-union-number.yaml");
        Files.writeString(configPath, yaml);

        IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> new PipelineTemplateConfigLoader().load(configPath));

        assertTrue(exception.getMessage().contains("Duplicate variant number 1"));
    }

    @Test
    void rejectsUnionNameThatCollidesWithMessageName() throws Exception {
        String yaml = """
            version: 2
            appName: "Union Test App"
            basePackage: "com.example.test"
            transport: "GRPC"
            messages:
              PaymentRequest:
                fields:
                  - number: 1
                    name: "orderId"
                    type: "uuid"
              PaymentOutcome:
                fields:
                  - number: 1
                    name: "orderId"
                    type: "uuid"
            unions:
              PaymentOutcome:
                variants:
                  captured:
                    type: "PaymentOutcome"
                    number: 1
            steps:
              - name: "Capture Payment"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "PaymentRequest"
                outputTypeName: "PaymentOutcome"
            """;
        Path configPath = tempDir.resolve("pipeline-config-union-message-collision.yaml");
        Files.writeString(configPath, yaml);

        IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> new PipelineTemplateConfigLoader().load(configPath));

        assertEquals("Union name 'PaymentOutcome' conflicts with a message name", exception.getMessage());
    }

    @Test
    void stillLoadsLegacyTemplateFieldDefinitions() throws Exception {
        String yaml = """
            appName: "Legacy Test App"
            basePackage: "com.example.test"
            transport: "GRPC"
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
        Path configPath = tempDir.resolve("pipeline-config-legacy.yaml");
        Files.writeString(configPath, yaml);

        PipelineTemplateConfig config = new PipelineTemplateConfigLoader().load(configPath);

        assertEquals("Legacy Test App", config.appName());
        assertEquals(1, config.steps().size());
        PipelineTemplateStep step = config.steps().getFirst();
        assertEquals("FooInput", step.inputTypeName());
        assertEquals("FooOutput", step.outputTypeName());
        assertEquals(1, step.inputFields().size());
        assertEquals("id", step.inputFields().getFirst().name());
        assertEquals("UUID", step.inputFields().getFirst().type());
        assertEquals("string", step.inputFields().getFirst().protoType());
        assertEquals(1, step.outputFields().size());
        assertEquals("status", step.outputFields().getFirst().name());
        assertEquals("String", step.outputFields().getFirst().type());
        assertEquals("string", step.outputFields().getFirst().protoType());
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
    void rejectsNonListIdempotencyKeyFieldsInCheckpoint() throws Exception {
        String yaml = """
            appName: "Test App"
            basePackage: "com.example.test"
            transport: "GRPC"
            steps:
              - name: "Process Foo"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "FooInput"
                outputTypeName: "FooOutput"
            output:
              checkpoint:
                publication: "orders-processed"
                idempotencyKeyFields: "orderId"
            """;
        Path configPath = tempDir.resolve("pipeline-config-bad-idempotency-keys.yaml");
        Files.writeString(configPath, yaml);

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> new PipelineTemplateConfigLoader().load(configPath));

        assertEquals("output.checkpoint.idempotencyKeyFields must be declared as a YAML list", exception.getMessage());
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
