package org.pipelineframework.config.composition;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.config.boundary.PipelineCheckpointConfig;
import org.pipelineframework.config.boundary.PipelineInputBoundaryConfig;
import org.pipelineframework.config.boundary.PipelineOutputBoundaryConfig;
import org.pipelineframework.config.boundary.PipelineSubscriptionConfig;
import org.pipelineframework.config.template.PipelinePlatform;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.config.template.PipelineTemplateField;
import org.pipelineframework.config.template.PipelineTemplateMaterialization;
import org.pipelineframework.config.template.PipelineTemplateStep;

import static org.junit.jupiter.api.Assertions.*;

class PipelineCompositionConfigLoaderTest {
    @TempDir
    Path tempDir;

    @Test
    void loadIrResolvesPipelinePathsRelativeToManifest() throws Exception {
        Path configs = Files.createDirectories(tempDir.resolve("configs"));
        writePipeline(configs.resolve("producer.yaml"), "Input", fields(field(1, "id", "uuid")), "Output",
            fields(field(1, "id", "uuid")), null, "orders-ready");
        Path manifest = writeManifest(configs,
            """
              - id: producer
                path: producer.yaml
            """);

        PipelineCompositionIr ir = new PipelineCompositionConfigLoader().loadIr(manifest);

        assertEquals(1, ir.nodes().size());
        assertEquals(configs.resolve("producer.yaml").toAbsolutePath().normalize(), ir.nodes().getFirst().path());
        assertEquals(List.of("producer"), ir.entrypointPipelineIds());
        assertEquals(List.of("orders-ready"), ir.terminalPublications());
    }

    @Test
    void duplicatePipelineIdsFail() throws Exception {
        Path manifest = writeManifest(tempDir,
            """
              - id: duplicate
                path: a.yaml
              - id: duplicate
                path: b.yaml
            """);

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> new PipelineCompositionConfigLoader().load(manifest));

        assertTrue(exception.getMessage().contains("Duplicate pipeline id"));
    }

    @Test
    void missingPipelineFileFailsClearly() throws Exception {
        Path manifest = writeManifest(tempDir,
            """
              - id: missing
                path: missing.yaml
            """);

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> new PipelineCompositionConfigLoader().loadIr(manifest));

        assertTrue(exception.getMessage().contains("Pipeline composition pipeline 'missing' file does not exist"));
    }

    @Test
    void invalidManifestShapeFailsClearly() throws Exception {
        Path manifest = tempDir.resolve("pipeline-composition.yaml");
        Files.writeString(manifest,
            """
            version: 1
            name: invalid
            pipelines:
              id: not-a-list
            """);

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> new PipelineCompositionConfigLoader().load(manifest));

        assertEquals("pipeline composition pipelines must be declared as a YAML list", exception.getMessage());
    }

    @Test
    void allowsFanOutFromOneCheckpointPublicationToMultipleSubscribers() throws Exception {
        writePipeline(tempDir.resolve("producer.yaml"), "Input", fields(field(1, "id", "uuid")), "OrderReady",
            fields(field(1, "id", "uuid")), null, "orders-ready");
        writePipeline(tempDir.resolve("consumer-a.yaml"), "OrderReady", fields(field(1, "id", "uuid")), "DoneA",
            fields(field(1, "id", "uuid")), "orders-ready", "done-a");
        writePipeline(tempDir.resolve("consumer-b.yaml"), "OrderReady", fields(field(1, "id", "uuid")), "DoneB",
            fields(field(1, "id", "uuid")), "orders-ready", "done-b");
        Path manifest = writeManifest(tempDir,
            """
              - id: producer
                path: producer.yaml
              - id: consumer-a
                path: consumer-a.yaml
              - id: consumer-b
                path: consumer-b.yaml
            """);

        PipelineCompositionIr ir = new PipelineCompositionConfigLoader().loadIr(manifest);

        assertEquals(3, ir.nodes().size());
        assertEquals(2, ir.handoffs().size());
        assertEquals(List.of("producer"), ir.entrypointPipelineIds());
        assertEquals(List.of("done-a", "done-b"), ir.terminalPublications());
    }

    @Test
    void rejectsSubscriptionWithoutProducer() throws Exception {
        writePipeline(tempDir.resolve("consumer.yaml"), "OrderReady", fields(field(1, "id", "uuid")), "Done",
            fields(field(1, "id", "uuid")), "orders-ready", "done");
        Path manifest = writeManifest(tempDir,
            """
              - id: consumer
                path: consumer.yaml
            """);

        IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> new PipelineCompositionConfigLoader().loadIr(manifest));

        assertEquals("Pipeline subscription for publication 'orders-ready' has no producer in composition",
            exception.getMessage());
    }

    @Test
    void rejectsDuplicatePublicationProducers() throws Exception {
        writePipeline(tempDir.resolve("producer-a.yaml"), "InputA", fields(field(1, "id", "uuid")), "OrderReady",
            fields(field(1, "id", "uuid")), null, "orders-ready");
        writePipeline(tempDir.resolve("producer-b.yaml"), "InputB", fields(field(1, "id", "uuid")), "OrderReady",
            fields(field(1, "id", "uuid")), null, "orders-ready");
        Path manifest = writeManifest(tempDir,
            """
              - id: producer-a
                path: producer-a.yaml
              - id: producer-b
                path: producer-b.yaml
            """);

        IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> new PipelineCompositionConfigLoader().loadIr(manifest));

        assertTrue(exception.getMessage().contains("Duplicate checkpoint publication 'orders-ready'"));
    }

    @Test
    void rejectsHandoffTypeNameMismatch() throws Exception {
        writePipeline(tempDir.resolve("producer.yaml"), "Input", fields(field(1, "id", "uuid")), "OrderReady",
            fields(field(1, "id", "uuid")), null, "orders-ready");
        writePipeline(tempDir.resolve("consumer.yaml"), "DifferentOrder", fields(field(1, "id", "uuid")), "Done",
            fields(field(1, "id", "uuid")), "orders-ready", "done");
        Path manifest = producerConsumerManifest();

        IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> new PipelineCompositionConfigLoader().loadIr(manifest));

        assertTrue(exception.getMessage().contains("Pipeline handoff type mismatch for publication 'orders-ready'"));
    }

    @Test
    void rejectsHandoffFieldNumberMismatch() throws Exception {
        writePipeline(tempDir.resolve("producer.yaml"), "OrderReady", fields(field(1, "id", "uuid")), "OrderReady",
            fields(field(1, "id", "uuid")), null, "orders-ready");
        writePipeline(tempDir.resolve("consumer.yaml"), "OrderReady", fields(field(2, "id", "uuid")), "Done",
            fields(field(1, "id", "uuid")), "orders-ready", "done");
        Path manifest = producerConsumerManifest();

        IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> new PipelineCompositionConfigLoader().loadIr(manifest));

        assertTrue(exception.getMessage().contains("field number mismatch"));
    }

    @Test
    void rejectsHandoffFieldSemanticTypeMismatch() throws Exception {
        writePipeline(tempDir.resolve("producer.yaml"), "OrderReady", fields(field(1, "id", "uuid")), "OrderReady",
            fields(field(1, "id", "uuid")), null, "orders-ready");
        writePipeline(tempDir.resolve("consumer.yaml"), "OrderReady", fields(field(1, "id", "string")), "Done",
            fields(field(1, "id", "uuid")), "orders-ready", "done");
        Path manifest = producerConsumerManifest();

        IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> new PipelineCompositionConfigLoader().loadIr(manifest));

        assertTrue(exception.getMessage().contains("field type mismatch"));
    }

    @Test
    void rejectsHandoffFieldProtoTypeMismatch() throws Exception {
        PipelineCompositionConfig config = new PipelineCompositionConfig(1, "test",
            List.of(
                new PipelineCompositionPipeline("producer", "producer.yaml"),
                new PipelineCompositionPipeline("consumer", "consumer.yaml")));
        PipelineTemplateField outputField = normalizedField(1, "id", "uuid", "string");
        PipelineTemplateField inputField = normalizedField(1, "id", "uuid", "bytes");

        IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> new PipelineCompositionConfigLoader().validate(config, List.of(
                node("producer", null, "orders-ready", "Input", List.of(outputField), "OrderReady", List.of(outputField)),
                node("consumer", "orders-ready", "done", "OrderReady", List.of(inputField), "Done", List.of(inputField)))));

        assertTrue(exception.getMessage().contains("field protoType mismatch"));
    }

    @Test
    void acceptsKitchenFanOutFanInInsideSinglePipeline() throws Exception {
        Files.writeString(tempDir.resolve("kitchen.yaml"),
            """
            version: 2
            appName: Kitchen
            basePackage: com.example.kitchen
            transport: GRPC
            messages:
              OrderAccepted:
                fields:
                  - number: 1
                    name: id
                    type: uuid
              KitchenTask:
                fields:
                  - number: 1
                    name: id
                    type: uuid
                  - number: 2
                    name: task
                    type: string
              OrderReady:
                fields:
                  - number: 1
                    name: id
                    type: uuid
            steps:
              - name: Expand Tasks
                cardinality: ONE_TO_MANY
                inputTypeName: OrderAccepted
                outputTypeName: KitchenTask
              - name: Reduce Tasks
                cardinality: MANY_TO_ONE
                inputTypeName: KitchenTask
                outputTypeName: OrderReady
            output:
              checkpoint:
                publication: orders-ready
                idempotencyKeyFields: [id]
            """);
        Path manifest = writeManifest(tempDir,
            """
              - id: kitchen
                path: kitchen.yaml
            """);

        PipelineCompositionIr ir = new PipelineCompositionConfigLoader().loadIr(manifest);

        assertEquals(1, ir.nodes().size());
        assertEquals("MANY_TO_ONE", ir.nodes().getFirst().terminalStep().cardinality());
        assertEquals(List.of("orders-ready"), ir.terminalPublications());
    }

    @Test
    void compositionSchemaResourceIsPackaged() {
        assertNotNull(Thread.currentThread().getContextClassLoader()
            .getResource("META-INF/pipeline/pipeline-composition-schema.json"));
    }

    private Path producerConsumerManifest() throws Exception {
        return writeManifest(tempDir,
            """
              - id: producer
                path: producer.yaml
              - id: consumer
                path: consumer.yaml
            """);
    }

    private Path writeManifest(Path directory, String pipelineEntries) throws Exception {
        Path manifest = directory.resolve("pipeline-composition.yaml");
        Files.writeString(manifest,
            """
            version: 1
            name: test-composition
            pipelines:
            %s
            """.formatted(pipelineEntries));
        return manifest;
    }

    private void writePipeline(
        Path path,
        String inputType,
        String inputFields,
        String outputType,
        String outputFields,
        String subscriptionPublication,
        String checkpointPublication
    ) throws Exception {
        String inputBoundary = subscriptionPublication == null ? "" : """
            input:
              subscription:
                publication: %s
            """.formatted(subscriptionPublication);
        String outputBoundary = checkpointPublication == null ? "" : """
            output:
              checkpoint:
                publication: %s
                idempotencyKeyFields: [id]
            """.formatted(checkpointPublication);
        String messages = inputType.equals(outputType)
            ? message(inputType, inputFields)
            : message(inputType, inputFields) + message(outputType, outputFields);
        Files.writeString(path,
            """
            version: 2
            appName: Test
            basePackage: com.example.test
            transport: GRPC
            messages:
            %s
            steps:
              - name: Process
                cardinality: ONE_TO_ONE
                inputTypeName: %s
                outputTypeName: %s
            %s%s
            """.formatted(messages, inputType, outputType, inputBoundary, outputBoundary));
    }

    private String message(String typeName, String fields) {
        return """
              %s:
                fields:
            %s
            """.formatted(typeName, fields);
    }

    private String fields(String... fields) {
        return String.join("", fields);
    }

    private String field(int number, String name, String type) {
        return """
                  - number: %d
                    name: %s
                    type: %s
            """.formatted(number, name, type);
    }

    private PipelineCompositionNode node(
        String id,
        String subscriptionPublication,
        String checkpointPublication,
        String inputTypeName,
        List<PipelineTemplateField> inputFields,
        String outputTypeName,
        List<PipelineTemplateField> outputFields
    ) {
        PipelineInputBoundaryConfig input = subscriptionPublication == null
            ? null
            : new PipelineInputBoundaryConfig(new PipelineSubscriptionConfig(subscriptionPublication, null));
        PipelineOutputBoundaryConfig output = checkpointPublication == null
            ? null
            : new PipelineOutputBoundaryConfig(new PipelineCheckpointConfig(checkpointPublication, List.of("id")));
        PipelineTemplateConfig config = new PipelineTemplateConfig(
            2,
            id,
            "com.example." + id.replace('-', '.'),
            "GRPC",
            PipelinePlatform.COMPUTE,
            Map.of(),
            Map.of(),
            List.of(new PipelineTemplateStep(
                "Process",
                "ONE_TO_ONE",
                inputTypeName,
                inputFields,
                outputTypeName,
                outputFields)),
            Map.of(),
            input,
            output,
            new PipelineTemplateMaterialization(List.of()));
        return new PipelineCompositionNode(id, tempDir.resolve(id + ".yaml"), config);
    }

    private PipelineTemplateField normalizedField(int number, String name, String type, String protoType) {
        return new PipelineTemplateField(
            number,
            name,
            type,
            type,
            null,
            "UUID",
            protoType,
            null,
            null,
            false,
            false,
            false,
            null,
            null,
            null,
            null,
            null);
    }
}
