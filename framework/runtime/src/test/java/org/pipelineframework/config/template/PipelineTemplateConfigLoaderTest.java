package org.pipelineframework.config.template;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.materialization.MaterializationAction;
import org.pipelineframework.materialization.MaterializationPosition;
import org.pipelineframework.materialization.MaterializationScope;

import static org.junit.jupiter.api.Assertions.*;

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
                  options:
                    retries: 3
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
        PipelineTemplateAspect persistence = aspects.get("persistence");
        assertThrows(UnsupportedOperationException.class, () -> persistence.config().put("newKey", "newValue"));
        @SuppressWarnings("unchecked")
        Map<String, Object> options = (Map<String, Object>) persistence.config().get("options");
        assertThrows(UnsupportedOperationException.class, () -> options.put("timeout", "PT5S"));
        @SuppressWarnings("unchecked")
        List<Object> enabledTargets = (List<Object>) persistence.config().get("enabledTargets");
        assertThrows(UnsupportedOperationException.class, () -> enabledTargets.add("REST_RESOURCE"));
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
    void loadsBranchRoutingAcceptsAndTerminalFields() throws Exception {
        String yaml = """
            version: 2
            appName: "Order Routing"
            basePackage: "com.example.order"
            transport: "GRPC"
            messages:
              StockReserved: { fields: [] }
              LicenseProvisioned: { fields: [] }
              FinalizedOrder: { fields: [] }
            unions:
              OrderCompletion:
                variants:
                  stock:
                    type: "StockReserved"
                    number: 1
                  license:
                    type: "LicenseProvisioned"
                    number: 2
            steps:
              - name: "Finalize"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "OrderCompletion"
                outputTypeName: "FinalizedOrder"
                accepts:
                  - "StockReserved"
                  - "LicenseProvisioned"
                terminal: true
            """;
        Path configPath = tempDir.resolve("pipeline-config-branch-routing.yaml");
        Files.writeString(configPath, yaml);

        PipelineTemplateConfig config = new PipelineTemplateConfigLoader().load(configPath);

        PipelineTemplateStep step = config.steps().getFirst();
        assertEquals(List.of("StockReserved", "LicenseProvisioned"), step.accepts());
        assertTrue(step.terminal());
    }

    @Test
    void rejectsPredicateStyleRoutingKeys() throws Exception {
        String yaml = """
            version: 2
            appName: "Order Routing"
            basePackage: "com.example.order"
            transport: "GRPC"
            messages:
              PhysicalOrder: { fields: [] }
              StockReserved: { fields: [] }
            steps:
              - name: "Reserve Stock"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "PhysicalOrder"
                outputTypeName: "StockReserved"
                when: "country == 'ES'"
            """;
        Path configPath = tempDir.resolve("pipeline-config-predicate.yaml");
        Files.writeString(configPath, yaml);

        IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> new PipelineTemplateConfigLoader().load(configPath));

        assertTrue(exception.getMessage().contains("unsupported predicate-style routing keys"));
        assertTrue(exception.getMessage().contains("when"));
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
    void loadsObjectPublishOutputBoundary() throws Exception {
        String yaml = """
            appName: "Test App"
            basePackage: "com.example.test"
            transport: "GRPC"
            publish:
              results:
                kind: object
                provider: filesystem
                location:
                  root: "/tmp/outgoing"
                naming:
                  keyTemplate: "{groupKey}.out"
                payload:
                  contentType: text/csv
                grouping:
                  maxOpenGroups: 7
            output:
              to: results
              consumes:
                type: com.example.test.FooOutput
                typeName: FooOutput
                mapper: com.example.test.mapper.FooOutputPublishMapper
            steps:
              - name: "Process Foo"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "FooInput"
                outputTypeName: "FooOutput"
            """;
        Path configPath = tempDir.resolve("pipeline-config-object-publish.yaml");
        Files.writeString(configPath, yaml);

        PipelineTemplateConfig config = new PipelineTemplateConfigLoader().load(configPath);

        assertEquals(1, config.publish().size());
        assertEquals("filesystem", config.publish().get("results").provider());
        assertEquals("/tmp/outgoing", config.publish().get("results").location().get("root"));
        assertEquals("{groupKey}.out", config.publish().get("results").naming().keyTemplate());
        assertEquals("text/csv", config.publish().get("results").payload().contentType());
        assertEquals(7, config.publish().get("results").grouping().maxOpenGroups());
        assertNotNull(config.output());
        assertEquals("results", config.output().object().target());
        assertEquals("com.example.test.FooOutput", config.output().object().type());
        assertEquals("FooOutput", config.output().object().typeName());
        assertEquals("com.example.test.mapper.FooOutputPublishMapper", config.output().object().mapper());
    }

    @Test
    void loadsObjectPublishOutputBoundaryDefaults() throws Exception {
        String yaml = """
            appName: "Test App"
            basePackage: "com.example.test"
            transport: "GRPC"
            publish:
              results:
                kind: object
                provider: filesystem
                location:
                  root: "/tmp/outgoing"
            output:
              to: results
              consumes:
                type: com.example.test.FooOutput
                typeName: FooOutput
                mapper: com.example.test.mapper.FooOutputPublishMapper
            steps:
              - name: "Process Foo"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "FooInput"
                outputTypeName: "FooOutput"
            """;
        Path configPath = tempDir.resolve("pipeline-config-object-publish-defaults.yaml");
        Files.writeString(configPath, yaml);

        PipelineTemplateConfig config = new PipelineTemplateConfigLoader().load(configPath);

        assertEquals("{groupKey}", config.publish().get("results").naming().keyTemplate());
        assertEquals("application/octet-stream", config.publish().get("results").payload().contentType());
        assertEquals(StandardCharsets.UTF_8, config.publish().get("results").payload().charset());
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
    void rejectsObjectOutputReferencingMissingPublishTarget() throws Exception {
        String yaml = """
            appName: "Test App"
            basePackage: "com.example.test"
            transport: "GRPC"
            output:
              to: missing
              consumes:
                type: com.example.test.FooOutput
                mapper: com.example.test.mapper.FooOutputPublishMapper
            steps: []
            """;
        Path configPath = tempDir.resolve("pipeline-config-missing-publish.yaml");
        Files.writeString(configPath, yaml);

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> new PipelineTemplateConfigLoader().load(configPath));

        assertEquals("output.object publish target not found: missing", exception.getMessage());
    }

    @Test
    void rejectsNonPositiveObjectSourcePollSettings() throws Exception {
        String yaml = """
            appName: "Test App"
            basePackage: "com.example.test"
            transport: "GRPC"
            sources:
              documents:
                kind: object
                provider: filesystem
                poll:
                  interval: PT0S
            steps: []
            """;
        Path configPath = tempDir.resolve("pipeline-config-bad-poll.yaml");
        Files.writeString(configPath, yaml);

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> new PipelineTemplateConfigLoader().load(configPath));

        assertEquals("object source poll.interval must be positive", exception.getMessage());
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

    @Test
    void loadsReferenceableFieldAndMaterializationPolicy() throws Exception {
        String yaml = """
            version: 2
            appName: "Materialized Search"
            basePackage: "com.example.search"
            transport: "GRPC"
            messages:
              ParsedDocument:
                fields:
                  - number: 1
                    name: "docId"
                    type: "string"
                  - number: 2
                    name: "text"
                    type: "string"
                    optional: true
                    referenceable:
                      refField: "textRef"
                  - number: 3
                    name: "textRef"
                    type: "payload_ref"
                    optional: true
              SemanticChunk:
                fields:
                  - number: 1
                    name: "docId"
                    type: "string"
                  - number: 2
                    name: "text"
                    type: "string"
            steps:
              - name: "Chunk Document"
                cardinality: "ONE_TO_MANY"
                inputTypeName: "ParsedDocument"
                outputTypeName: "SemanticChunk"
            materialization:
              aspects:
                - name: "chunker-needs-text"
                  enabled: true
                  scope: "STEPS"
                  position: "BEFORE_STEP"
                  targetSteps: ["Chunk Document"]
                  action: "dereference"
                  message: "ParsedDocument"
                  fields: ["text"]
            """;
        Path configPath = tempDir.resolve("pipeline-config-materialization.yaml");
        Files.writeString(configPath, yaml);

        PipelineTemplateConfig config = new PipelineTemplateConfigLoader().load(configPath);

        PipelineTemplateField text = config.messages().get("ParsedDocument").fields().stream()
            .filter(field -> "text".equals(field.name()))
            .findFirst()
            .orElseThrow();
        assertEquals("textRef", text.referenceable().refField());
        PipelineTemplateField textRef = config.messages().get("ParsedDocument").fields().stream()
            .filter(field -> "textRef".equals(field.name()))
            .findFirst()
            .orElseThrow();
        assertEquals("payload_ref", textRef.canonicalType());

        PipelineTemplateMaterializationAspect aspect = config.materialization().aspects().getFirst();
        assertEquals("chunker-needs-text", aspect.name());
        assertEquals(MaterializationScope.STEPS, aspect.scope());
        assertEquals(MaterializationPosition.BEFORE_STEP, aspect.position());
        assertEquals(MaterializationAction.DEREFERENCE, aspect.action());
        assertEquals(List.of("Chunk Document"), aspect.targetSteps());
        assertEquals(List.of("text"), aspect.fields());
    }

    @Test
    void rejectsReferenceableFieldWithoutPayloadReferenceSibling() throws Exception {
        String yaml = """
            version: 2
            appName: "Bad Materialization"
            basePackage: "com.example.search"
            transport: "GRPC"
            messages:
              ParsedDocument:
                fields:
                  - number: 1
                    name: "text"
                    type: "string"
                    referenceable:
                      refField: "textRef"
                  - number: 2
                    name: "textRef"
                    type: "string"
                    optional: true
            steps: []
            """;
        Path configPath = tempDir.resolve("pipeline-config-bad-referenceable.yaml");
        Files.writeString(configPath, yaml);

        IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> new PipelineTemplateConfigLoader().load(configPath));

        assertTrue(exception.getMessage().contains("payload_ref"));
    }

    @Test
    void rejectsMaterializationPolicyForUnknownStep() throws Exception {
        String yaml = """
            version: 2
            appName: "Bad Policy"
            basePackage: "com.example.search"
            transport: "GRPC"
            messages:
              ParsedDocument:
                fields:
                  - number: 1
                    name: "text"
                    type: "string"
                    referenceable:
                      refField: "textRef"
                  - number: 2
                    name: "textRef"
                    type: "payload_ref"
                    optional: true
            steps: []
            materialization:
              aspects:
                - name: "bad-target"
                  scope: "STEPS"
                  position: "AFTER_STEP"
                  targetSteps: ["Parse Document"]
                  action: "reference"
                  message: "ParsedDocument"
                  fields: ["text"]
            """;
        Path configPath = tempDir.resolve("pipeline-config-bad-policy-step.yaml");
        Files.writeString(configPath, yaml);

        IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> new PipelineTemplateConfigLoader().load(configPath));

        assertTrue(exception.getMessage().contains("targets unknown step 'Parse Document'"));
    }

    @Test
    void rejectsInlinePayloadReferenceMessageName() throws Exception {
        String yaml = """
            version: 2
            appName: "Bad Inline Message"
            basePackage: "com.example.search"
            transport: "GRPC"
            steps:
              - name: "Inline Payload Reference"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "PayloadReference"
                inputFields:
                  - number: 1
                    name: "key"
                    type: "string"
                outputTypeName: "PayloadReference"
                outputFields:
                  - number: 1
                    name: "key"
                    type: "string"
            """;
        Path configPath = tempDir.resolve("pipeline-config-bad-inline-payload-reference.yaml");
        Files.writeString(configPath, yaml);

        IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> new PipelineTemplateConfigLoader().load(configPath));

        assertTrue(exception.getMessage().contains("PayloadReference"));
        assertTrue(exception.getMessage().contains("reserved for payload_ref fields"));
    }

    @Test
    void rejectsBlankMaterializationFieldEntries() throws Exception {
        String yaml = """
            version: 2
            appName: "Bad Policy"
            basePackage: "com.example.search"
            transport: "GRPC"
            messages:
              ParsedDocument:
                fields:
                  - number: 1
                    name: "text"
                    type: "string"
                    referenceable:
                      refField: "textRef"
                  - number: 2
                    name: "textRef"
                    type: "payload_ref"
                    optional: true
            steps: []
            materialization:
              aspects:
                - name: "bad-fields"
                  action: "reference"
                  message: "ParsedDocument"
                  fields: ["text", " "]
            """;
        Path configPath = tempDir.resolve("pipeline-config-bad-blank-materialization-field.yaml");
        Files.writeString(configPath, yaml);

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> new PipelineTemplateConfigLoader().load(configPath));

        assertTrue(exception.getMessage().contains("fields"));
        assertTrue(exception.getMessage().contains("blank entries"));
    }

    @Test
    void normalizesTypesTuplesAndDeprecatedMessagesObjectsToTheSameModel() throws Exception {
        String prefix = """
            version: 2
            appName: "Compact Types"
            basePackage: "com.example.types"
            transport: "GRPC"
            """;
        Path compact = tempDir.resolve("compact-types.yaml");
        Files.writeString(compact, prefix + """
            types:
              Payment:
                fields:
                  - [1, orderId, uuid]
                  - [2, amount, decimal]
            steps: []
            """);
        Path verbose = tempDir.resolve("verbose-messages.yaml");
        Files.writeString(verbose, prefix + """
            messages:
              Payment:
                fields:
                  - number: 1
                    name: orderId
                    type: uuid
                  - number: 2
                    name: amount
                    type: decimal
            steps: []
            """);
        List<String> warnings = new java.util.ArrayList<>();

        PipelineTemplateConfig compactConfig = new PipelineTemplateConfigLoader().load(compact);
        PipelineTemplateConfig verboseConfig = new PipelineTemplateConfigLoader(key -> null, key -> null, warnings::add)
            .load(verbose);

        assertEquals(compactConfig.messages(), verboseConfig.messages());
        assertEquals(compactConfig.types(), compactConfig.messages());
        assertEquals(List.of("Top-level 'messages' is deprecated; use 'types'."), warnings);
    }

    @Test
    void rejectsTypesAndMessagesTogether() throws Exception {
        Path configPath = tempDir.resolve("duplicate-type-sections.yaml");
        Files.writeString(configPath, """
            version: 2
            appName: "Duplicate Types"
            basePackage: "com.example.types"
            transport: "GRPC"
            types: {}
            messages: {}
            steps: []
            """);

        IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> new PipelineTemplateConfigLoader().load(configPath));

        assertEquals(
            "Pipeline template cannot declare both 'types' and deprecated 'messages'; use 'types' only.",
            exception.getMessage());
    }

    @Test
    void rejectsMalformedFieldTupleWithTypeAndIndexDiagnostic() throws Exception {
        Path configPath = tempDir.resolve("bad-field-tuple.yaml");
        Files.writeString(configPath, """
            version: 2
            appName: "Bad Tuple"
            basePackage: "com.example.types"
            transport: "GRPC"
            types:
              Payment:
                fields:
                  - [0, orderId, uuid]
            steps: []
            """);

        IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> new PipelineTemplateConfigLoader().load(configPath));

        assertTrue(exception.getMessage().contains("type 'Payment' field 0"));
        assertTrue(exception.getMessage().contains("[positiveNumber, nonBlankName, type]"));
    }

    @Test
    void propagatesLinearInputsAndValidatesFinalOutputAssertion() throws Exception {
        Path configPath = tempDir.resolve("linear-contracts.yaml");
        Files.writeString(configPath, """
            version: 2
            appName: "Linear Contracts"
            basePackage: "com.example.linear"
            transport: "GRPC"
            types:
              PaymentRequest:
                fields: [[1, id, uuid]]
              ValidatedPayment:
                fields: [[1, id, uuid]]
              PaymentOutcome:
                fields: [[1, id, uuid]]
            contract:
              input: PaymentRequest
              output: PaymentOutcome
            steps:
              - name: Validate Payment
                cardinality: ONE_TO_ONE
                output: ValidatedPayment
              - name: Process Payment
                cardinality: ONE_TO_ONE
                output: PaymentOutcome
            """);

        PipelineTemplateConfig config = new PipelineTemplateConfigLoader().load(configPath);

        assertEquals("PaymentRequest", config.inputContract());
        assertEquals("PaymentOutcome", config.outputContract());
        assertNull(config.input());
        assertNull(config.output());
        assertEquals("PaymentRequest", config.steps().get(0).inputTypeName());
        assertEquals("ValidatedPayment", config.steps().get(1).inputTypeName());
        assertEquals(config.messages().get("ValidatedPayment").fields(), config.steps().get(1).inputFields());
    }

    @Test
    void rejectsLinearInputMismatchAndMissingPreviousOutput() throws Exception {
        String prefix = """
            version: 2
            appName: "Linear Failure"
            basePackage: "com.example.linear"
            transport: "GRPC"
            types:
              A:
                fields: [[1, id, uuid]]
              B:
                fields: [[1, id, uuid]]
              C:
                fields: [[1, id, uuid]]
            contract:
              input: A
            """;
        Path mismatch = tempDir.resolve("linear-mismatch.yaml");
        Files.writeString(mismatch, prefix + """
            steps:
              - name: First
                cardinality: ONE_TO_ONE
                input: B
                output: C
            """);
        Path missing = tempDir.resolve("linear-missing-output.yaml");
        Files.writeString(missing, prefix + """
            steps:
              - name: First
                cardinality: ONE_TO_ONE
              - name: Second
                cardinality: ONE_TO_ONE
                output: C
            """);

        IllegalStateException mismatchError = assertThrows(
            IllegalStateException.class,
            () -> new PipelineTemplateConfigLoader().load(mismatch));
        IllegalStateException missingError = assertThrows(
            IllegalStateException.class,
            () -> new PipelineTemplateConfigLoader().load(missing));

        assertTrue(mismatchError.getMessage().contains("pipeline input resolves to 'A'"));
        assertTrue(missingError.getMessage().contains("previous output is missing or not singular"));
    }

    @Test
    void rejectsLinearPropagationForBranchAwareTemplate() throws Exception {
        Path configPath = tempDir.resolve("linear-branch.yaml");
        Files.writeString(configPath, """
            version: 2
            appName: "Branch Failure"
            basePackage: "com.example.branch"
            transport: "GRPC"
            types:
              A:
                fields: [[1, id, uuid]]
              B:
                fields: [[1, id, uuid]]
            contract:
              input: A
            steps:
              - name: Branch
                cardinality: ONE_TO_ONE
                output: B
                accepts: [A]
            """);

        IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> new PipelineTemplateConfigLoader().load(configPath));

        assertTrue(exception.getMessage().contains("cannot be used with branch-aware templates"));
    }

    @Test
    void keepsPhysicalBoundariesSeparateFromLogicalContracts() throws Exception {
        Path configPath = tempDir.resolve("boundary-and-contract.yaml");
        Files.writeString(configPath, """
            version: 2
            appName: "Boundary And Contract"
            basePackage: "com.example.boundary"
            transport: "GRPC"
            types:
              PaymentRequest:
                fields: [[1, id, uuid]]
              PaymentOutcome:
                fields: [[1, id, uuid]]
            input:
              subscription:
                publication: payment-requests
            output:
              checkpoint:
                publication: payment-outcomes
            contract:
              input: PaymentRequest
              output: PaymentOutcome
            steps:
              - name: Process Payment
                cardinality: ONE_TO_ONE
                output: PaymentOutcome
            """);

        PipelineTemplateConfig config = new PipelineTemplateConfigLoader().load(configPath);

        assertNotNull(config.input());
        assertNotNull(config.output());
        assertEquals("PaymentRequest", config.inputContract());
        assertEquals("PaymentOutcome", config.outputContract());
        assertEquals("PaymentRequest", config.steps().getFirst().inputTypeName());
    }

    @Test
    void rejectsRootScalarContractsWithTheCollisionFreeNamespaceDiagnostic() throws Exception {
        Path configPath = tempDir.resolve("root-scalar-contract.yaml");
        Files.writeString(configPath, """
            version: 2
            appName: "Root Scalar Contract"
            basePackage: "com.example.boundary"
            transport: "GRPC"
            types:
              PaymentRequest:
                fields: [[1, id, uuid]]
            input: PaymentRequest
            steps: []
            """);

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> new PipelineTemplateConfigLoader().load(configPath));

        assertTrue(exception.getMessage().contains("contract.input"));
    }

    @Test
    void preservesFullyQualifiedJavaStepContractsWithoutTreatingThemAsLogicalTypes() throws Exception {
        Path legacy = tempDir.resolve("qualified-java-v1.yaml");
        Files.writeString(legacy, """
            appName: "Qualified Java v1"
            basePackage: "com.example"
            transport: "REST"
            steps:
              - name: payment
                input: com.example.domain.PaymentRecord
                output: com.example.domain.PaymentStatus
            """);
        Path v2 = tempDir.resolve("qualified-java-v2.yaml");
        Files.writeString(v2, """
            version: 2
            appName: "Qualified Java v2"
            basePackage: "com.example"
            transport: "REST"
            types:
              PaymentRecord:
                fields: [[1, id, uuid]]
              PaymentStatus:
                fields: [[1, id, uuid]]
            steps:
              - name: payment
                input: com.example.domain.PaymentRecord
                inputTypeName: PaymentRecord
                output: com.example.domain.PaymentStatus
                outputTypeName: PaymentStatus
            """);

        PipelineTemplateConfig legacyConfig = new PipelineTemplateConfigLoader().load(legacy);
        PipelineTemplateConfig v2Config = new PipelineTemplateConfigLoader().load(v2);

        assertNull(legacyConfig.steps().getFirst().inputTypeName());
        assertNull(legacyConfig.steps().getFirst().outputTypeName());
        assertEquals("PaymentRecord", v2Config.steps().getFirst().inputTypeName());
        assertEquals("PaymentStatus", v2Config.steps().getFirst().outputTypeName());
    }

    @Test
    void acceptsLogicalContractsWithNestedJavaBindingsAndWarnsForLegacyFqcnSyntax() throws Exception {
        Path canonical = tempDir.resolve("canonical-java-bindings.yaml");
        Files.writeString(canonical, """
            version: 2
            appName: Canonical Java Bindings
            basePackage: com.example
            types:
              PaymentRecord: { fields: [[1, id, uuid]] }
              PaymentStatus: { fields: [[1, id, uuid]] }
            steps:
              - name: payment
                cardinality: ONE_TO_ONE
                input: PaymentRecord
                output: PaymentStatus
                java:
                  input: com.example.domain.PaymentRecord
                  output: com.example.domain.PaymentStatus
            """);
        Path legacy = tempDir.resolve("legacy-java-bindings.yaml");
        Files.writeString(legacy, """
            version: 2
            appName: Legacy Java Bindings
            basePackage: com.example
            types:
              PaymentRecord: { fields: [[1, id, uuid]] }
              PaymentStatus: { fields: [[1, id, uuid]] }
            steps:
              - name: payment
                cardinality: ONE_TO_ONE
                input: com.example.domain.PaymentRecord
                inputTypeName: PaymentRecord
                output: com.example.domain.PaymentStatus
                outputTypeName: PaymentStatus
            """);

        PipelineTemplateConfig canonicalConfig = new PipelineTemplateConfigLoader().load(canonical);
        List<String> warnings = new java.util.ArrayList<>();
        PipelineTemplateConfig legacyConfig = new PipelineTemplateConfigLoader(key -> null, key -> null, warnings::add).load(legacy);

        assertEquals("PaymentRecord", canonicalConfig.steps().getFirst().inputTypeName());
        assertEquals("PaymentStatus", canonicalConfig.steps().getFirst().outputTypeName());
        assertEquals(canonicalConfig.steps(), legacyConfig.steps());
        assertEquals(List.of("Step 'payment' uses deprecated fully qualified 'input/output' contracts; use logical "
            + "input/output with java.input/java.output instead."), warnings);
    }

    @Test
    void resolvesUnqualifiedCanonicalContractsAfterCollectingInlineMessages() throws Exception {
        Path configPath = tempDir.resolve("inline-canonical-contracts.yaml");
        Files.writeString(configPath, """
            version: 2
            appName: "Inline Contracts"
            basePackage: "com.example.inline"
            transport: "GRPC"
            steps:
              - name: Produce
                cardinality: ONE_TO_ONE
                input: SourceInput
                inputFields: [[1, id, uuid]]
                output: InlineResult
                outputFields: [[1, id, uuid]]
              - name: Consume
                cardinality: ONE_TO_ONE
                input: InlineResult
                output: FinalResult
                outputFields: [[1, id, uuid]]
            """);

        PipelineTemplateConfig config = new PipelineTemplateConfigLoader().load(configPath);

        assertTrue(config.messages().containsKey("InlineResult"));
        assertTrue(config.messages().containsKey("FinalResult"));
        assertEquals("InlineResult", config.steps().get(1).inputTypeName());
        assertEquals(config.messages().get("InlineResult").fields(), config.steps().get(1).inputFields());
    }

    @Test
    void rejectsUnknownUnqualifiedCanonicalContractInsteadOfDroppingIt() throws Exception {
        Path configPath = tempDir.resolve("unknown-canonical-contract.yaml");
        Files.writeString(configPath, """
            version: 2
            appName: "Unknown Contract"
            basePackage: "com.example.unknown"
            transport: "GRPC"
            steps:
              - name: Process
                cardinality: ONE_TO_ONE
                input: MisspelledInput
                output: KnownOutput
                outputFields: [[1, id, uuid]]
            """);

        IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> new PipelineTemplateConfigLoader().load(configPath));

        assertTrue(exception.getMessage().contains("unknown input message or union 'MisspelledInput'"), exception.getMessage());
    }
}
