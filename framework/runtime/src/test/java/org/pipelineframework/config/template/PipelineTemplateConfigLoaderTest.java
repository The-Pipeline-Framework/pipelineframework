package org.pipelineframework.config.template;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorProviderManifestCatalog;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLoader;
import org.pipelineframework.materialization.MaterializationAction;
import org.pipelineframework.materialization.MaterializationPosition;
import org.pipelineframework.materialization.MaterializationScope;
import org.pipelineframework.protocol.ProtocolTypeDescriptor;
import org.pipelineframework.protocol.ProtocolTypeIdentity;
import org.pipelineframework.protocol.ProtocolTypeRegistry;

import static org.junit.jupiter.api.Assertions.*;

class PipelineTemplateConfigLoaderTest {

    @TempDir
    Path tempDir;

    @Test
    void loadsV3LocalPipelineCatalogThroughTheOrdinaryStepGrammar() throws Exception {
        Path configPath = tempDir.resolve("v3-local-pipelines.yaml");
        Files.writeString(configPath, """
            version: 3
            appName: Local catalog
            basePackage: com.example.local
            transport: LOCAL
            contract:
              input: Value
              output: Value
            types:
              Value:
                fields: [[id, string]]
            pipelines:
              inner:
                input: Value
                output: Value
                steps:
                  - name: X
                    cardinality: ONE_TO_ONE
                    input: Value
                    output: Value
                  - name: Y
                    cardinality: ONE_TO_ONE
                    input: Value
                    output: Value
            steps:
              - name: Call inner
                cardinality: ONE_TO_ONE
                input: Value
                output: Value
                pipeline: inner
            """);

        PipelineTemplateConfig config = new PipelineTemplateConfigLoader().load(configPath);

        assertEquals(List.of("inner"), config.pipelines().keySet().stream().toList());
        assertEquals(List.of("X", "Y"), config.pipelines().get("inner").steps().stream()
            .map(PipelineTemplateStep::name).toList());
        assertEquals(java.util.Optional.of("inner"), config.steps().getFirst().pipelineReference());
    }

    @Test
    void rejectsDuplicateV3LocalPipelineIds() throws Exception {
        Path configPath = tempDir.resolve("duplicate-v3-local-pipelines.yaml");
        Files.writeString(configPath, """
            version: 3
            appName: Duplicate catalog
            basePackage: com.example.local
            contract: { input: Value, output: Value }
            types: { Value: { fields: [[id, string]] } }
            pipelines:
              inner: { input: Value, output: Value, steps: [{ name: X, cardinality: ONE_TO_ONE, input: Value, output: Value }] }
              inner: { input: Value, output: Value, steps: [{ name: Y, cardinality: ONE_TO_ONE, input: Value, output: Value }] }
            steps: [{ name: Root, cardinality: ONE_TO_ONE, input: Value, output: Value }]
            """);

        RuntimeException exception = assertThrows(RuntimeException.class,
            () -> new PipelineTemplateConfigLoader().load(configPath));

        assertTrue(exception.getMessage().contains("duplicate key inner"), exception.getMessage());
    }

    @Test
    void rejectsEmptyV3LocalPipelineDefinition() throws Exception {
        Path configPath = tempDir.resolve("empty-v3-local-pipeline.yaml");
        Files.writeString(configPath, """
            version: 3
            appName: Empty catalog entry
            basePackage: com.example.local
            contract: { input: Value, output: Value }
            types: { Value: { fields: [[id, string]] } }
            pipelines:
              empty:
                input: Value
                output: Value
                steps: []
            steps: []
            """);

        IllegalStateException failure = assertThrows(
            IllegalStateException.class,
            () -> new PipelineTemplateConfigLoader().load(configPath));
        assertTrue(failure.getMessage().contains("Pipeline definition 'empty' requires at least one step"));
    }

    @Test
    void rejectsPipelineCompositionSyntaxBeforeV3() throws Exception {
        Path catalog = tempDir.resolve("v2-pipeline-catalog.yaml");
        Files.writeString(catalog, """
            version: 2
            pipelines: { inner: { input: Value, output: Value, steps: [] } }
            steps: []
            """);
        IllegalStateException catalogFailure = assertThrows(IllegalStateException.class,
            () -> new PipelineTemplateConfigLoader().load(catalog));
        assertEquals("Top-level 'pipelines' requires version: 3", catalogFailure.getMessage());

        Path invocation = tempDir.resolve("v2-pipeline-invocation.yaml");
        Files.writeString(invocation, """
            version: 2
            steps:
              - { name: Call inner, pipeline: inner }
            """);
        IllegalStateException invocationFailure = assertThrows(IllegalStateException.class,
            () -> new PipelineTemplateConfigLoader().load(invocation));
        assertEquals("Step property 'pipeline' requires version: 3", invocationFailure.getMessage());
    }

    @Test
    void resolvesContributedProtocolTypeInOrdinaryV3Union() throws Exception {
        Path configPath = write("contributed-union.yaml", """
            version: 3
            appName: Protocol Types
            basePackage: com.example.protocol
            transport: LOCAL
            types:
              Recommendation:
                fields:
                  - [message, string]
              Decision:
                variants:
                  call: <tpf.test.ProtocolCall>
                  complete: Recommendation
            contract:
              input: Recommendation
              output: Decision
            steps:
              - name: Decide
                cardinality: ONE_TO_ONE
                input: Recommendation
                output: Decision
            """);

        PipelineTemplateConfig config = loader(protocolCall("tpf.test")).load(configPath);

        PipelineTemplateTypeDefinition.UnionType decision = (PipelineTemplateTypeDefinition.UnionType)
            config.typeModel().definition("Decision").orElseThrow();
        assertEquals("ProtocolCall", decision.variants().get("call").payload().name());
        assertEquals("tpf.test.ProtocolCall",
            config.typeModel().contributedTypeIdentity("ProtocolCall").orElseThrow().qualifiedName());
        assertTrue(config.typeModel().definition("ProtocolCall").isPresent());
        assertEquals("tpf.test.ProtocolCall", PipelineIdlSnapshot.from(config).types().get("ProtocolCall")
            .contributedIdentity().orElseThrow());
    }

    @Test
    void resolvesUniqueShortReferenceAndReportsUnknownOrAmbiguousReferences() throws Exception {
        Path unique = write("contributed-short.yaml", protocolUnionYaml("<ProtocolCall>"));
        PipelineTemplateConfig config = loader(protocolCall("tpf.test")).load(unique);
        assertTrue(config.typeModel().contains("ProtocolCall"));

        IllegalArgumentException unknown = assertThrows(IllegalArgumentException.class,
            () -> loader(protocolCall("tpf.test")).load(write("contributed-unknown.yaml", protocolUnionYaml("<Missing>"))));
        assertTrue(unknown.getMessage().contains("unknown contributed protocol type '<Missing>'"));

        ProtocolTypeRegistry ambiguousRegistry = registry(protocolCall("tpf.alpha"), protocolCall("tpf.beta"));
        IllegalArgumentException ambiguous = assertThrows(IllegalArgumentException.class,
            () -> loader(ambiguousRegistry).load(write("contributed-ambiguous.yaml", protocolUnionYaml("<ProtocolCall>"))));
        assertTrue(ambiguous.getMessage().contains("tpf.alpha.ProtocolCall, tpf.beta.ProtocolCall"));
    }

    @Test
    void normalizesContributedContractsAndImportsOnlyTheirDependencyClosure() throws Exception {
        Path configPath = write("contributed-contracts.yaml", """
            version: 3
            appName: Protocol Contracts
            basePackage: com.example.protocol
            transport: LOCAL
            types:
              Marker:
                fields: [[value, string]]
            contract:
              input: <tpf.test.ProtocolCall>
              output: <tpf.test.ProtocolCall>
            steps:
              - name: Dispatch
                cardinality: ONE_TO_ONE
                input: <ProtocolCall>
                output: <ProtocolCall>
                accepts: [<ProtocolCall>]
            """);
        ProtocolTypeDescriptor observation = protocolObservation();
        ProtocolTypeDescriptor call = new ProtocolTypeDescriptor(
            new ProtocolTypeIdentity(ConnectorProviderId.of("tpf.test"), "ProtocolCall"),
            new PipelineTemplateTypeDefinition.RecordType("ProtocolCall", List.of(
                new PipelineTemplateTypeDefinition.Field("observation",
                    new PipelineTemplateTypeReference.Contributed("tpf.test.ProtocolObservation")))));
        ProtocolTypeDescriptor unused = new ProtocolTypeDescriptor(
            new ProtocolTypeIdentity(ConnectorProviderId.of("tpf.test"), "UnusedProtocol"),
            new PipelineTemplateTypeDefinition.RecordType("UnusedProtocol", List.of()));

        PipelineTemplateConfig config = loader(call, observation, unused).load(configPath);

        assertEquals("ProtocolCall", config.inputContract());
        assertEquals("ProtocolCall", config.outputContract());
        assertEquals("ProtocolCall", config.steps().getFirst().inputTypeName());
        assertEquals(List.of("ProtocolCall"), config.steps().getFirst().accepts());
        assertTrue(config.typeModel().contains("ProtocolObservation"));
        assertFalse(config.typeModel().contains("UnusedProtocol"));
    }

    @Test
    void normalizesCallableInputAgainstTheCanonicalV3TypeGraph() throws Exception {
        Path configPath = write("llm-callables.yaml", """
            version: 3
            appName: LLM Query
            basePackage: com.example.llm
            transport: LOCAL
            types:
              State: { fields: [[id, string]] }
              ChargeArguments: { fields: [[id, string]] }
              Complete: { fields: [[status, string]] }
              Decision:
                variants:
                  call: <tpf.llm.AgentCall>
                  complete: Complete
            steps:
              - name: Decide
                kind: query
                cardinality: ONE_TO_ONE
                input: State
                output: Decision
                callables:
                  charge:
                    using: payments
                    operation: charge.create
                    operationVersion: 2
                    kind: command
                    input: ChargeArguments
            """);
        ProtocolTypeDescriptor agentCall = new ProtocolTypeDescriptor(
            new ProtocolTypeIdentity(ConnectorProviderId.of("tpf.llm"), "AgentCall"),
            new PipelineTemplateTypeDefinition.RecordType("AgentCall", List.of(
                new PipelineTemplateTypeDefinition.Field("binding", new PipelineTemplateTypeReference.Scalar("string")),
                new PipelineTemplateTypeDefinition.Field("operation", new PipelineTemplateTypeReference.Scalar("string")),
                new PipelineTemplateTypeDefinition.Field("argumentsJson", new PipelineTemplateTypeReference.Scalar("string")))));

        PipelineTemplateStep step = loader(agentCall).load(configPath).steps().getFirst();

        var callable = step.callables().get("charge");
        assertEquals("payments", callable.using());
        assertEquals("charge.create", callable.operation());
        assertEquals(2, callable.operationVersion());
        assertEquals("ChargeArguments", callable.input());
    }

    @Test
    void rejectsCallableInputOutsideTheCanonicalV3TypeGraph() throws Exception {
        Path configPath = write("unknown-llm-callable-input.yaml", """
            version: 3
            appName: LLM Query
            basePackage: com.example.llm
            transport: LOCAL
            types:
              State: { fields: [[id, string]] }
            steps:
              - name: Decide
                kind: query
                cardinality: ONE_TO_ONE
                input: State
                output: State
                callables:
                  charge: { using: payments, operation: charge.create, kind: command, input: MissingArguments }
            """);

        IllegalStateException failure = assertThrows(IllegalStateException.class,
            () -> loader().load(configPath));

        assertTrue(failure.getMessage().contains("unknown input type 'MissingArguments'"), failure.getMessage());
    }

    private Path write(String name, String content) throws Exception {
        Path path = tempDir.resolve(name);
        Files.writeString(path, content);
        return path;
    }

    private static PipelineTemplateConfigLoader loader(ProtocolTypeDescriptor... descriptors) {
        return loader(registry(descriptors));
    }

    private static PipelineTemplateConfigLoader loader(ProtocolTypeRegistry registry) {
        return new PipelineTemplateConfigLoader(key -> null, key -> null, warning -> { }, registry);
    }

    private static ProtocolTypeRegistry registry(ProtocolTypeDescriptor... descriptors) {
        return new ProtocolTypeRegistry(List.of(descriptors), new ConnectorProviderManifestCatalog(List.of()));
    }

    private static ProtocolTypeDescriptor protocolCall(String namespace) {
        return new ProtocolTypeDescriptor(
            new ProtocolTypeIdentity(ConnectorProviderId.of(namespace), "ProtocolCall"),
            new PipelineTemplateTypeDefinition.RecordType("ProtocolCall", List.of(
                new PipelineTemplateTypeDefinition.Field("operation", new PipelineTemplateTypeReference.Scalar("string")))));
    }

    private static ProtocolTypeDescriptor protocolObservation() {
        return new ProtocolTypeDescriptor(
            new ProtocolTypeIdentity(ConnectorProviderId.of("tpf.test"), "ProtocolObservation"),
            new PipelineTemplateTypeDefinition.RecordType("ProtocolObservation", List.of(
                new PipelineTemplateTypeDefinition.Field("value", new PipelineTemplateTypeReference.Scalar("string")))));
    }

    private static String protocolUnionYaml(String reference) {
        return """
            version: 3
            appName: Protocol Types
            basePackage: com.example.protocol
            transport: LOCAL
            types:
              Recommendation:
                fields:
                  - [message, string]
              Decision:
                variants:
                  call: %s
                  complete: Recommendation
            steps:
              - name: Decide
                cardinality: ONE_TO_ONE
                input: Recommendation
                output: Decision
            """.formatted(reference);
    }

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
        assertEquals(java.util.Optional.empty(), step.pipelineReference());
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
    void loadsCanonicalCsvPaymentsConfiguration() throws Exception {
        Path configPath = Path.of("..", "..", "examples", "csv-payments", "config", "pipeline.yaml")
            .toRealPath();

        PipelineTemplateConfig config = new PipelineTemplateConfigLoader().load(configPath);

        assertEquals(PipelineTemplateDialect.V3, config.dialect());
        RepresentationMapping mapping = config.typeModel()
            .representationMapping("PaymentOutput", "persistence")
            .orElseThrow();
        assertEquals("org.pipelineframework.csv.common.domain.PaymentOutput", mapping.representationType().orElseThrow());
        assertEquals("org.pipelineframework.csv.common.mapper.PaymentOutputPersistenceMapper", mapping.mapperType().orElseThrow());
        assertEquals("CsvPaymentsInputFile", config.steps().getFirst().inputTypeName());
        assertEquals("PaymentRecord", config.steps().getFirst().outputTypeName());
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
    void acceptsConnectorBindingsUsedByProviderBackedSteps() throws Exception {
        String yaml = """
            version: 3
            appName: "Test App"
            basePackage: "com.example.test"
            transport: "GRPC"
            types:
              FooInput: { fields: [[id, string]] }
              FooOutput: { fields: [[id, string]] }
            connectors:
              model: { provider: llm.query, version: 1, config: { model: qwen3 } }
            steps:
              - name: Analyse
                kind: query
                cardinality: ONE_TO_ONE
                input: FooInput
                output: FooOutput
                operation: decide
                operationVersion: 1
                using: model
            """;
        Path configPath = tempDir.resolve("pipeline-config-connectors.yaml");
        Files.writeString(configPath, yaml);

        PipelineTemplateConfig config = new PipelineTemplateConfigLoader().load(configPath);
        var operation = new PipelineYamlConfigLoader().load(configPath).steps().getFirst()
            .operationSelection().orElseThrow();

        assertEquals(3, config.version());
        assertEquals("FooInput", config.steps().getFirst().inputTypeName());
        assertEquals("FooOutput", config.steps().getFirst().outputTypeName());
        assertEquals("decide", operation.operation());
        assertEquals("model", operation.using());
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
        assertTrue(warnings.contains("Top-level 'messages' is deprecated; use 'types'."));
        assertTrue(warnings.stream().anyMatch(warning -> warning.contains("deprecated authored number")));
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
        assertTrue(exception.getMessage().contains("[nonBlankName, type]"));
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
        assertTrue(warnings.contains("Step 'payment' uses deprecated fully qualified 'input/output' contracts; use logical "
            + "input/output with java.input/java.output instead."));
        assertTrue(warnings.stream().anyMatch(warning -> warning.contains("deprecated authored number")));
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

    @Test
    void loadsV3AlgebraicTypesWithoutLeakingWireMetadata() throws Exception {
        Path configPath = tempDir.resolve("v3-types.yaml");
        Files.writeString(configPath, """
            version: 3
            appName: V3 Types
            basePackage: com.example.v3
            transport: GRPC
            types:
              OrderId:
                wraps: uuid
              Description:
                alias: string
              PaymentApproved:
                fields:
                  - [orderId, OrderId]
                  - name: description
                    type: Description
              PaymentOutcome:
                variants:
                  approved: PaymentApproved
            steps:
              - name: process
                cardinality: ONE_TO_ONE
                input: PaymentApproved
                output: PaymentOutcome
            """);

        PipelineTemplateConfig config = new PipelineTemplateConfigLoader().load(configPath);

        assertEquals(PipelineTemplateDialect.V3, config.dialect());
        assertTrue(config.messages().isEmpty());
        assertTrue(config.unions().isEmpty());
        assertEquals(4, config.typeModel().definitions().size());
        assertEquals(List.of("OrderId", "Description", "PaymentApproved", "PaymentOutcome"),
            config.typeModel().definitions().keySet().stream().toList());
        assertTrue(config.typeModel().isAssignable("Description", "Description"));
        assertTrue(config.typeModel().isAssignable("string", "Description"));
        assertFalse(config.typeModel().isAssignable("OrderId", "Description"));
        assertTrue(config.typeModel().isAssignable("PaymentApproved", "PaymentOutcome"));

        PipelineIdlSnapshot snapshot = PipelineIdlSnapshot.from(config);
        assertEquals(List.of("description", "order_id"), snapshot.types().get("PaymentApproved").fields().stream()
            .map(PipelineIdlSnapshot.TypeFieldSnapshot::protoName).toList());
        assertEquals(List.of(1, 2), snapshot.types().get("PaymentApproved").fields().stream()
            .map(PipelineIdlSnapshot.TypeFieldSnapshot::number).toList());
    }

    @Test
    void loadsV3RepeatedFieldsAsOrderedValueMultiplicity() throws Exception {
        Path configPath = tempDir.resolve("v3-repeated-fields.yaml");
        Files.writeString(configPath, """
            version: 3
            appName: V3 Repeated Fields
            basePackage: com.example.v3
            transport: GRPC
            types:
              LineItem:
                fields: [[sku, string]]
              Payment:
                fields:
                  - [paymentId, uuid]
                  - name: tags
                    repeated: string
                  - name: lineItems
                    repeated: LineItem
            steps: [{ name: process, cardinality: ONE_TO_ONE, input: Payment, output: Payment }]
            """);

        PipelineTemplateConfig config = new PipelineTemplateConfigLoader().load(configPath);
        PipelineTemplateTypeDefinition.RecordType payment = (PipelineTemplateTypeDefinition.RecordType)
            config.typeModel().definitions().get("Payment");

        assertEquals(List.of(false, true, true), payment.fields().stream()
            .map(PipelineTemplateTypeDefinition.Field::repeated).toList());
        assertEquals(List.of("uuid", "string", "LineItem"), payment.fields().stream()
            .map(field -> field.type().name()).toList());
    }

    @Test
    void rejectsAmbiguousAndLegacyV3RepeatedFieldForms() throws Exception {
        Path both = tempDir.resolve("v3-repeated-both.yaml");
        Files.writeString(both, repeatedFieldTemplate("type: LineItem\nrepeated: LineItem"));
        IllegalStateException bothError = assertThrows(IllegalStateException.class,
            () -> new PipelineTemplateConfigLoader().load(both));
        assertTrue(bothError.getMessage().contains("exactly one of 'type' or 'repeated'"), bothError.getMessage());

        Path legacyBoolean = tempDir.resolve("v3-repeated-boolean.yaml");
        Files.writeString(legacyBoolean, repeatedFieldTemplate("repeated: true"));
        IllegalStateException booleanError = assertThrows(IllegalStateException.class,
            () -> new PipelineTemplateConfigLoader().load(legacyBoolean));
        assertTrue(booleanError.getMessage().contains("supported scalar or named type"), booleanError.getMessage());
    }

    private String repeatedFieldTemplate(String fieldDeclaration) {
        return """
            version: 3
            appName: Invalid Repeated Field
            basePackage: com.example.v3
            transport: GRPC
            types:
              LineItem: { fields: [[sku, string]] }
              Payment:
                fields:
                  - name: lineItems
            """ + fieldDeclaration.indent(8) + """
            steps: [{ name: process, cardinality: ONE_TO_ONE, input: Payment, output: Payment }]
            """;
    }

    @Test
    void loadsOptionalV3RepresentationMappingsOutsideWireIdentity() throws Exception {
        Path configPath = tempDir.resolve("v3-representation-mappings.yaml");
        Files.writeString(configPath, """
            version: 3
            appName: V3 Representation Mappings
            basePackage: com.example.v3
            transport: GRPC
            types:
              Payment:
                fields: [[id, uuid]]
                mappings:
                  persistence:
                    type: com.example.persistence.PaymentEntity
                    mapper: com.example.persistence.PaymentEntityMapper
                  json: {}
            steps: [{ name: process, cardinality: ONE_TO_ONE, input: Payment, output: Payment }]
            """);

        PipelineTemplateConfig config = new PipelineTemplateConfigLoader().load(configPath);

        RepresentationMapping persistence = config.typeModel().representationMapping("Payment", "persistence").orElseThrow();
        assertEquals("Payment", persistence.domainType());
        assertEquals("com.example.persistence.PaymentEntity", persistence.representationType().orElseThrow());
        assertEquals("com.example.persistence.PaymentEntityMapper", persistence.mapperType().orElseThrow());
        assertTrue(config.typeModel().representationMapping("Payment", "json").orElseThrow().representationType().isEmpty());
        Path mappingFreeConfigPath = tempDir.resolve("v3-representation-mappings-free.yaml");
        Files.writeString(mappingFreeConfigPath, """
            version: 3
            appName: V3 Representation Mappings
            basePackage: com.example.v3
            transport: GRPC
            types:
              Payment:
                fields: [[id, uuid]]
            steps: [{ name: process, cardinality: ONE_TO_ONE, input: Payment, output: Payment }]
            """);
        PipelineIdlSnapshot mappedSnapshot = PipelineIdlSnapshot.from(config);
        assertEquals(mappedSnapshot, PipelineIdlSnapshot.from(new PipelineTemplateConfigLoader().load(mappingFreeConfigPath)));
        assertTrue(mappedSnapshot.types().containsKey("Payment"));
    }

    @Test
    void representationMappingConformanceCoversEveryNamedV3TypeKind() throws Exception {
        Path configPath = tempDir.resolve("v3-representation-mapping-conformance.yaml");
        Files.writeString(configPath, """
            version: 3
            appName: V3 Representation Mapping Conformance
            basePackage: com.example.v3
            transport: GRPC
            types:
              Payment:
                fields: [[id, uuid]]
                mappings:
                  record-boundary:
                    type: com.example.RecordRepresentation
              PaymentId:
                wraps: uuid
                mappings:
                  wrapper-boundary:
                    mapper: com.example.WrapperMapper
              PaymentAlias:
                alias: Payment
                mappings:
                  alias-boundary: {}
              PaymentOutcome:
                variants:
                  accepted: Payment
                mappings:
                  union-boundary: {}
            steps: [{ name: process, cardinality: ONE_TO_ONE, input: Payment, output: Payment }]
            """);

        PipelineTemplateTypeModel typeModel = new PipelineTemplateConfigLoader().load(configPath).typeModel();

        assertEquals("Payment", typeModel.representationMapping("Payment", "record-boundary").orElseThrow().domainType());
        assertEquals("com.example.RecordRepresentation",
            typeModel.representationMapping("Payment", "record-boundary").orElseThrow().representationType().orElseThrow());
        assertEquals("com.example.WrapperMapper",
            typeModel.representationMapping("PaymentId", "wrapper-boundary").orElseThrow().mapperType().orElseThrow());
        assertTrue(typeModel.representationMapping("PaymentAlias", "alias-boundary").orElseThrow().representationType().isEmpty());
        assertTrue(typeModel.representationMapping("PaymentOutcome", "union-boundary").orElseThrow().mapperType().isEmpty());
        assertTrue(typeModel.representationMapping("Payment", "missing-boundary").isEmpty());
    }

    @Test
    void rejectsDuplicateV3RepresentationMappingKeysDeterministically() throws Exception {
        Path configPath = tempDir.resolve("v3-duplicate-representation-mapping.yaml");
        Files.writeString(configPath, """
            version: 3
            appName: Duplicate mappings
            basePackage: com.example.v3
            transport: GRPC
            types:
              Payment:
                fields: [[id, uuid]]
                mappings:
                  persistence: {}
                  persistence: {}
            steps: [{ name: process, cardinality: ONE_TO_ONE, input: Payment, output: Payment }]
            """);

        RuntimeException error = assertThrows(RuntimeException.class,
            () -> new PipelineTemplateConfigLoader().load(configPath));

        assertTrue(error.getMessage().contains("duplicate key"), error.getMessage());
    }

    @Test
    void rejectsMalformedV3RepresentationMappings() throws Exception {
        Path invalidValue = tempDir.resolve("v3-invalid-representation-mapping.yaml");
        Files.writeString(invalidValue, """
            version: 3
            appName: V3 Representation Mappings
            basePackage: com.example.v3
            transport: GRPC
            types:
              Payment:
                fields: [[id, uuid]]
                mappings:
                  persistence: com.example.PaymentEntity
            steps: [{ name: process, cardinality: ONE_TO_ONE, input: Payment, output: Payment }]
            """);
        IllegalStateException valueError = assertThrows(IllegalStateException.class,
            () -> new PipelineTemplateConfigLoader().load(invalidValue));
        assertTrue(valueError.getMessage().contains("Payment") && valueError.getMessage().contains("persistence"));

        Path invalidProperty = tempDir.resolve("v3-invalid-representation-mapping-property.yaml");
        Files.writeString(invalidProperty, """
            version: 3
            appName: V3 Representation Mappings
            basePackage: com.example.v3
            transport: GRPC
            types:
              Payment:
                fields: [[id, uuid]]
                mappings:
                  persistence:
                    type: com.example.PaymentEntity
                    unexpected: value
            steps: [{ name: process, cardinality: ONE_TO_ONE, input: Payment, output: Payment }]
            """);
        IllegalStateException propertyError = assertThrows(IllegalStateException.class,
            () -> new PipelineTemplateConfigLoader().load(invalidProperty));
        assertTrue(propertyError.getMessage().contains("Payment") && propertyError.getMessage().contains("unexpected"));
    }

    @Test
    void loadsTargetNeutralV3WrapperConstraints() throws Exception {
        Path configPath = tempDir.resolve("v3-wrapper-constraints.yaml");
        Files.writeString(configPath, """
            version: 3
            appName: V3 Constraints
            basePackage: com.example.v3
            transport: GRPC
            types:
              CurrencyCode:
                wraps: string
                minLength: 3
                maxLength: 3
                pattern: "[A-Z]{3}"
              CurrencyAlias:
                alias: CurrencyCode
              ContactEmail:
                wraps: string
                format: email
              PositiveAmount:
                wraps: decimal
                minimumExclusive: 0
              Payment:
                fields: [[currency, CurrencyAlias], [amount, PositiveAmount]]
            steps:
              - name: process
                cardinality: ONE_TO_ONE
                input: Payment
                output: Payment
            """);

        PipelineTemplateConfig config = new PipelineTemplateConfigLoader().load(configPath);
        PipelineTemplateTypeDefinition.WrapperType currency = (PipelineTemplateTypeDefinition.WrapperType)
            config.typeModel().definitions().get("CurrencyCode");
        PipelineTemplateTypeDefinition.WrapperType amount = (PipelineTemplateTypeDefinition.WrapperType)
            config.typeModel().definitions().get("PositiveAmount");

        assertEquals(3, currency.constraints().minLength().orElseThrow());
        assertEquals("[A-Z]{3}", currency.constraints().pattern().orElseThrow());
        assertEquals(PipelineTemplateWrapperConstraints.Format.EMAIL,
            ((PipelineTemplateTypeDefinition.WrapperType) config.typeModel().definitions().get("ContactEmail"))
                .constraints().format().orElseThrow());
        assertEquals(0, amount.constraints().minimumExclusive().orElseThrow().compareTo(java.math.BigDecimal.ZERO));
        assertEquals(amount.constraints(), ((PipelineTemplateTypeDefinition.WrapperType) config.typeModel().definitions()
            .get("PositiveAmount")).constraints());
        assertEquals(new PipelineTemplateTypeReference.Named("CurrencyCode"),
            config.typeModel().resolveAliases(new PipelineTemplateTypeReference.Named("CurrencyAlias")));
    }

    @Test
    void rejectsInvalidV3WrapperConstraintPlacementApplicabilityAndIntervals() throws Exception {
        Path fieldConstraint = tempDir.resolve("v3-field-constraint.yaml");
        Files.writeString(fieldConstraint, """
            version: 3
            appName: V3 Constraints
            basePackage: com.example.v3
            transport: GRPC
            types:
              Payment:
                fields: [[id, string]]
                pattern: ".*"
            steps: [{ name: process, cardinality: ONE_TO_ONE, input: Payment, output: Payment }]
            """);
        IllegalStateException placement = assertThrows(IllegalStateException.class,
            () -> new PipelineTemplateConfigLoader().load(fieldConstraint));
        assertTrue(placement.getMessage().contains("only beside wraps"));

        Path invalid = tempDir.resolve("v3-invalid-wrapper-constraints.yaml");
        Files.writeString(invalid, """
            version: 3
            appName: V3 Constraints
            basePackage: com.example.v3
            transport: GRPC
            types:
              Identifier:
                wraps: uuid
                minLength: 1
            steps: [{ name: process, cardinality: ONE_TO_ONE, input: Identifier, output: Identifier }]
            """);
        IllegalStateException applicability = assertThrows(IllegalStateException.class,
            () -> new PipelineTemplateConfigLoader().load(invalid));
        assertTrue(applicability.getMessage().contains("only when wraps: string"));

        Path emptyInterval = tempDir.resolve("v3-empty-wrapper-interval.yaml");
        Files.writeString(emptyInterval, """
            version: 3
            appName: V3 Constraints
            basePackage: com.example.v3
            transport: GRPC
            types:
              Amount:
                wraps: decimal
                minimumExclusive: 1
                maximum: 1
            steps: [{ name: process, cardinality: ONE_TO_ONE, input: Amount, output: Amount }]
            """);
        IllegalStateException interval = assertThrows(IllegalStateException.class,
            () -> new PipelineTemplateConfigLoader().load(emptyInterval));
        assertTrue(interval.getMessage().contains("empty numeric constraint interval"));

        Path unboundedPattern = tempDir.resolve("v3-unbounded-wrapper-pattern.yaml");
        Files.writeString(unboundedPattern, """
            version: 3
            appName: V3 Constraints
            basePackage: com.example.v3
            transport: GRPC
            types:
              AccountCode:
                wraps: string
                pattern: "[A-Z]+"
            steps: [{ name: process, cardinality: ONE_TO_ONE, input: AccountCode, output: AccountCode }]
            """);
        IllegalStateException pattern = assertThrows(IllegalStateException.class,
            () -> new PipelineTemplateConfigLoader().load(unboundedPattern));
        assertTrue(pattern.getMessage().contains("pattern requires maxLength"));

        Path unsafePattern = tempDir.resolve("v3-unsafe-wrapper-pattern.yaml");
        Files.writeString(unsafePattern, """
            version: 3
            appName: V3 Constraints
            basePackage: com.example.v3
            transport: GRPC
            types:
              AccountCode:
                wraps: string
                pattern: "(a+)+"
                maxLength: 128
            steps: [{ name: process, cardinality: ONE_TO_ONE, input: AccountCode, output: AccountCode }]
            """);
        IllegalStateException unsafe = assertThrows(IllegalStateException.class,
            () -> new PipelineTemplateConfigLoader().load(unsafePattern));
        assertTrue(unsafe.getMessage().contains("unsafe for runtime model validation"));
    }

    @Test
    void rejectsV3WireMetadataAndLegacyContractSyntaxIndependently() throws Exception {
        Path configPath = tempDir.resolve("v3-wire-metadata.yaml");
        Files.writeString(configPath, """
            version: 3
            appName: V3 Types
            basePackage: com.example.v3
            transport: GRPC
            types:
              Payment:
                fields:
                  - number: 1
                    name: id
                    type: uuid
            steps:
              - name: process
                cardinality: ONE_TO_ONE
                inputTypeName: Payment
                output: Payment
            """);

        IllegalStateException exception = assertThrows(IllegalStateException.class,
            () -> new PipelineTemplateConfigLoader().load(configPath));

        assertTrue(exception.getMessage().contains("cannot declare protobuf wire metadata"));

        Path legacyContracts = tempDir.resolve("v3-legacy-contracts.yaml");
        Files.writeString(legacyContracts, """
            version: 3
            appName: V3 Types
            basePackage: com.example.v3
            transport: GRPC
            types:
              Payment:
                fields: [[id, uuid]]
            steps:
              - name: process
                cardinality: ONE_TO_ONE
                inputTypeName: Payment
                output: Payment
            """);

        IllegalStateException legacyException = assertThrows(IllegalStateException.class,
            () -> new PipelineTemplateConfigLoader().load(legacyContracts));

        assertTrue(legacyException.getMessage().contains("inputTypeName/outputTypeName are not supported in version: 3"));

        Path unknownContract = tempDir.resolve("v3-unknown-contract.yaml");
        Files.writeString(unknownContract, """
            version: 3
            appName: V3 Types
            basePackage: com.example.v3
            transport: GRPC
            types:
              Payment:
                fields: [[id, uuid]]
            steps:
              - name: process
                cardinality: ONE_TO_ONE
                input: MissingPayment
                output: Payment
            """);

        IllegalStateException unknownException = assertThrows(IllegalStateException.class,
            () -> new PipelineTemplateConfigLoader().load(unknownContract));

        assertTrue(unknownException.getMessage().contains("references unknown input type 'MissingPayment'"));
    }

    @Test
    void rejectsNullReferencesAndMapReferenceCyclesInTheNormalizedTypeModel() {
        IllegalStateException nullAlias = assertThrows(IllegalStateException.class,
            () -> new PipelineTemplateTypeModel(Map.of(
                "Alias", new PipelineTemplateTypeDefinition.AliasType("Alias", null))));
        assertTrue(nullAlias.getMessage().contains("invalid type reference"));

        IllegalStateException nullVariant = assertThrows(IllegalStateException.class,
            () -> new PipelineTemplateTypeModel(Map.of(
                "Outcome", new PipelineTemplateTypeDefinition.UnionType("Outcome", Map.of(
                    "missing", new PipelineTemplateTypeDefinition.Variant("missing", null))))));
        assertTrue(nullVariant.getMessage().contains("invalid type reference"));

        IllegalStateException mapCycle = assertThrows(IllegalStateException.class,
            () -> new PipelineTemplateTypeModel(Map.of(
                "A", new PipelineTemplateTypeDefinition.AliasType("A",
                    new PipelineTemplateTypeReference.MapType(
                        new PipelineTemplateTypeReference.Scalar("string"),
                        new PipelineTemplateTypeReference.Named("A"))))));
        assertTrue(mapCycle.getMessage().contains("Recursive v3 type reference"));
    }

    @Test
    void rejectsV3RecursiveAndUnknownReferences() throws Exception {
        Path configPath = tempDir.resolve("v3-recursive.yaml");
        Files.writeString(configPath, """
            version: 3
            appName: V3 Types
            basePackage: com.example.v3
            transport: GRPC
            types:
              A:
                fields: [[next, B]]
              B:
                alias: A
            steps:
              - name: process
                cardinality: ONE_TO_ONE
                input: A
                output: A
            """);

        IllegalStateException exception = assertThrows(IllegalStateException.class,
            () -> new PipelineTemplateConfigLoader().load(configPath));

        assertTrue(exception.getMessage().contains("Recursive v3 type reference"));
    }

    @Test
    void validatesV3ContractLinksAndRejectsMaterialization() throws Exception {
        Path incompatible = tempDir.resolve("v3-incompatible-links.yaml");
        Files.writeString(incompatible, """
            version: 3
            appName: V3 Links
            basePackage: com.example.v3
            transport: GRPC
            contract:
              input: Request
            types:
              Request: { fields: [[id, uuid]] }
              OtherRequest: { fields: [[id, uuid]] }
              Result: { fields: [[id, uuid]] }
            steps:
              - name: process
                cardinality: ONE_TO_ONE
                input: OtherRequest
                output: Result
            """);

        IllegalStateException linkError = assertThrows(IllegalStateException.class,
            () -> new PipelineTemplateConfigLoader().load(incompatible));
        assertTrue(linkError.getMessage().contains("Pipeline input contract 'Request' is not assignable"));

        Path materialized = tempDir.resolve("v3-materialization.yaml");
        Files.writeString(materialized, """
            version: 3
            appName: V3 Materialization
            basePackage: com.example.v3
            transport: GRPC
            types:
              Payment: { fields: [[id, uuid]] }
            materialization:
              aspects:
                - name: payload
                  message: Payment
                  action: REFERENCE
            steps:
              - name: process
                cardinality: ONE_TO_ONE
                input: Payment
                output: Payment
            """);

        IllegalStateException materializationError = assertThrows(IllegalStateException.class,
            () -> new PipelineTemplateConfigLoader().load(materialized));
        assertEquals("Version: 3 does not support materialization declarations.", materializationError.getMessage());
    }

    @Test
    void rejectsV3FlowBetweenOverlappingButIncompatibleUnions() throws Exception {
        Path configPath = tempDir.resolve("v3-overlapping-unions.yaml");
        Files.writeString(configPath, """
            version: 3
            appName: V3 Overlapping Unions
            basePackage: com.example.v3
            transport: GRPC
            types:
              Start: { fields: [[id, uuid]] }
              Shared: { fields: [[id, uuid]] }
              SourceOnly: { fields: [[id, uuid]] }
              TargetOnly: { fields: [[id, uuid]] }
              Result: { fields: [[id, uuid]] }
              SourceUnion:
                variants:
                  shared: Shared
                  sourceOnly: SourceOnly
              TargetUnion:
                variants:
                  shared: Shared
                  targetOnly: TargetOnly
            steps:
              - name: produce
                cardinality: ONE_TO_ONE
                input: Start
                output: SourceUnion
              - name: consume
                cardinality: ONE_TO_ONE
                input: TargetUnion
                output: Result
            """);

        IllegalStateException exception = assertThrows(IllegalStateException.class,
            () -> new PipelineTemplateConfigLoader().load(configPath));

        assertTrue(exception.getMessage().contains("No preceding step output is assignable to step 'consume'"));
    }

    @Test
    void preservesLegacyMapAndScalarUnionReferencesInTheCompatibilityTypeModel() throws Exception {
        Path configPath = tempDir.resolve("v2-map-and-scalar-union.yaml");
        Files.writeString(configPath, """
            version: 2
            appName: Legacy Types
            basePackage: com.example.v2
            transport: GRPC
            types:
              Input:
                fields:
                  - name: metadata
                    type: map
                    keyType: string
                    valueType: string
            unions:
              Outcome:
                variants:
                  accepted:
                    type: string
            steps:
              - name: process
                cardinality: ONE_TO_ONE
                input: Input
                output: Outcome
            """);

        PipelineTemplateConfig config = new PipelineTemplateConfigLoader().load(configPath);
        PipelineTemplateTypeDefinition.RecordType input = (PipelineTemplateTypeDefinition.RecordType) config.typeModel()
            .definitions().get("Input");
        PipelineTemplateTypeDefinition.UnionType outcome = (PipelineTemplateTypeDefinition.UnionType) config.typeModel()
            .definitions().get("Outcome");

        assertInstanceOf(PipelineTemplateTypeReference.MapType.class, input.fields().getFirst().type());
        assertInstanceOf(PipelineTemplateTypeReference.Scalar.class, outcome.variants().get("accepted").payload());
    }

    @Test
    void rejectsRepresentationProviderConfigurationOutsideVersionThree() throws Exception {
        Path configPath = tempDir.resolve("v2-representations.yaml");
        Files.writeString(configPath, """
            version: 2
            appName: Legacy Types
            basePackage: com.example.v2
            transport: GRPC
            representations:
              opencsv: {}
            messages:
              Input: { fields: [] }
            steps: []
            """);

        IllegalStateException exception = assertThrows(IllegalStateException.class,
            () -> new PipelineTemplateConfigLoader().load(configPath));

        assertEquals("Top-level representations require version: 3", exception.getMessage());
    }
}
