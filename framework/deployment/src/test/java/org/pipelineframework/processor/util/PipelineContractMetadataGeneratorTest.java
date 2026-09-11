package org.pipelineframework.processor.util;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.StreamSupport;
import javax.annotation.processing.Filer;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.element.Element;
import javax.tools.FileObject;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.config.PlatformMode;
import org.pipelineframework.config.CardinalitySemantics;
import org.pipelineframework.config.template.PipelinePlatform;
import org.pipelineframework.config.template.PipelineFieldNullability;
import org.pipelineframework.config.template.PipelineFieldPresence;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.config.template.PipelineTemplateConfigLoader;
import org.pipelineframework.config.template.PipelineTemplateMaterialization;
import org.pipelineframework.config.template.PipelineTemplateRepeatedFieldConstraints;
import org.pipelineframework.config.template.PipelineTemplateTypeDefinition;
import org.pipelineframework.config.template.PipelineTemplateTypeModel;
import org.pipelineframework.config.template.PipelineTemplateTypeReference;
import org.pipelineframework.config.template.PipelineTemplateWrapperConstraints;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.composition.PipelineDefinition;
import org.pipelineframework.processor.composition.PipelineDefinitionLinker;
import org.pipelineframework.processor.composition.PipelineDefinitionStep;
import org.pipelineframework.processor.composition.PipelineReference;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.DeferredCompletionSelection;
import org.pipelineframework.processor.ir.ExecutionMode;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.PipelineTransport;
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.processor.ir.TypeMapping;
import org.pipelineframework.processor.mapping.PipelineRuntimeMapping;
import org.pipelineframework.parallelism.OrderingRequirement;
import org.pipelineframework.parallelism.ThreadSafety;
import org.pipelineframework.protocol.ProtocolTypeIdentity;
import org.pipelineframework.processor.block.ImportedPipelineDefinition;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PipelineContractMetadataGeneratorTest {

    @TempDir
    Path tempDir;

    @Test
    void writesDeterministicContractWithOrderedStepsAndAwaitTransport() throws IOException {
        Path pipelineYaml = writePipelineYaml();
        Path firstOutput = tempDir.resolve("first");
        Path secondOutput = tempDir.resolve("second");

        writeMetadata(pipelineYaml, firstOutput);
        writeMetadata(pipelineYaml, secondOutput);

        JsonObject first = readContract(firstOutput);
        JsonObject second = readContract(secondOutput);
        assertEquals(first.get("contractHash").getAsString(), second.get("contractHash").getAsString());
        assertEquals(first.get("contractVersion").getAsString(), second.get("contractVersion").getAsString());
        assertEquals(1, first.get("schemaVersion").getAsInt());
        assertTrue(first.getAsJsonObject("canonicalTypes").entrySet().isEmpty());
        assertTrue(first.get("canonicalCatalogFingerprint").getAsString().matches("[0-9a-f]{64}"));
        assertEquals("org.example.restaurant", first.get("pipelineId").getAsString());
        assertEquals("COMPUTE", first.get("platform").getAsString());
        assertEquals("REST", first.get("transport").getAsString());
        assertEquals("orchestrator-svc", first.get("module").getAsString());
        assertEquals("MONOLITH", first.get("runtimeLayout").getAsString());
        assertTrue(first.get("contractVersion").getAsString().startsWith("sha256:"));
        assertFalse(Files.exists(firstOutput.resolve(Path.of("META-INF", "pipeline", "bundle" + "-manifest.json"))));

        JsonArray steps = first.getAsJsonArray("steps");
        assertEquals(2, steps.size());
        assertEquals("Validate Order Request", steps.get(0).getAsJsonObject().get("authoredName").getAsString());
        assertEquals("Await Restaurant Decision", steps.get(1).getAsJsonObject().get("authoredName").getAsString());
        assertEquals("internal", steps.get(1).getAsJsonObject().get("kind").getAsString());
        JsonObject deferredCompletion = steps.get(1).getAsJsonObject().getAsJsonObject("deferredCompletion");
        assertEquals("interaction-api", deferredCompletion.get("transportType").getAsString());
        assertTrue(deferredCompletion.get("fingerprint").getAsString().matches("[0-9a-f]{64}"));
        assertEquals(
            "org.example.restaurant.domain.RestaurantDecision",
            steps.get(1).getAsJsonObject().get("outputTypeId").getAsString());

        JsonObject capabilities = first.getAsJsonObject("capabilities");
        assertTrue(capabilities.get("localTransitionExecution").getAsBoolean());
        assertEquals(4, capabilities.getAsJsonArray("transitionWorkerProtocols").size());
    }

    @Test
    void deferredCompletionFingerprintIgnoresTransportConfigInsertionOrder() throws IOException {
        Path pipelineYaml = writePipelineYaml();
        Map<String, Object> firstConfig = new LinkedHashMap<>();
        Map<String, Object> firstHeaders = new LinkedHashMap<>();
        firstHeaders.put("tenant", "north");
        firstHeaders.put("source", "orders");
        firstConfig.put("topic", "approval.requests");
        firstConfig.put("headers", firstHeaders);
        Map<String, Object> secondConfig = new LinkedHashMap<>();
        Map<String, Object> reversedHeaders = new LinkedHashMap<>();
        reversedHeaders.put("source", "orders");
        reversedHeaders.put("tenant", "north");
        secondConfig.put("headers", reversedHeaders);
        secondConfig.put("topic", "approval.requests");

        Path firstOutput = tempDir.resolve("first-transport-order");
        Path secondOutput = tempDir.resolve("second-transport-order");
        writeMetadata(pipelineYaml, firstOutput, firstConfig);
        writeMetadata(pipelineYaml, secondOutput, secondConfig);

        JsonObject firstCompletion = readContract(firstOutput).getAsJsonArray("steps").get(1)
            .getAsJsonObject().getAsJsonObject("deferredCompletion");
        JsonObject secondCompletion = readContract(secondOutput).getAsJsonArray("steps").get(1)
            .getAsJsonObject().getAsJsonObject("deferredCompletion");
        assertEquals(firstCompletion.get("transportConfigFingerprint"),
            secondCompletion.get("transportConfigFingerprint"));
        assertEquals(firstCompletion.get("fingerprint"), secondCompletion.get("fingerprint"));
    }

    @Test
    void callbackAuthorityAndAllThreeContractsParticipateInTheReleaseHash() throws IOException {
        var operation = org.pipelineframework.processor.ir.ConnectorOperationSelection.command("StartJob",
            org.pipelineframework.connector.ConnectorBindingName.of("jobs"),
            new org.pipelineframework.connector.ConnectorOperationIdentity(ConnectorProviderId.of("test.jobs"),
                "start", org.pipelineframework.connector.ConnectorOperationKind.COMMAND, 1), 1, Map.of(),
            new org.pipelineframework.processor.ir.ConnectorOperationSelection.CommandSelection(
                ClassName.get("org.example", "IdGenerator"), org.pipelineframework.command.CommandDuplicatePolicy.RETURN_RECORDED,
                org.pipelineframework.connector.CommandPolicy.none()));
        var hashes = new java.util.HashSet<String>();
        var releaseHashes = new java.util.HashSet<String>();
        record Variant(String name, String immediate, String result, String payload, String authenticator) { }
        var variants = List.of(
            new Variant("baseline", "JobAccepted", "JobResult", "JobCallback", "SignatureV1"),
            new Variant("authenticator", "JobAccepted", "JobResult", "JobCallback", "SignatureV2"),
            new Variant("immediate", "DifferentAccepted", "JobResult", "JobCallback", "SignatureV1"),
            new Variant("result", "JobAccepted", "DifferentResult", "JobCallback", "SignatureV1"),
            new Variant("payload", "JobAccepted", "JobResult", "DifferentCallback", "SignatureV1"));
        for (var variant : variants) {
            Path output = tempDir.resolve(variant.name());
            var processing = processingEnv(output, Map.of());
            var context = new PipelineCompilationContext(processing, mock(RoundEnvironment.class));
            var callback = new DeferredCompletionSelection.ResolvedConnectorCallback(
                new org.pipelineframework.connector.ConnectorOperationCallbackDescriptor("completed",
                    new org.pipelineframework.connector.ConnectorOperationTypeContract(variant.payload(), Optional.empty()), true),
                operation, ClassName.get("org.example", "EndpointResolver"), ClassName.get("org.example", variant.authenticator()));
            var completion = new DeferredCompletionSelection(ClassName.get("org.example", variant.result()), variant.result(),
                Optional.of(variant.payload()), java.time.Duration.ofMinutes(1), List.of(), "signedResumeToken", "", Map.of(),
                Optional.of(ClassName.get("org.example", variant.payload())), Optional.of(ClassName.get("org.example", "Projector")),
                Optional.of(callback));
            context.setStepModels(List.of(step("StartJob", "StartJobRequest", variant.immediate(), StreamingShape.UNARY_UNARY,
                Set.of(GenerationTarget.COMMAND_CLIENT_STEP)).toBuilder().connectorOperationSelection(operation)
                .deferredCompletionSelection(completion).build()));
            new PipelineContractMetadataGenerator(processing).writePipelineContract(context);
            var contract = readContract(output);
            var node = contract.getAsJsonArray("steps").get(0).getAsJsonObject().getAsJsonObject("deferredCompletion");
            assertEquals("CONNECTOR_CALLBACK", node.get("mode").getAsString());
            assertEquals("org.example.restaurant.domain." + variant.immediate(), node.get("operationOutputTypeId").getAsString());
            assertEquals("org.example." + variant.result(), node.get("finalOutputTypeId").getAsString());
            assertEquals("org.example." + variant.payload(), node.get("completionPayloadTypeId").getAsString());
            assertEquals("jobs", node.getAsJsonObject("callback").get("binding").getAsString());
            hashes.add(node.get("fingerprint").getAsString());
            releaseHashes.add(contract.get("contractHash").getAsString());
        }
        assertEquals(variants.size(), hashes.size());
        assertEquals(variants.size(), releaseHashes.size());
    }

    @Test
    void skipsContractWhenNoPipelineModelExists() throws IOException {
        ProcessingEnvironment processingEnv = processingEnv(tempDir.resolve("empty"), Map.of());
        RoundEnvironment roundEnv = mock(RoundEnvironment.class);
        PipelineCompilationContext ctx = new PipelineCompilationContext(processingEnv, roundEnv);

        PipelineContractMetadataGenerator generator = new PipelineContractMetadataGenerator(processingEnv);
        generator.writePipelineContract(ctx);

        assertFalse(Files.exists(tempDir.resolve("empty").resolve("META-INF/pipeline/pipeline-contract.json")));
    }

    @Test
    void writesStableV3CanonicalDefinitionAndCatalogFingerprints() throws IOException {
        Path pipelineYaml = writePipelineYaml();
        Path firstOutput = tempDir.resolve("v3-first");
        Path secondOutput = tempDir.resolve("v3-second");

        writeV3Metadata(pipelineYaml, firstOutput, v3TypeModel(false));
        writeV3Metadata(pipelineYaml, secondOutput, v3TypeModel(true));

        JsonObject first = readContract(firstOutput);
        JsonObject second = readContract(secondOutput);
        JsonObject firstTypes = first.getAsJsonObject("canonicalTypes");
        JsonObject secondTypes = second.getAsJsonObject("canonicalTypes");
        assertEquals(2, first.get("schemaVersion").getAsInt());
        assertEquals(List.of("Alpha", "Zeta"), firstTypes.keySet().stream().toList());
        assertEquals(first.get("canonicalCatalogFingerprint").getAsString(),
            second.get("canonicalCatalogFingerprint").getAsString());
        assertEquals(firstTypes.getAsJsonObject("Alpha").get("definitionFingerprint").getAsString(),
            secondTypes.getAsJsonObject("Alpha").get("definitionFingerprint").getAsString());
        JsonObject nestedMap = firstTypes.getAsJsonObject("Zeta").getAsJsonObject("definition")
            .getAsJsonArray("fields").get(0).getAsJsonObject().getAsJsonObject("type");
        assertEquals("map", nestedMap.get("kind").getAsString());
        assertEquals("Alpha", nestedMap.getAsJsonObject("value").get("id").getAsString());
    }

    @Test
    void repeatedFieldSemanticsAreDeterministicAndAffectTheReleaseHash() throws IOException {
        Path pipelineYaml = writePipelineYaml();
        Path singularOutput = tempDir.resolve("v3-singular");
        Path repeatedOutput = tempDir.resolve("v3-repeated");
        Path repeatedReorderedOutput = tempDir.resolve("v3-repeated-reordered");

        writeV3Metadata(pipelineYaml, singularOutput, v3TypeModel(false));
        writeV3Metadata(pipelineYaml, repeatedOutput, repeatedTypeModel(false));
        writeV3Metadata(pipelineYaml, repeatedReorderedOutput, repeatedTypeModel(true));

        JsonObject singular = readContract(singularOutput);
        JsonObject repeated = readContract(repeatedOutput);
        JsonObject repeatedReordered = readContract(repeatedReorderedOutput);
        JsonObject repeatedField = repeated.getAsJsonObject("canonicalTypes").getAsJsonObject("Zeta")
            .getAsJsonObject("definition").getAsJsonArray("fields").get(1).getAsJsonObject();

        assertTrue(repeatedField.get("repeated").getAsBoolean());
        assertEquals(1, repeatedField.get("minItems").getAsInt());
        assertEquals(3, repeatedField.get("maxItems").getAsInt());
        assertEquals(List.of("Accrual", "Cash"), repeated.getAsJsonObject("canonicalTypes")
            .getAsJsonObject("AccountingMethod").getAsJsonObject("definition").getAsJsonArray("allowedValues")
            .asList().stream().map(com.google.gson.JsonElement::getAsString).toList());
        assertNotEquals(singular.get("canonicalCatalogFingerprint").getAsString(),
            repeated.get("canonicalCatalogFingerprint").getAsString());
        assertNotEquals(singular.get("contractHash").getAsString(), repeated.get("contractHash").getAsString());
        assertEquals(repeated.get("canonicalCatalogFingerprint").getAsString(),
            repeatedReordered.get("canonicalCatalogFingerprint").getAsString());
        assertEquals(repeated.get("contractHash").getAsString(), repeatedReordered.get("contractHash").getAsString());
    }

    @Test
    void presenceAndNullabilityAffectHashesButSyntaxOnlyRewritesDoNot() throws IOException {
        Path compact = tempDir.resolve("compact.yaml");
        Path verbose = tempDir.resolve("verbose.yaml");
        String header = """
            version: 3
            appName: v3-contract
            basePackage: org.example.v3
            transport: REST
            types:
              Alpha:
                fields: [[code, string]]
              Zeta:
                fields:
            """;
        Files.writeString(compact, header + "      - [description?, string?]\n"
            + "steps: [{ name: ProcessV3, cardinality: ONE_TO_ONE, input: Alpha, output: Zeta }]\n");
        Files.writeString(verbose, header + "      - { name: description, type: string, presence: optional, nullability: nullable }\n"
            + "steps: [{ name: ProcessV3, cardinality: ONE_TO_ONE, input: Alpha, output: Zeta }]\n");

        PipelineTemplateTypeModel compactModel = new PipelineTemplateConfigLoader().load(compact).typeModel();
        PipelineTemplateTypeModel verboseModel = new PipelineTemplateConfigLoader().load(verbose).typeModel();
        PipelineTemplateTypeModel strictModel = new PipelineTemplateTypeModel(Map.of(
            "Alpha", compactModel.definitions().get("Alpha"),
            "Zeta", new PipelineTemplateTypeDefinition.RecordType("Zeta", List.of(
                new PipelineTemplateTypeDefinition.Field("description", new PipelineTemplateTypeReference.Scalar("string"))))));

        Path metadataPipeline = writePipelineYaml();
        writeV3Metadata(metadataPipeline, tempDir.resolve("compact-output"), compactModel);
        writeV3Metadata(metadataPipeline, tempDir.resolve("verbose-output"), verboseModel);
        writeV3Metadata(metadataPipeline, tempDir.resolve("strict-output"), strictModel);

        JsonObject compactContract = readContract(tempDir.resolve("compact-output"));
        JsonObject verboseContract = readContract(tempDir.resolve("verbose-output"));
        JsonObject strictContract = readContract(tempDir.resolve("strict-output"));
        JsonObject field = compactContract.getAsJsonObject("canonicalTypes").getAsJsonObject("Zeta")
            .getAsJsonObject("definition").getAsJsonArray("fields").get(0).getAsJsonObject();

        assertEquals("OPTIONAL", field.get("presence").getAsString());
        assertEquals("NULLABLE", field.get("nullability").getAsString());
        assertEquals(compactContract.get("contractHash").getAsString(), verboseContract.get("contractHash").getAsString());
        assertNotEquals(compactContract.get("contractHash").getAsString(), strictContract.get("contractHash").getAsString());
    }

    @Test
    void embedsTheResolvedCompositionInTheExistingHashedContract() throws IOException {
        Path output = tempDir.resolve("composition");
        ProcessingEnvironment processingEnv = processingEnv(output, Map.of());
        PipelineCompilationContext ctx = new PipelineCompilationContext(processingEnv, mock(RoundEnvironment.class));
        PipelineReference outer = new PipelineReference("outer");
        PipelineReference inner = new PipelineReference("inner");
        PipelineDefinition innerDefinition = new PipelineDefinition(inner, "Value", "Value", List.of(
            PipelineDefinitionStep.direct("x", "Value", "Value", CardinalitySemantics.ONE_TO_ONE),
            PipelineDefinitionStep.direct("y", "Value", "Value", CardinalitySemantics.ONE_TO_ONE)));
        PipelineDefinition outerDefinition = new PipelineDefinition(outer, "Value", "Value", List.of(
            PipelineDefinitionStep.direct("a", "Value", "Value", CardinalitySemantics.ONE_TO_ONE),
            PipelineDefinitionStep.pipeline("call-inner", "Value", "Value", inner),
            PipelineDefinitionStep.direct("c", "Value", "Value", CardinalitySemantics.ONE_TO_ONE)));
        ctx.setResolvedPipelineDefinitionGraph(new PipelineDefinitionLinker(
            reference -> java.util.Optional.ofNullable(Map.of(inner, innerDefinition).get(reference))).link(outerDefinition));
        ctx.setStepModels(List.of(step("ProcessCompositionService", "Value", "Value",
            StreamingShape.UNARY_UNARY, Set.of(GenerationTarget.REST_CLIENT_STEP))));

        new PipelineContractMetadataGenerator(processingEnv).writePipelineContract(ctx);

        JsonObject contract = readContract(output);
        assertEquals(3, contract.get("schemaVersion").getAsInt());
        assertTrue(contract.get("contractVersion").getAsString().startsWith("sha256:"));
        assertEquals("outer", contract.getAsJsonObject("composition").get("rootDefinitionId").getAsString());
        JsonArray definitions = contract.getAsJsonObject("composition").getAsJsonArray("definitions");
        JsonObject projectedOuter = StreamSupport.stream(definitions.spliterator(), false)
            .map(element -> element.getAsJsonObject())
            .filter(definition -> "outer".equals(definition.get("definitionId").getAsString()))
            .findFirst()
            .orElseThrow();
        assertEquals("ROOT_TERMINAL", projectedOuter.getAsJsonArray("continuations")
            .get(2).getAsJsonObject().get("kind").getAsString());
    }

    @Test
    void includesContributedIdentityInReleaseContractAndHash() throws IOException {
        Path pipelineYaml = writePipelineYaml();
        Path firstOutput = tempDir.resolve("contributed-first");
        Path secondOutput = tempDir.resolve("contributed-second");

        writeV3Metadata(pipelineYaml, firstOutput, contributedModel("tpf.alpha"));
        writeV3Metadata(pipelineYaml, secondOutput, contributedModel("tpf.beta"));

        JsonObject first = readContract(firstOutput);
        JsonObject second = readContract(secondOutput);
        assertEquals(3, first.get("schemaVersion").getAsInt());
        assertEquals("tpf.alpha.Alpha", first.getAsJsonObject("canonicalTypes")
            .getAsJsonObject("Alpha").get("contributedIdentity").getAsString());
        assertNotEquals(first.get("contractHash").getAsString(), second.get("contractHash").getAsString());
    }

    @Test
    void includesImportedDefinitionProvenanceInTheBreakingSchemaThreeContractAndHash() throws IOException {
        Path firstOutput = tempDir.resolve("block-first");
        Path secondOutput = tempDir.resolve("block-second");

        writeImportedDefinitionMetadata(firstOutput, "1.0.0", "sha256:first", "RETURN_RECORDED");
        writeImportedDefinitionMetadata(secondOutput, "1.1.0", "sha256:second", "FAIL");

        JsonObject first = readContract(firstOutput);
        JsonObject second = readContract(secondOutput);
        JsonObject imported = first.getAsJsonArray("importedDefinitions").get(0).getAsJsonObject();
        assertEquals(3, first.get("schemaVersion").getAsInt());
        assertEquals("org.example.documents/document-text-extraction",
            imported.get("qualifiedId").getAsString());
        assertEquals("org.example:document-text-extraction", imported.get("groupId").getAsString()
            + ":" + imported.get("artifactId").getAsString());
        assertEquals("1.0.0", imported.get("version").getAsString());
        assertEquals("sha256:linked-RETURN_RECORDED",
            imported.get("linkedDefinitionFingerprint").getAsString());
        JsonObject requirement = imported.getAsJsonArray("resolvedRequirements").get(0).getAsJsonObject();
        assertEquals("graphql.write", requirement.get("name").getAsString());
        assertEquals("primary-graphql", requirement.get("binding").getAsString());
        assertEquals("graphql.smallrye", requirement.get("provider").getAsString());
        assertEquals("execute.mutation", requirement.getAsJsonArray("operations")
            .get(0).getAsJsonObject().get("id").getAsString());
        assertEquals("RETURN_RECORDED", requirement.get("duplicatePolicy").getAsString());
        assertEquals("configuration-digest", requirement.get("connectorConfigurationDigest").getAsString());
        JsonObject callable = imported.getAsJsonArray("resolvedCallables").get(0).getAsJsonObject();
        assertEquals("Decide", callable.get("sourceStep").getAsString());
        assertEquals("update", callable.get("alias").getAsString());
        assertEquals("UpdateRequest", callable.get("inputType").getAsString());
        assertEquals("UpdateResult", callable.get("outputType").getAsString());
        assertEquals("nextEffectKey", callable.getAsJsonObject("trustedArguments")
            .get("effectKey").getAsString());
        assertEquals("RETURN_RECORDED", callable.get("duplicatePolicy").getAsString());
        assertFalse(first.toString().contains("credential"));
        assertNotEquals(first.get("contractHash").getAsString(), second.get("contractHash").getAsString());
    }

    @Test
    void emitsOneDescriptorPerAuthoredStepWhenMonolithHasClientAndServerModels() throws IOException {
        Path pipelineYaml = writePipelineYaml();
        Path output = tempDir.resolve("monolith");
        ProcessingEnvironment processingEnv = processingEnv(output, Map.of("pipeline.config", pipelineYaml.toString()));
        RoundEnvironment roundEnv = mock(RoundEnvironment.class);
        PipelineCompilationContext ctx = new PipelineCompilationContext(processingEnv, roundEnv);
        ctx.setModuleName("orchestrator-svc");
        ctx.setPlatformMode(PlatformMode.COMPUTE);
        ctx.setTransportMode(PipelineTransport.REST);
        ctx.setRuntimeMapping(new PipelineRuntimeMapping(
            PipelineRuntimeMapping.Layout.MONOLITH,
            PipelineRuntimeMapping.Validation.AUTO,
            PipelineRuntimeMapping.Defaults.defaultValues(),
            Map.of(),
            Map.of(),
            Map.of(),
            Map.of()));

        PipelineStepModel validateServer = step(
            "ProcessValidateOrderRequestService", "PlaceRestaurantOrderRequest", "ValidatedRestaurantOrderRequest",
            StreamingShape.UNARY_UNARY, Set.of(GenerationTarget.REST_RESOURCE));
        PipelineStepModel validateClient = step(
            "ProcessValidateOrderRequestService", "PlaceRestaurantOrderRequest", "ValidatedRestaurantOrderRequest",
            StreamingShape.UNARY_UNARY, Set.of(GenerationTarget.REST_CLIENT_STEP));
        ctx.setStepModels(java.util.List.of(
            validateServer,
            validateClient,
            deferredStep()));

        PipelineContractMetadataGenerator generator = new PipelineContractMetadataGenerator(processingEnv);
        generator.writePipelineContract(ctx);

        JsonArray contractSteps = readContract(output).getAsJsonArray("steps");
        assertEquals(2, contractSteps.size());
        assertEquals(
            "org.example.restaurant.pipeline.ProcessValidateOrderRequestRestClientStep",
            contractSteps.get(0).getAsJsonObject().get("clientClass").getAsString());
        assertEquals(
            "org.example.restaurant.pipeline.ProcessAwaitRestaurantDecisionDeferredCompletionStep",
            contractSteps.get(1).getAsJsonObject().get("clientClass").getAsString());
    }

    private void writeMetadata(Path pipelineYaml, Path outputDir) throws IOException {
        writeMetadata(pipelineYaml, outputDir, Map.of());
    }

    private void writeMetadata(Path pipelineYaml, Path outputDir, Map<String, Object> transportConfig)
        throws IOException {
        ProcessingEnvironment processingEnv = processingEnv(outputDir, Map.of("pipeline.config", pipelineYaml.toString()));
        RoundEnvironment roundEnv = mock(RoundEnvironment.class);
        PipelineCompilationContext ctx = new PipelineCompilationContext(processingEnv, roundEnv);
        ctx.setModuleName("orchestrator-svc");
        ctx.setPlatformMode(PlatformMode.COMPUTE);
        ctx.setTransportMode(PipelineTransport.REST);
        ctx.setRuntimeMapping(new PipelineRuntimeMapping(
            PipelineRuntimeMapping.Layout.MONOLITH,
            PipelineRuntimeMapping.Validation.AUTO,
            PipelineRuntimeMapping.Defaults.defaultValues(),
            Map.of(),
            Map.of(),
            Map.of(),
            Map.of()));
        ctx.setStepModels(java.util.List.of(
            step("ProcessValidateOrderRequestService", "PlaceRestaurantOrderRequest", "ValidatedRestaurantOrderRequest",
                StreamingShape.UNARY_UNARY, Set.of(GenerationTarget.REST_CLIENT_STEP)),
            deferredStep(transportConfig)));

        PipelineContractMetadataGenerator generator = new PipelineContractMetadataGenerator(processingEnv);
        generator.writePipelineContract(ctx);
    }

    private void writeImportedDefinitionMetadata(
        Path outputDir,
        String version,
        String fingerprint,
        String duplicatePolicy
    ) throws IOException {
        ProcessingEnvironment processingEnv = processingEnv(outputDir, Map.of());
        PipelineCompilationContext ctx = new PipelineCompilationContext(processingEnv, mock(RoundEnvironment.class));
        ctx.setStepModels(List.of(step("ProcessImportedService", "Input", "Output",
            StreamingShape.UNARY_UNARY, Set.of(GenerationTarget.LOCAL_CLIENT_STEP))));
        ctx.setImportedPipelineDefinitions(List.of(new ImportedPipelineDefinition(
            "org.example.documents/document-text-extraction", "document-text-extraction", "org.example.documents",
            "org.example", "document-text-extraction", version,
            "META-INF/pipeline/document-text-extraction.yaml", fingerprint,
            "sha256:linked-" + duplicatePolicy,
            List.of(new ImportedPipelineDefinition.ResolvedBlockRequirement(
                "graphql.write", "COMMAND", "primary-graphql", "graphql.smallrye", 1,
                List.of(new ImportedPipelineDefinition.ResolvedOperation("execute.mutation", 1)),
                "com.example.GraphQlMutationCommandId", duplicatePolicy,
                Map.of("requiredExecutionPosture", "AUTOMATED"), "configuration-digest")),
            List.of(new ImportedPipelineDefinition.ResolvedBlockCallable(
                "Decide", "update", "graphql.write", "COMMAND", "primary-graphql",
                "graphql.smallrye", 1, "execute.mutation", 1,
                "UpdateRequest", "UpdateResult", Map.of("effectKey", "nextEffectKey"),
                "com.example.GraphQlMutationCommandId", duplicatePolicy,
                Map.of("requiredExecutionPosture", "AUTOMATED"), "configuration-digest")))));
        new PipelineContractMetadataGenerator(processingEnv).writePipelineContract(ctx);
    }

    private void writeV3Metadata(Path pipelineYaml, Path outputDir, PipelineTemplateTypeModel typeModel) throws IOException {
        ProcessingEnvironment processingEnv = processingEnv(outputDir, Map.of("pipeline.config", pipelineYaml.toString()));
        RoundEnvironment roundEnv = mock(RoundEnvironment.class);
        PipelineCompilationContext ctx = new PipelineCompilationContext(processingEnv, roundEnv);
        ctx.setModuleName("orchestrator-svc");
        ctx.setPlatformMode(PlatformMode.COMPUTE);
        ctx.setTransportMode(PipelineTransport.REST);
        ctx.setPipelineTemplateConfig(new PipelineTemplateConfig(
            3, "v3-contract", "org.example.v3", "REST", PipelinePlatform.COMPUTE,
            Map.of(), Map.of(), Map.of(), Map.of(), List.of(), Map.of(), null, null,
            new PipelineTemplateMaterialization(List.of()), null, null, typeModel));
        ctx.setStepModels(List.of(step("ProcessV3Service", "Alpha", "Zeta",
            StreamingShape.UNARY_UNARY, Set.of(GenerationTarget.REST_CLIENT_STEP))));
        new PipelineContractMetadataGenerator(processingEnv).writePipelineContract(ctx);
    }

    private static PipelineTemplateTypeModel v3TypeModel(boolean reverseDefinitionOrder) {
        Map<String, PipelineTemplateTypeDefinition> definitions = new LinkedHashMap<>();
        PipelineTemplateTypeDefinition alpha = new PipelineTemplateTypeDefinition.RecordType("Alpha", List.of(
            new PipelineTemplateTypeDefinition.Field("code", new PipelineTemplateTypeReference.Scalar("string"))));
        PipelineTemplateTypeDefinition zeta = new PipelineTemplateTypeDefinition.RecordType("Zeta", List.of(
            new PipelineTemplateTypeDefinition.Field("attributes", new PipelineTemplateTypeReference.MapType(
                new PipelineTemplateTypeReference.Scalar("string"), new PipelineTemplateTypeReference.Named("Alpha"))),
            new PipelineTemplateTypeDefinition.Field("description", new PipelineTemplateTypeReference.Scalar("string"))));
        if (reverseDefinitionOrder) {
            definitions.put("Zeta", zeta);
            definitions.put("Alpha", alpha);
        } else {
            definitions.put("Alpha", alpha);
            definitions.put("Zeta", zeta);
        }
        return new PipelineTemplateTypeModel(definitions);
    }

    private static PipelineTemplateTypeModel contributedModel(String namespace) {
        PipelineTemplateTypeModel base = v3TypeModel(false);
        return new PipelineTemplateTypeModel(base.definitions(), Map.of(), Map.of(), Map.of(
            "Alpha", new ProtocolTypeIdentity(ConnectorProviderId.of(namespace), "Alpha")));
    }

    private static PipelineTemplateTypeModel repeatedTypeModel(boolean reverseDefinitionOrder) {
        Map<String, PipelineTemplateTypeDefinition> definitions = new LinkedHashMap<>();
        PipelineTemplateTypeDefinition alpha = new PipelineTemplateTypeDefinition.RecordType("Alpha", List.of(
            new PipelineTemplateTypeDefinition.Field("code", new PipelineTemplateTypeReference.Scalar("string"))));
        PipelineTemplateTypeDefinition zeta = new PipelineTemplateTypeDefinition.RecordType("Zeta", List.of(
            new PipelineTemplateTypeDefinition.Field("attributes", new PipelineTemplateTypeReference.MapType(
                new PipelineTemplateTypeReference.Scalar("string"), new PipelineTemplateTypeReference.Named("Alpha"))),
            new PipelineTemplateTypeDefinition.Field(
                "description", new PipelineTemplateTypeReference.Scalar("string"), true,
                PipelineFieldPresence.REQUIRED, PipelineFieldNullability.NON_NULL,
                new PipelineTemplateRepeatedFieldConstraints(Optional.of(1), Optional.of(3)))));
        PipelineTemplateTypeDefinition accountingMethod = new PipelineTemplateTypeDefinition.WrapperType(
            "AccountingMethod", new PipelineTemplateTypeReference.Scalar("string"),
            new PipelineTemplateWrapperConstraints(
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), reverseDefinitionOrder
                    ? List.of("Cash", "Accrual", "Cash") : List.of("Accrual", "Cash")));
        if (reverseDefinitionOrder) {
            definitions.put("Zeta", zeta);
            definitions.put("AccountingMethod", accountingMethod);
            definitions.put("Alpha", alpha);
        } else {
            definitions.put("Alpha", alpha);
            definitions.put("AccountingMethod", accountingMethod);
            definitions.put("Zeta", zeta);
        }
        return new PipelineTemplateTypeModel(definitions);
    }

    private PipelineStepModel step(
        String generatedName,
        String inputType,
        String outputType,
        StreamingShape shape,
        Set<GenerationTarget> targets) {
        return new PipelineStepModel.Builder()
            .serviceName(generatedName)
            .generatedName(generatedName)
            .servicePackage("org.example.restaurant")
            .serviceClassName(ClassName.get("org.example.restaurant.service", generatedName))
            .inputMapping(new TypeMapping(ClassName.get("org.example.restaurant.domain", inputType), null, false))
            .outputMapping(new TypeMapping(ClassName.get("org.example.restaurant.domain", outputType), null, false))
            .streamingShape(shape)
            .enabledTargets(targets)
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.ORCHESTRATOR_CLIENT)
            .sideEffect(false)
            .orderingRequirement(OrderingRequirement.RELAXED)
            .threadSafety(ThreadSafety.SAFE)
            .build();
    }

    private PipelineStepModel deferredStep() {
        return deferredStep(Map.of());
    }

    private PipelineStepModel deferredStep(Map<String, Object> transportConfig) {
        return new PipelineStepModel.Builder()
            .serviceName("ProcessAwaitRestaurantDecisionService")
            .generatedName("ProcessAwaitRestaurantDecisionService")
            .servicePackage("org.example.restaurant")
            .serviceClassName(ClassName.get("org.example.restaurant.service", "ProcessAwaitRestaurantDecisionService"))
            .inputMapping(new TypeMapping(
                ClassName.get("org.example.restaurant.domain", "PendingRestaurantApproval"), null, false))
            .outputMapping(new TypeMapping(
                ClassName.get("org.example.restaurant.domain", "PendingRestaurantApproval"), null, false))
            .deferredCompletionSelection(new DeferredCompletionSelection(
                ClassName.get("org.example.restaurant.domain", "RestaurantDecision"),
                "RestaurantDecision",
                Optional.empty(),
                java.time.Duration.ofMinutes(30),
                List.of("orderId"),
                "interactionId",
                "interaction-api",
                transportConfig,
                Optional.empty(),
                Optional.empty()))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .enabledTargets(Set.of(GenerationTarget.DEFERRED_COMPLETION_STEP))
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.ORCHESTRATOR_CLIENT)
            .sideEffect(false)
            .orderingRequirement(OrderingRequirement.RELAXED)
            .threadSafety(ThreadSafety.SAFE)
            .build();
    }

    private Path writePipelineYaml() throws IOException {
        Path yaml = tempDir.resolve("pipeline.yaml");
        Files.writeString(yaml, """
            basePackage: org.example.restaurant
            transport: REST
            platform: COMPUTE
            steps:
              - name: Validate Order Request
                cardinality: ONE_TO_ONE
                input: PlaceRestaurantOrderRequest
                output: ValidatedRestaurantOrderRequest
              - name: Await Restaurant Decision
                service: org.example.restaurant.CreatePendingApprovalService
                cardinality: ONE_TO_ONE
                input: PendingRestaurantApproval
                output: RestaurantDecision
                await:
                  operationOutput:
                    type: PendingRestaurantApproval
                  timeout: PT30M
                  correlation:
                    strategy: interactionId
                  transport:
                    type: interaction-api
            """);
        return yaml;
    }

    private JsonObject readContract(Path outputDir) throws IOException {
        Path contract = outputDir.resolve("META-INF/pipeline/pipeline-contract.json");
        return new Gson().fromJson(Files.readString(contract), JsonObject.class);
    }

    private ProcessingEnvironment processingEnv(Path outputDir, Map<String, String> options) {
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getFiler()).thenReturn(new PathResourceFiler(outputDir));
        when(processingEnv.getOptions()).thenReturn(options);
        return processingEnv;
    }

    private static final class PathResourceFiler implements Filer {
        private final Path outputDir;

        private PathResourceFiler(Path outputDir) {
            this.outputDir = outputDir;
        }

        @Override
        public JavaFileObject createSourceFile(CharSequence name, Element... originatingElements) {
            throw new UnsupportedOperationException("Source generation is not supported in this test.");
        }

        @Override
        public JavaFileObject createClassFile(CharSequence name, Element... originatingElements) {
            throw new UnsupportedOperationException("Class generation is not supported in this test.");
        }

        @Override
        public FileObject createResource(
            JavaFileManager.Location location,
            CharSequence pkg,
            CharSequence relativeName,
            Element... originatingElements) {
            return new PathFileObject(outputDir.resolve(relativeName.toString()));
        }

        @Override
        public FileObject getResource(
            JavaFileManager.Location location,
            CharSequence pkg,
            CharSequence relativeName) {
            return new PathFileObject(outputDir.resolve(relativeName.toString()));
        }
    }

    private static final class PathFileObject extends SimpleJavaFileObject {
        private final Path path;

        private PathFileObject(Path path) {
            super(path.toUri(), Kind.OTHER);
            this.path = path;
        }

        @Override
        public Writer openWriter() throws IOException {
            Files.createDirectories(path.getParent());
            return Files.newBufferedWriter(path);
        }

        @Override
        public OutputStream openOutputStream() throws IOException {
            Files.createDirectories(path.getParent());
            return Files.newOutputStream(path);
        }
    }
}
