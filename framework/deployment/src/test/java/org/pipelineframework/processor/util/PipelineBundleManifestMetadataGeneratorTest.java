package org.pipelineframework.processor.util;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
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
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.ExecutionMode;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.PipelineTransport;
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.processor.ir.TypeMapping;
import org.pipelineframework.processor.mapping.PipelineRuntimeMapping;
import org.pipelineframework.parallelism.OrderingRequirement;
import org.pipelineframework.parallelism.ThreadSafety;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PipelineBundleManifestMetadataGeneratorTest {

    @TempDir
    Path tempDir;

    @Test
    void writesDeterministicManifestWithOrderedStepsAndAwaitTransport() throws IOException {
        Path pipelineYaml = writePipelineYaml();
        Path firstOutput = tempDir.resolve("first");
        Path secondOutput = tempDir.resolve("second");

        writeMetadata(pipelineYaml, firstOutput);
        writeMetadata(pipelineYaml, secondOutput);

        JsonObject first = readManifest(firstOutput);
        JsonObject second = readManifest(secondOutput);
        JsonObject firstContract = readContract(firstOutput);
        JsonObject secondContract = readContract(secondOutput);
        assertEquals(first.get("bundleHash").getAsString(), second.get("bundleHash").getAsString());
        assertEquals(first.get("bundleVersionId").getAsString(), second.get("bundleVersionId").getAsString());
        assertEquals(firstContract.get("contractHash").getAsString(), secondContract.get("contractHash").getAsString());
        assertEquals(firstContract.get("contractVersion").getAsString(), secondContract.get("contractVersion").getAsString());
        assertEquals(1, first.get("schemaVersion").getAsInt());
        assertEquals("org.example.restaurant", first.get("pipelineId").getAsString());
        assertEquals("org.example.restaurant", firstContract.get("pipelineId").getAsString());
        assertEquals(first.get("bundleHash").getAsString(), firstContract.get("contractHash").getAsString());
        assertEquals(first.get("bundleVersionId").getAsString(), firstContract.get("contractVersion").getAsString());
        assertEquals("COMPUTE", first.get("platform").getAsString());
        assertEquals("REST", first.get("transport").getAsString());
        assertEquals("orchestrator-svc", first.get("module").getAsString());
        assertEquals("MONOLITH", first.get("runtimeLayout").getAsString());
        assertTrue(first.get("bundleVersionId").getAsString().startsWith("sha256:"));

        JsonArray steps = first.getAsJsonArray("steps");
        assertEquals(2, steps.size());
        assertEquals("Validate Order Request", steps.get(0).getAsJsonObject().get("authoredName").getAsString());
        assertEquals("Await Restaurant Decision", steps.get(1).getAsJsonObject().get("authoredName").getAsString());
        assertEquals("await", steps.get(1).getAsJsonObject().get("kind").getAsString());
        assertEquals("interaction-api", steps.get(1).getAsJsonObject().get("awaitTransport").getAsString());
        assertEquals(
            "org.example.restaurant.domain.RestaurantDecision",
            steps.get(1).getAsJsonObject().get("outputTypeId").getAsString());

        JsonObject capabilities = first.getAsJsonObject("capabilities");
        assertTrue(capabilities.get("localTransitionExecution").getAsBoolean());
        assertEquals(4, capabilities.getAsJsonArray("transitionWorkerProtocols").size());
    }

    @Test
    void skipsManifestWhenNoPipelineModelExists() throws IOException {
        ProcessingEnvironment processingEnv = processingEnv(tempDir.resolve("empty"), Map.of());
        RoundEnvironment roundEnv = mock(RoundEnvironment.class);
        PipelineCompilationContext ctx = new PipelineCompilationContext(processingEnv, roundEnv);

        PipelineBundleManifestMetadataGenerator generator = new PipelineBundleManifestMetadataGenerator(processingEnv);
        generator.writeBundleManifest(ctx);
        generator.writePipelineContract(ctx);

        assertFalse(Files.exists(tempDir.resolve("empty").resolve("META-INF/pipeline/bundle-manifest.json")));
        assertFalse(Files.exists(tempDir.resolve("empty").resolve("META-INF/pipeline/pipeline-contract.json")));
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
            step("ProcessAwaitRestaurantDecisionService", "PendingRestaurantApproval", "RestaurantDecision",
                StreamingShape.UNARY_UNARY, Set.of(GenerationTarget.AWAIT_CLIENT_STEP))));

        PipelineBundleManifestMetadataGenerator generator = new PipelineBundleManifestMetadataGenerator(processingEnv);
        generator.writeBundleManifest(ctx);
        generator.writePipelineContract(ctx);

        JsonArray manifestSteps = readManifest(output).getAsJsonArray("steps");
        JsonArray contractSteps = readContract(output).getAsJsonArray("steps");
        assertEquals(2, manifestSteps.size());
        assertEquals(2, contractSteps.size());
        assertEquals(
            "org.example.restaurant.pipeline.ProcessValidateOrderRequestRestClientStep",
            manifestSteps.get(0).getAsJsonObject().get("clientClass").getAsString());
        assertEquals(
            "org.example.restaurant.pipeline.ProcessAwaitRestaurantDecisionAwaitClientStep",
            manifestSteps.get(1).getAsJsonObject().get("clientClass").getAsString());
    }

    private void writeMetadata(Path pipelineYaml, Path outputDir) throws IOException {
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
            step("ProcessAwaitRestaurantDecisionService", "PendingRestaurantApproval", "RestaurantDecision",
                StreamingShape.UNARY_UNARY, Set.of(GenerationTarget.AWAIT_CLIENT_STEP))));

        PipelineBundleManifestMetadataGenerator generator = new PipelineBundleManifestMetadataGenerator(processingEnv);
        generator.writeBundleManifest(ctx);
        generator.writePipelineContract(ctx);
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
                kind: await
                cardinality: ONE_TO_ONE
                input: PendingRestaurantApproval
                output: RestaurantDecision
                await:
                  correlation:
                    strategy: interactionId
                  transport:
                    type: interaction-api
            """);
        return yaml;
    }

    private JsonObject readManifest(Path outputDir) throws IOException {
        Path manifest = outputDir.resolve("META-INF/pipeline/bundle-manifest.json");
        return new Gson().fromJson(Files.readString(manifest), JsonObject.class);
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
