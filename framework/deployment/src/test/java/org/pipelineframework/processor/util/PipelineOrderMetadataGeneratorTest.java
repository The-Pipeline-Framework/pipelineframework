package org.pipelineframework.processor.util;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
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
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.ExecutionMode;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.processor.ir.TransportMode;
import org.pipelineframework.processor.ir.TypeMapping;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PipelineOrderMetadataGeneratorTest {

    @TempDir
    Path tempDir;

    @Test
    void writesAwaitClientStepToOrderMetadata() throws IOException {
        Path classOutput = tempDir.resolve("class-output");
        Path moduleDir = tempDir.resolve("module");
        Files.createDirectories(moduleDir);
        Files.writeString(moduleDir.resolve("pipeline.yaml"), """
            version: 2
            appName: "Test"
            basePackage: "com.example"
            transport: "GRPC"
            steps:
              - name: "Fraud Check"
                kind: "await"
                input: "com.example.FraudCheckRequest"
                output: "com.example.FraudCheckDecision"
                timeout: "PT10M"
                await:
                  transport:
                    type: "webhook"
            """);

        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getOptions()).thenReturn(java.util.Map.of());
        when(processingEnv.getFiler()).thenReturn(new PathResourceFiler(classOutput));
        RoundEnvironment roundEnv = mock(RoundEnvironment.class);

        PipelineCompilationContext ctx = new PipelineCompilationContext(processingEnv, roundEnv);
        ctx.setTransportMode(TransportMode.GRPC);
        ctx.setOrchestratorGenerated(true);
        ctx.setModuleDir(moduleDir);

        PipelineStepModel awaitModel = new PipelineStepModel.Builder()
            .serviceName("FraudCheck")
            .generatedName("FraudCheckService")
            .servicePackage("com.example.fraud")
            .serviceClassName(ClassName.get("org.pipelineframework.awaitable", "AwaitStepDescriptor"))
            .inputMapping(new TypeMapping(ClassName.get("com.example.fraud", "FraudCheckRequest"), null, false))
            .outputMapping(new TypeMapping(ClassName.get("com.example.fraud", "FraudCheckDecision"), null, false))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .enabledTargets(Set.of(GenerationTarget.AWAIT_CLIENT_STEP))
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.ORCHESTRATOR_CLIENT)
            .build();

        ctx.setStepModels(List.of(awaitModel));

        new PipelineOrderMetadataGenerator(processingEnv).writeOrderMetadata(ctx);

        Path orderFile = classOutput.resolve("META-INF/pipeline/order.json");
        assertTrue(Files.exists(orderFile), "order.json should be written");

        JsonObject metadata = new Gson().fromJson(Files.readString(orderFile), JsonObject.class);
        JsonArray order = metadata.getAsJsonArray("order");
        assertTrue(order.size() > 0, "Expected at least one ordered step");

        String firstStep = order.get(0).getAsString();
        assertTrue(firstStep.endsWith("FraudCheckAwaitClientStep"),
            "Expected generated class to end with FraudCheckAwaitClientStep but was: " + firstStep);
        assertTrue(firstStep.contains("com.example.fraud.pipeline"),
            "Expected generated class to be in pipeline package but was: " + firstStep);
    }

    @Test
    void normalStepUsesTransportSpecificSuffix() throws IOException {
        Path classOutput = tempDir.resolve("class-output2");
        Path moduleDir = tempDir.resolve("module2");
        Files.createDirectories(moduleDir);
        Files.writeString(moduleDir.resolve("pipeline.yaml"), """
            version: 2
            appName: "Test"
            basePackage: "com.example"
            transport: "GRPC"
            steps:
              - name: "Charge Card"
                service: "com.example.ChargeCardService"
            """);

        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getOptions()).thenReturn(java.util.Map.of());
        when(processingEnv.getFiler()).thenReturn(new PathResourceFiler(classOutput));
        RoundEnvironment roundEnv = mock(RoundEnvironment.class);

        PipelineCompilationContext ctx = new PipelineCompilationContext(processingEnv, roundEnv);
        ctx.setTransportMode(TransportMode.GRPC);
        ctx.setOrchestratorGenerated(true);
        ctx.setModuleDir(moduleDir);

        PipelineStepModel grpcModel = new PipelineStepModel.Builder()
            .serviceName("ChargeCard")
            .generatedName("ChargeCardService")
            .servicePackage("com.example.charge")
            .serviceClassName(ClassName.get("com.example.charge", "ChargeCardService"))
            .inputMapping(new TypeMapping(ClassName.get("com.example", "ChargeRequest"), null, false))
            .outputMapping(new TypeMapping(ClassName.get("com.example", "ChargeResult"), null, false))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .enabledTargets(Set.of(GenerationTarget.CLIENT_STEP))
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.ORCHESTRATOR_CLIENT)
            .build();

        ctx.setStepModels(List.of(grpcModel));

        new PipelineOrderMetadataGenerator(processingEnv).writeOrderMetadata(ctx);

        Path orderFile = classOutput.resolve("META-INF/pipeline/order.json");
        assertTrue(Files.exists(orderFile), "order.json should be written");

        JsonObject metadata = new Gson().fromJson(Files.readString(orderFile), JsonObject.class);
        JsonArray order = metadata.getAsJsonArray("order");
        assertTrue(order.size() > 0, "Expected at least one ordered step");

        String firstStep = order.get(0).getAsString();
        assertTrue(firstStep.endsWith("GrpcClientStep"),
            "Expected GRPC step to end with GrpcClientStep but was: " + firstStep);
    }

    @Test
    void doesNothingWhenNoStepModels() throws IOException {
        Path classOutput = tempDir.resolve("class-output3");
        Path moduleDir = tempDir.resolve("module3");
        Files.createDirectories(moduleDir);
        Files.writeString(moduleDir.resolve("pipeline.yaml"), """
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "review"
                service: "com.example.ReviewService"
            """);

        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getOptions()).thenReturn(java.util.Map.of());
        RoundEnvironment roundEnv = mock(RoundEnvironment.class);

        PipelineCompilationContext ctx = new PipelineCompilationContext(processingEnv, roundEnv);
        ctx.setTransportMode(TransportMode.GRPC);
        ctx.setOrchestratorGenerated(true);
        ctx.setModuleDir(moduleDir);
        ctx.setStepModels(List.of());  // no models

        new PipelineOrderMetadataGenerator(processingEnv).writeOrderMetadata(ctx);

        // No file should be written
        Path orderFile = classOutput.resolve("META-INF/pipeline/order.json");
        assertFalse(Files.exists(orderFile), "No order.json should be written when no step models");
    }

    @Test
    void stripTrailingServiceHandlesNameWithoutServiceSuffix() throws IOException {
        // Test indirectly: if generatedName does NOT end with "Service", it should still be included as-is
        Path classOutput = tempDir.resolve("class-output4");
        Path moduleDir = tempDir.resolve("module4");
        Files.createDirectories(moduleDir);
        Files.writeString(moduleDir.resolve("pipeline.yaml"), """
            version: 2
            appName: "Test"
            basePackage: "com.example"
            transport: "GRPC"
            steps:
              - name: "FraudCheck"
                kind: "await"
                input: "com.example.FraudCheckRequest"
                output: "com.example.FraudCheckDecision"
                timeout: "PT5M"
                await:
                  transport:
                    type: "webhook"
            """);

        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getOptions()).thenReturn(java.util.Map.of());
        when(processingEnv.getFiler()).thenReturn(new PathResourceFiler(classOutput));
        RoundEnvironment roundEnv = mock(RoundEnvironment.class);

        PipelineCompilationContext ctx = new PipelineCompilationContext(processingEnv, roundEnv);
        ctx.setTransportMode(TransportMode.GRPC);
        ctx.setOrchestratorGenerated(true);
        ctx.setModuleDir(moduleDir);

        // generatedName does NOT end with "Service" - tests stripTrailingService(name) returns name unchanged
        PipelineStepModel awaitModel = new PipelineStepModel.Builder()
            .serviceName("FraudCheck")
            .generatedName("FraudCheck")  // no "Service" suffix
            .servicePackage("com.example.fraud")
            .serviceClassName(ClassName.get("org.pipelineframework.awaitable", "AwaitStepDescriptor"))
            .inputMapping(new TypeMapping(ClassName.get("com.example.fraud", "FraudCheckRequest"), null, false))
            .outputMapping(new TypeMapping(ClassName.get("com.example.fraud", "FraudCheckDecision"), null, false))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .enabledTargets(Set.of(GenerationTarget.AWAIT_CLIENT_STEP))
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.ORCHESTRATOR_CLIENT)
            .build();

        ctx.setStepModels(List.of(awaitModel));

        new PipelineOrderMetadataGenerator(processingEnv).writeOrderMetadata(ctx);

        Path orderFile = classOutput.resolve("META-INF/pipeline/order.json");
        assertTrue(Files.exists(orderFile), "order.json should be written");

        JsonObject metadata = new Gson().fromJson(Files.readString(orderFile), JsonObject.class);
        JsonArray order = metadata.getAsJsonArray("order");
        assertTrue(order.size() > 0, "Expected at least one ordered step");

        // When generatedName = "FraudCheck" (no Service suffix), class should be FraudCheckAwaitClientStep
        String firstStep = order.get(0).getAsString();
        assertTrue(firstStep.endsWith("FraudCheckAwaitClientStep"),
            "Expected class to end with FraudCheckAwaitClientStep but was: " + firstStep);
    }

    // ---- Helper classes (reused from PipelinePlatformMetadataGeneratorTest pattern) ----

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
            Path path = outputDir.resolve(relativeName.toString());
            return new PathFileObject(path);
        }

        @Override
        public FileObject getResource(
            JavaFileManager.Location location,
            CharSequence pkg,
            CharSequence relativeName) {
            Path path = outputDir.resolve(relativeName.toString());
            return new PathFileObject(path);
        }
    }

    private static final class PathFileObject extends SimpleJavaFileObject {
        private final Path path;

        private PathFileObject(Path path) {
            super(path.toUri(), JavaFileObject.Kind.OTHER);
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
