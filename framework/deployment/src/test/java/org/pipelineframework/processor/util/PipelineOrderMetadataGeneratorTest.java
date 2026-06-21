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
import org.pipelineframework.processor.ir.PipelineTransport;
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
                  correlation:
                    strategy: "signedResumeToken"
                  transport:
                    type: "webhook"
                    request:
                      url: "https://partner.example/fraud-check"
            """);

        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getOptions()).thenReturn(java.util.Map.of());
        when(processingEnv.getFiler()).thenReturn(new PathResourceFiler(classOutput));
        RoundEnvironment roundEnv = mock(RoundEnvironment.class);

        PipelineCompilationContext ctx = new PipelineCompilationContext(processingEnv, roundEnv);
        ctx.setTransportMode(PipelineTransport.GRPC);
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
    void writesAwaitClientStepToLocalExecutionOrderMetadata() throws IOException {
        Path classOutput = tempDir.resolve("class-output-local-await");
        Path moduleDir = tempDir.resolve("module-local-await");
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
                  correlation:
                    strategy: "signedResumeToken"
                  transport:
                    type: "webhook"
                    request:
                      url: "https://partner.example/fraud-check"
            """);

        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getOptions()).thenReturn(java.util.Map.of());
        when(processingEnv.getFiler()).thenReturn(new PathResourceFiler(classOutput));
        RoundEnvironment roundEnv = mock(RoundEnvironment.class);

        PipelineCompilationContext ctx = new PipelineCompilationContext(processingEnv, roundEnv);
        ctx.setTransportMode(PipelineTransport.GRPC);
        ctx.setOrchestratorGenerated(false);
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
        assertEquals(1, order.size(), "Expected only the generated await client step in local order");

        String firstStep = order.get(0).getAsString();
        assertEquals("com.example.fraud.pipeline.FraudCheckAwaitClientStep", firstStep);
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
        ctx.setTransportMode(PipelineTransport.GRPC);
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
        ctx.setTransportMode(PipelineTransport.GRPC);
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
                  correlation:
                    strategy: "signedResumeToken"
                  transport:
                    type: "webhook"
                    request:
                      url: "https://partner.example/fraud-check"
            """);

        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getOptions()).thenReturn(java.util.Map.of());
        when(processingEnv.getFiler()).thenReturn(new PathResourceFiler(classOutput));
        RoundEnvironment roundEnv = mock(RoundEnvironment.class);

        PipelineCompilationContext ctx = new PipelineCompilationContext(processingEnv, roundEnv);
        ctx.setTransportMode(PipelineTransport.GRPC);
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

    @Test
    void writesQueryClientStepToOrchestratorOrderMetadata() throws IOException {
        Path classOutput = tempDir.resolve("class-output-query-orch");
        Path moduleDir = tempDir.resolve("module-query-orch");
        Files.createDirectories(moduleDir);
        Files.writeString(moduleDir.resolve("pipeline.yaml"), """
            version: 2
            appName: "Test"
            basePackage: "com.example"
            transport: "GRPC"
            queries:
              customer-risk-by-id:
                connector: "jpa"
                input: "com.example.CustomerRiskLookup"
                output: "com.example.CustomerRiskSnapshot"
                jpa:
                  entity: "com.example.CustomerRiskEntity"
                  where:
                    customerId: "input.customerId"
            steps:
              - name: "Load Customer Risk"
                kind: "query"
                cardinality: "ONE_TO_ONE"
                query: "customer-risk-by-id"
                input: "com.example.CustomerRiskLookup"
                output: "com.example.CustomerRiskSnapshot"
                capture:
                  keyFields: ["customerId"]
            """);

        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getOptions()).thenReturn(java.util.Map.of());
        when(processingEnv.getFiler()).thenReturn(new PathResourceFiler(classOutput));
        RoundEnvironment roundEnv = mock(RoundEnvironment.class);

        PipelineCompilationContext ctx = new PipelineCompilationContext(processingEnv, roundEnv);
        ctx.setTransportMode(PipelineTransport.GRPC);
        ctx.setOrchestratorGenerated(true);
        ctx.setModuleDir(moduleDir);

        PipelineStepModel queryModel = new PipelineStepModel.Builder()
            .serviceName("LoadCustomerRisk")
            .generatedName("LoadCustomerRiskService")
            .servicePackage("com.example.risk")
            .serviceClassName(ClassName.get("org.pipelineframework.query", "QueryStepDescriptor"))
            .inputMapping(new TypeMapping(ClassName.get("com.example.risk", "CustomerRiskLookup"), null, false))
            .outputMapping(new TypeMapping(ClassName.get("com.example.risk", "CustomerRiskSnapshot"), null, false))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .enabledTargets(Set.of(GenerationTarget.QUERY_CLIENT_STEP))
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.ORCHESTRATOR_CLIENT)
            .build();

        ctx.setStepModels(List.of(queryModel));

        new PipelineOrderMetadataGenerator(processingEnv).writeOrderMetadata(ctx);

        Path orderFile = classOutput.resolve("META-INF/pipeline/order.json");
        assertTrue(Files.exists(orderFile), "order.json should be written");

        JsonObject metadata = new Gson().fromJson(Files.readString(orderFile), JsonObject.class);
        JsonArray order = metadata.getAsJsonArray("order");
        assertTrue(order.size() > 0, "Expected at least one ordered step");

        String firstStep = order.get(0).getAsString();
        assertTrue(firstStep.endsWith("LoadCustomerRiskQueryClientStep"),
            "Expected generated class to end with LoadCustomerRiskQueryClientStep but was: " + firstStep);
        assertTrue(firstStep.contains("com.example.risk.pipeline"),
            "Expected generated class to be in pipeline package but was: " + firstStep);
    }

    @Test
    void writesQueryClientStepToLocalExecutionOrderMetadata() throws IOException {
        Path classOutput = tempDir.resolve("class-output-query-local");
        Path moduleDir = tempDir.resolve("module-query-local");
        Files.createDirectories(moduleDir);
        Files.writeString(moduleDir.resolve("pipeline.yaml"), """
            version: 2
            appName: "Test"
            basePackage: "com.example"
            transport: "GRPC"
            queries:
              order-history:
                connector: "jpa"
                input: "com.example.OrderHistoryLookup"
                output: "com.example.OrderHistorySnapshot"
                jpa:
                  entity: "com.example.OrderHistoryEntity"
                  where:
                    orderId: "input.orderId"
            steps:
              - name: "Load Order History"
                kind: "query"
                cardinality: "ONE_TO_ONE"
                query: "order-history"
                input: "com.example.OrderHistoryLookup"
                output: "com.example.OrderHistorySnapshot"
            """);

        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getOptions()).thenReturn(java.util.Map.of());
        when(processingEnv.getFiler()).thenReturn(new PathResourceFiler(classOutput));
        RoundEnvironment roundEnv = mock(RoundEnvironment.class);

        PipelineCompilationContext ctx = new PipelineCompilationContext(processingEnv, roundEnv);
        ctx.setTransportMode(PipelineTransport.GRPC);
        ctx.setOrchestratorGenerated(false);
        ctx.setModuleDir(moduleDir);

        PipelineStepModel queryModel = new PipelineStepModel.Builder()
            .serviceName("LoadOrderHistory")
            .generatedName("LoadOrderHistoryService")
            .servicePackage("com.example.order")
            .serviceClassName(ClassName.get("org.pipelineframework.query", "QueryStepDescriptor"))
            .inputMapping(new TypeMapping(ClassName.get("com.example.order", "OrderHistoryLookup"), null, false))
            .outputMapping(new TypeMapping(ClassName.get("com.example.order", "OrderHistorySnapshot"), null, false))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .enabledTargets(Set.of(GenerationTarget.QUERY_CLIENT_STEP))
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.ORCHESTRATOR_CLIENT)
            .build();

        ctx.setStepModels(List.of(queryModel));

        new PipelineOrderMetadataGenerator(processingEnv).writeOrderMetadata(ctx);

        Path orderFile = classOutput.resolve("META-INF/pipeline/order.json");
        assertTrue(Files.exists(orderFile), "order.json should be written");

        JsonObject metadata = new Gson().fromJson(Files.readString(orderFile), JsonObject.class);
        JsonArray order = metadata.getAsJsonArray("order");
        assertEquals(1, order.size(), "Expected only the query client step in local execution order");

        String firstStep = order.get(0).getAsString();
        assertEquals("com.example.order.pipeline.LoadOrderHistoryQueryClientStep", firstStep);
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
