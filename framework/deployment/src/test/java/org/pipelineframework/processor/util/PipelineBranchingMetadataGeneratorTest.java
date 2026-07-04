package org.pipelineframework.processor.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
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
import com.google.gson.JsonObject;
import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.ExecutionMode;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.PipelineTransport;
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.processor.ir.TypeMapping;
import org.pipelineframework.processor.routing.PipelineBranchingPlan;

class PipelineBranchingMetadataGeneratorTest {

    @TempDir
    Path tempDir;

    @Test
    void writesBranchingMetadataWithRuntimeClassesAndAcceptedContracts() throws IOException {
        Path classOutput = tempDir.resolve("class-output");

        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getOptions()).thenReturn(Map.of());
        when(processingEnv.getFiler()).thenReturn(new PathResourceFiler(classOutput));
        RoundEnvironment roundEnv = mock(RoundEnvironment.class);

        PipelineCompilationContext ctx = new PipelineCompilationContext(processingEnv, roundEnv);
        ctx.setTransportMode(PipelineTransport.REST);
        ctx.setOrchestratorGenerated(true);
        ctx.setStepModels(List.of(
            stepModel("ReserveStock", "com.example.order.inventory", "PhysicalOrder", "StockReserved"),
            stepModel("Finalize", "com.example.order.finalize", "OrderCompletion", "FinalizedOrder")));
        ctx.setBranchingPlan(new PipelineBranchingPlan(
            true,
            1,
            List.of(
                new PipelineBranchingPlan.BranchStep(
                    0,
                    "Reserve Stock",
                    "PhysicalOrder",
                    "StockReserved",
                    List.of("PhysicalOrder"),
                    List.of("StockReserved"),
                    List.of(ClassName.get("com.example.common.domain", "PhysicalOrder")),
                    false),
                new PipelineBranchingPlan.BranchStep(
                    1,
                    "Finalize",
                    "OrderCompletion",
                    "FinalizedOrder",
                    List.of("StockReserved", "LicenseProvisioned"),
                    List.of("FinalizedOrder"),
                    List.of(
                        ClassName.get("com.example.common.domain", "StockReserved"),
                        ClassName.get("com.example.common.domain", "LicenseProvisioned")),
                    true))));

        new PipelineBranchingMetadataGenerator(processingEnv).writeBranchingMetadata(ctx);

        Path metadataFile = classOutput.resolve("META-INF/pipeline/branching.json");
        assertTrue(Files.exists(metadataFile), "branching.json should be written");

        JsonObject metadata = new Gson().fromJson(Files.readString(metadataFile), JsonObject.class);
        assertEquals(1, metadata.get("terminalStepIndex").getAsInt());
        assertEquals(2, metadata.getAsJsonArray("steps").size());

        JsonObject reserveStock = metadata.getAsJsonArray("steps").get(0).getAsJsonObject();
        assertEquals("Reserve Stock", reserveStock.get("step").getAsString());
        assertEquals(
            "com.example.order.inventory.pipeline.ReserveStockRestClientStep",
            reserveStock.get("runtimeStepClass").getAsString());
        assertEquals(
            "com.example.common.dto.PhysicalOrderDto",
            reserveStock.get("inputRuntimeClass").getAsString());
        assertEquals(
            "com.example.common.dto.PhysicalOrderDto",
            reserveStock.getAsJsonArray("acceptedRuntimeClasses").get(0).getAsString());

        JsonObject finalize = metadata.getAsJsonArray("steps").get(1).getAsJsonObject();
        assertTrue(finalize.get("terminal").getAsBoolean());
        assertEquals(2, finalize.getAsJsonArray("acceptedContracts").size());
        assertEquals(
            "com.example.common.dto.OrderCompletionDto",
            finalize.get("inputRuntimeClass").getAsString());
    }

    private static PipelineStepModel stepModel(
        String serviceName,
        String servicePackage,
        String inputType,
        String outputType
    ) {
        return new PipelineStepModel.Builder()
            .serviceName(serviceName)
            .generatedName(serviceName + "Service")
            .servicePackage(servicePackage)
            .serviceClassName(ClassName.get(servicePackage, serviceName + "Service"))
            .inputMapping(new TypeMapping(ClassName.get("com.example.common.domain", inputType), null, false))
            .outputMapping(new TypeMapping(ClassName.get("com.example.common.domain", outputType), null, false))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .enabledTargets(Set.of(GenerationTarget.CLIENT_STEP))
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.ORCHESTRATOR_CLIENT)
            .build();
    }

    private static final class PathResourceFiler implements Filer {
        private final Path classOutputRoot;

        private PathResourceFiler(Path classOutputRoot) {
            this.classOutputRoot = classOutputRoot;
        }

        @Override
        public JavaFileObject createSourceFile(CharSequence name, Element... originatingElements) {
            throw new UnsupportedOperationException();
        }

        @Override
        public JavaFileObject createClassFile(CharSequence name, Element... originatingElements) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileObject createResource(
            JavaFileManager.Location location,
            CharSequence pkg,
            CharSequence relativeName,
            Element... originatingElements
        ) throws IOException {
            Path target = classOutputRoot.resolve(relativeName.toString());
            Files.createDirectories(target.getParent());
            return new PathFileObject(target);
        }

        @Override
        public FileObject getResource(JavaFileManager.Location location, CharSequence pkg, CharSequence relativeName) {
            throw new UnsupportedOperationException();
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
