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
import com.google.gson.JsonObject;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.ir.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PipelineTelemetryMetadataGeneratorTest {

    @TempDir
    Path tempDir;

    @Test
    void writesTelemetryMetadataWithFirstConsumerAndLastProducer() throws IOException {
        PipelineCompilationContext ctx = buildContext();
        writeApplicationProperties("com.example.ItemIn", "com.example.ItemOut");

        List<PipelineStepModel> models = List.of(
            step("StepOneService", "com.example.step1", type("ItemIn"), type("Other"), false),
            step("StepTwoService", "com.example.step2", type("Other"), type("Other"), false),
            step("StepThreeService", "com.example.step3", type("Other"), type("ItemOut"), false),
            step("StepFourService", "com.example.step4", type("Other"), type("ItemOut"), false)
        );
        ctx.setStepModels(models);

        new PipelineTelemetryMetadataGenerator(ctx.getProcessingEnv()).writeTelemetryMetadata(ctx);

        JsonObject metadata = readTelemetryJson();
        assertEquals("com.example.ItemIn", metadata.get("itemInputType").getAsString());
        assertEquals("com.example.ItemOut", metadata.get("itemOutputType").getAsString());
        assertEquals(
            "com.example.step1.pipeline.StepOneGrpcClientStep",
            metadata.get("consumerStep").getAsString());
        assertEquals(
            "com.example.step4.pipeline.StepFourGrpcClientStep",
            metadata.get("producerStep").getAsString());
    }

    @Test
    void prefersOutputTypeWhenResolvingPluginParents() throws IOException {
        PipelineCompilationContext ctx = buildContext();
        writeApplicationProperties("com.example.Item", "com.example.Item");

        PipelineStepModel baseOutput = step("BaseOutputService", "com.example.base.output",
            type("Other"), type("Item"), false);
        PipelineStepModel baseInput = step("BaseInputService", "com.example.base.input",
            type("Item"), type("Other"), false);
        PipelineStepModel plugin = step("PluginService", "com.example.plugin",
            type("Item"), type("Other"), true);

        ctx.setStepModels(List.of(baseOutput, baseInput, plugin));

        new PipelineTelemetryMetadataGenerator(ctx.getProcessingEnv()).writeTelemetryMetadata(ctx);

        JsonObject metadata = readTelemetryJson();
        JsonObject parents = metadata.getAsJsonObject("stepParents");
        assertNotNull(parents);
        assertEquals(
            "com.example.base.output.pipeline.BaseOutputGrpcClientStep",
            parents.get("com.example.plugin.pipeline.PluginGrpcClientStep").getAsString());
    }

    @Test
    void resolvesCsvPaymentsBoundaryFromPaymentRecordType() throws IOException {
        PipelineCompilationContext ctx = buildContext();
        writeApplicationProperties(
            "org.pipelineframework.csv.common.domain.PaymentRecord",
            "org.pipelineframework.csv.common.domain.PaymentOutput");

        List<PipelineStepModel> models = List.of(
            csvStep("ProcessFolderService", "org.pipelineframework.csv.process_folder.service",
                csvType("CsvFolder"), csvType("CsvPaymentsInputFile")),
            csvStep("ProcessCsvPaymentsInputService", "org.pipelineframework.csv.process_csv_payments_input.service",
                csvType("CsvPaymentsInputFile"), csvType("PaymentRecord")),
            csvStep("ProcessSendPaymentRecordService", "org.pipelineframework.csv.process_send_payment_record.service",
                csvType("PaymentRecord"), csvType("AckPaymentSent")),
            csvStep("ProcessAckPaymentSentService", "org.pipelineframework.csv.process_ack_payment_sent.service",
                csvType("AckPaymentSent"), csvType("PaymentStatus")),
            csvStep("ProcessPaymentStatusService", "org.pipelineframework.csv.process_payment_status.service",
                csvType("PaymentStatus"), csvType("PaymentOutput")),
            csvStep("ProcessCsvPaymentsOutputFileService",
                "org.pipelineframework.csv.process_csv_payments_output_file.service",
                csvType("PaymentOutput"), csvType("CsvPaymentsOutputFile"))
        );
        ctx.setStepModels(models);

        new PipelineTelemetryMetadataGenerator(ctx.getProcessingEnv()).writeTelemetryMetadata(ctx);

        JsonObject metadata = readTelemetryJson();
        assertEquals(
            "org.pipelineframework.csv.process_send_payment_record.service.pipeline.ProcessSendPaymentRecordGrpcClientStep",
            metadata.get("consumerStep").getAsString());
        assertEquals(
            "org.pipelineframework.csv.process_payment_status.service.pipeline.ProcessPaymentStatusGrpcClientStep",
            metadata.get("producerStep").getAsString());
    }

    private PipelineCompilationContext buildContext() {
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getFiler()).thenReturn(new PathResourceFiler(tempDir.resolve("class-output")));
        RoundEnvironment roundEnv = mock(RoundEnvironment.class);
        PipelineCompilationContext ctx = new PipelineCompilationContext(processingEnv, roundEnv);
        ctx.setOrchestratorGenerated(true);
        ctx.setTransportModeGrpc(true);
        ctx.setModuleDir(tempDir);
        return ctx;
    }

    private void writeApplicationProperties(String inputType, String outputType) throws IOException {
        Path resourcesDir = tempDir.resolve("src/main/resources");
        Files.createDirectories(resourcesDir);
        Files.writeString(
            resourcesDir.resolve("application.properties"),
            "pipeline.telemetry.item-input-type=" + inputType + System.lineSeparator()
                + "pipeline.telemetry.item-output-type=" + outputType + System.lineSeparator());
    }

    private JsonObject readTelemetryJson() throws IOException {
        Path file = tempDir.resolve("class-output").resolve("META-INF/pipeline/telemetry.json");
        String content = Files.readString(file);
        return new Gson().fromJson(content, JsonObject.class);
    }

    private PipelineStepModel step(
        String generatedName,
        String servicePackage,
        TypeName inputType,
        TypeName outputType,
        boolean sideEffect) {
        return new PipelineStepModel.Builder()
            .serviceName(generatedName)
            .generatedName(generatedName)
            .servicePackage(servicePackage)
            .serviceClassName(ClassName.get(servicePackage, generatedName))
            .inputMapping(new TypeMapping(inputType, null, false))
            .outputMapping(new TypeMapping(outputType, null, false))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .enabledTargets(Set.of(GenerationTarget.CLIENT_STEP))
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.PIPELINE_SERVER)
            .sideEffect(sideEffect)
            .cacheKeyGenerator(null)
            .build();
    }

    private TypeName type(String simpleName) {
        return ClassName.get("com.example", simpleName);
    }

    private PipelineStepModel csvStep(
        String generatedName,
        String servicePackage,
        TypeName inputType,
        TypeName outputType) {
        return step(generatedName, servicePackage, inputType, outputType, false);
    }

    private TypeName csvType(String simpleName) {
        return ClassName.get("org.pipelineframework.csv.common.domain", simpleName);
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
