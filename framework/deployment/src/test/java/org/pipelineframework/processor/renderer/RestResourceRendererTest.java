package org.pipelineframework.processor.renderer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.annotation.processing.ProcessingEnvironment;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.processor.ir.*;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class RestResourceRendererTest {

    @TempDir
    Path tempDir;

    @Test
    void rendersUnaryResourceMatchingCsvPaymentExample() throws IOException {
        PipelineStepModel model = new PipelineStepModel.Builder()
            .serviceName("ProcessPaymentStatusReactiveService")
            .servicePackage("org.pipelineframework.csv.service")
            .serviceClassName(ClassName.get(
                "org.pipelineframework.csv.service",
                "ProcessPaymentStatusReactiveService"))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .executionMode(ExecutionMode.DEFAULT)
            .inputMapping(new TypeMapping(
                ClassName.get("org.pipelineframework.csv.common.domain", "PaymentStatus"),
                ClassName.get("org.pipelineframework.csv.common.mapper", "PaymentStatusMapper"),
                true))
            .outputMapping(new TypeMapping(
                ClassName.get("org.pipelineframework.csv.common.domain", "PaymentOutput"),
                ClassName.get("org.pipelineframework.csv.common.mapper", "PaymentOutputMapper"),
                true))
            .enabledTargets(java.util.Set.of(GenerationTarget.REST_RESOURCE))
            .build();

        RestBinding binding = new RestBinding(
            model,
            "/ProcessPaymentStatusReactiveService/remoteProcess");

        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        GenerationContext context = new GenerationContext(processingEnv, tempDir, DeploymentRole.REST_SERVER,
            java.util.Set.of(), null, null);

        RestResourceRenderer renderer = new RestResourceRenderer();
        renderer.render(binding, context);

        Path generatedSource = tempDir.resolve("org/pipelineframework/csv/service/pipeline/ProcessPaymentStatusResource.java");
        String source = Files.readString(generatedSource);

        assertTrue(source.contains("package org.pipelineframework.csv.service.pipeline;"));
        assertTrue(source.contains("@GeneratedRole(Role.REST_SERVER)"));
        assertTrue(source.contains("@Path(\"/ProcessPaymentStatusReactiveService/remoteProcess\")"));
        assertTrue(source.contains("class ProcessPaymentStatusResource"));
        assertTrue(source.contains("ProcessPaymentStatusReactiveService domainService;"));
        assertTrue(source.contains("PaymentStatusMapper paymentStatusMapper;"));
        assertTrue(source.contains("PaymentOutputMapper paymentOutputMapper;"));
        assertTrue(source.contains("@POST"));
        assertTrue(source.contains("@Path(\"/process\")"));
        assertTrue(source.contains("public Uni<PaymentOutputDto> process(PaymentStatusDto inputDto)"));
        assertTrue(source.contains("PaymentStatus inputDomain = paymentStatusMapper.fromDto(inputDto);"));
        assertTrue(source.contains("return domainService.process(inputDomain).map(output -> paymentOutputMapper.toDto(output));"));
    }

}
