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

class RestClientStepRendererTest {

    @TempDir
    Path tempDir;

    @Test
    void rendersUnaryRestClientStepWithConfigKey() throws IOException {
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
            .enabledTargets(java.util.Set.of(GenerationTarget.REST_CLIENT_STEP))
            .build();

        RestBinding binding = new RestBinding(
            model,
            "/ProcessPaymentStatusReactiveService/remoteProcess");

        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        GenerationContext context = new GenerationContext(
            processingEnv,
            tempDir,
            DeploymentRole.ORCHESTRATOR_CLIENT,
            java.util.Set.of(),
            null,
            null);

        RestClientStepRenderer renderer = new RestClientStepRenderer();
        renderer.render(binding, context);

        Path clientInterface = tempDir.resolve(
            "org/pipelineframework/csv/service/pipeline/ProcessPaymentStatusRestClient.java");
        Path clientStep = tempDir.resolve(
            "org/pipelineframework/csv/service/pipeline/ProcessPaymentStatusReactiveRestClientStep.java");

        String interfaceSource = Files.readString(clientInterface);
        String stepSource = Files.readString(clientStep);

        assertTrue(interfaceSource.contains("package org.pipelineframework.csv.service.pipeline;"));
        assertTrue(interfaceSource.contains("@RegisterRestClient"));
        assertTrue(interfaceSource.contains("process-payment-status-reactive"));
        assertTrue(interfaceSource.contains("@Path(\"/ProcessPaymentStatusReactiveService/remoteProcess\")"));
        assertTrue(interfaceSource.contains("interface ProcessPaymentStatusRestClient"));
        assertTrue(interfaceSource.contains("Uni<PaymentOutputDto> process(PaymentStatusDto inputDto)"));

        assertTrue(stepSource.contains("package org.pipelineframework.csv.service.pipeline;"));
        assertTrue(stepSource.contains("@GeneratedRole(Role.ORCHESTRATOR_CLIENT)"));
        assertTrue(stepSource.contains("class ProcessPaymentStatusReactiveRestClientStep"));
        assertTrue(stepSource.contains("@RestClient"));
        assertTrue(stepSource.contains("ProcessPaymentStatusRestClient restClient;"));
        assertTrue(stepSource.contains("public Uni<PaymentOutputDto> applyOneToOne(PaymentStatusDto input)"));
        assertTrue(stepSource.contains("HttpMetrics.instrumentClient"));
    }
}
