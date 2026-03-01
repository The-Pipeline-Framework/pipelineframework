package org.pipelineframework.processor.renderer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import javax.annotation.processing.ProcessingEnvironment;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.processor.ir.*;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
        when(processingEnv.getOptions()).thenReturn(Map.of());
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
            "org/pipelineframework/csv/service/pipeline/ProcessPaymentStatusRestClientStep.java");

        String interfaceSource = Files.readString(clientInterface);
        String stepSource = Files.readString(clientStep);

        assertTrue(interfaceSource.contains("package org.pipelineframework.csv.service.pipeline;"));
        assertTrue(interfaceSource.contains("@RegisterRestClient"));
        assertTrue(interfaceSource.contains("process-payment-status-reactive"));
        assertTrue(interfaceSource.contains("@Path(\"/ProcessPaymentStatusReactiveService/remoteProcess\")"));
        assertTrue(interfaceSource.contains("@Path(\"/\")"));
        assertTrue(interfaceSource.contains("interface ProcessPaymentStatusRestClient"));
        assertTrue(interfaceSource.contains("@HeaderParam"));
        assertTrue(interfaceSource.contains("PipelineContextHeaders"));
        assertTrue(interfaceSource.contains("Uni<PaymentOutputDto> process("));
        assertTrue(interfaceSource.contains("PaymentStatusDto inputDto"));

        assertTrue(stepSource.contains("package org.pipelineframework.csv.service.pipeline;"));
        assertTrue(stepSource.contains("@GeneratedRole("));
        assertTrue(stepSource.contains("ORCHESTRATOR_CLIENT"));
        assertTrue(stepSource.contains("class ProcessPaymentStatusRestClientStep"));
        assertTrue(stepSource.contains("@RestClient"));
        assertTrue(stepSource.contains("ProcessPaymentStatusRestClient restClient;"));
        assertTrue(stepSource.contains("public Uni<PaymentOutputDto> applyOneToOne(PaymentStatusDto input)"));
        assertTrue(stepSource.contains("HttpMetrics.instrumentClient"));
    }

    @Test
    void rendersSideEffectRestClientStepWithCacheReadBypass() throws IOException {
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
            .sideEffect(true)
            .enabledTargets(java.util.Set.of(GenerationTarget.REST_CLIENT_STEP))
            .build();

        RestBinding binding = new RestBinding(
            model,
            "/ProcessPaymentStatusReactiveService/remoteProcess");

        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getOptions()).thenReturn(Map.of());
        GenerationContext context = new GenerationContext(
            processingEnv,
            tempDir,
            DeploymentRole.ORCHESTRATOR_CLIENT,
            java.util.Set.of(),
            null,
            null);

        RestClientStepRenderer renderer = new RestClientStepRenderer();
        renderer.render(binding, context);

        Path clientStep = tempDir.resolve(
            "org/pipelineframework/csv/service/pipeline/ProcessPaymentStatusRestClientStep.java");

        String stepSource = Files.readString(clientStep);
        assertTrue(stepSource.contains("CacheReadBypass"));
    }

    @Test
    void rendersRestClientStepWithoutExplicitMapperWhenDomainTypesArePresent() throws IOException {
        PipelineStepModel model = new PipelineStepModel.Builder()
            .serviceName("ProcessCrawlSourceService")
            .servicePackage("org.pipelineframework.search.service")
            .serviceClassName(ClassName.get(
                "org.pipelineframework.search.service",
                "ProcessCrawlSourceService"))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .executionMode(ExecutionMode.DEFAULT)
            .inputMapping(new TypeMapping(
                ClassName.get("org.pipelineframework.search.common.domain", "CrawlRequest"),
                null,
                false))
            .outputMapping(new TypeMapping(
                ClassName.get("org.pipelineframework.search.common.domain", "RawDocument"),
                null,
                false))
            .enabledTargets(java.util.Set.of(GenerationTarget.REST_CLIENT_STEP))
            .build();

        RestBinding binding = new RestBinding(
            model,
            "/ProcessCrawlSourceService/remoteProcess");

        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getOptions()).thenReturn(Map.of());
        GenerationContext context = new GenerationContext(
            processingEnv,
            tempDir,
            DeploymentRole.ORCHESTRATOR_CLIENT,
            java.util.Set.of(),
            null,
            null);

        RestClientStepRenderer renderer = new RestClientStepRenderer();
        renderer.render(binding, context);

        Path clientStep = tempDir.resolve(
            "org/pipelineframework/search/service/pipeline/ProcessCrawlSourceRestClientStep.java");
        Path clientInterface = tempDir.resolve(
            "org/pipelineframework/search/service/pipeline/ProcessCrawlSourceRestClient.java");
        assertTrue(Files.exists(clientStep));
        assertTrue(Files.exists(clientInterface));
        String stepSource = Files.readString(clientStep);
        String interfaceSource = Files.readString(clientInterface);
        assertTrue(stepSource.contains("package org.pipelineframework.search.service.pipeline;"));
        assertTrue(stepSource.contains("class ProcessCrawlSourceRestClientStep"));
        assertTrue(stepSource.contains("CrawlRequestDto"));
        assertTrue(stepSource.contains("RawDocumentDto"));
        assertTrue(stepSource.contains("applyOneToOne("));
        assertTrue(interfaceSource.contains("interface ProcessCrawlSourceRestClient"));
        assertTrue(interfaceSource.contains("CrawlRequestDto"));
        assertTrue(interfaceSource.contains("RawDocumentDto"));
        assertTrue(interfaceSource.contains("process("));
    }
}
