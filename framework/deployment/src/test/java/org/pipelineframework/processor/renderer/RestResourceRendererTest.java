package org.pipelineframework.processor.renderer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import javax.annotation.processing.ProcessingEnvironment;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.processor.ir.*;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
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
        assertTrue(source.contains("@GeneratedRole("));
        assertTrue(source.contains("REST_SERVER"));
        assertTrue(source.contains("@Path(\"/ProcessPaymentStatusReactiveService/remoteProcess\")"));
        assertTrue(source.contains("class ProcessPaymentStatusResource"));
        assertTrue(source.contains("ProcessPaymentStatusReactiveService domainService;"));
        assertTrue(source.contains("PaymentStatusMapper paymentStatusMapper;"));
        assertTrue(source.contains("PaymentOutputMapper paymentOutputMapper;"));
        assertTrue(source.contains("@POST"));
        assertTrue(source.contains("@Path(\"/\")"));
        assertTrue(source.contains("public Uni<PaymentOutputDto> process(PaymentStatusDto inputDto)"));
        assertTrue(source.contains("PaymentStatus inputDomain = paymentStatusMapper.fromDto(inputDto);"));
        assertTrue(source.contains("return domainService.process(inputDomain).map(output -> paymentOutputMapper.toDto(output));"));
    }

    @Test
    void rendersStreamingCacheSideEffectWithoutMappers() {
        PipelineStepModel model = new PipelineStepModel.Builder()
            .serviceName("ObserveCacheSomethingSideEffectService")
            .servicePackage("org.pipelineframework.cache.service")
            .serviceClassName(ClassName.get(
                "org.pipelineframework.plugin.cache",
                "CacheService"))
            .streamingShape(StreamingShape.UNARY_STREAMING)
            .executionMode(ExecutionMode.DEFAULT)
            .sideEffect(true)
            .inputMapping(new TypeMapping(
                ClassName.get("com.example.domain", "InputType"),
                null,
                false))
            .outputMapping(new TypeMapping(
                ClassName.get("com.example.domain", "OutputType"),
                null,
                false))
            .enabledTargets(java.util.Set.of(GenerationTarget.REST_RESOURCE))
            .build();

        RestBinding binding = new RestBinding(
            model,
            "/ObserveCacheSomethingSideEffectService/remoteProcess");

        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        GenerationContext context = new GenerationContext(processingEnv, tempDir, DeploymentRole.REST_SERVER,
            java.util.Set.of(), null, null);

        RestResourceRenderer renderer = new RestResourceRenderer();
        assertDoesNotThrow(() -> renderer.render(binding, context));
    }

    @Test
    void derivesResourcefulPathFromOutputTypeForUnaryUnary() throws IOException {
        PipelineStepModel model = baseModelBuilder("ProcessAckPaymentSentService", StreamingShape.UNARY_UNARY)
            .inputMapping(new TypeMapping(
                ClassName.get("org.pipelineframework.csv.common.domain", "AckPaymentSent"),
                ClassName.get("org.pipelineframework.csv.common.mapper", "AckPaymentSentMapper"),
                true))
            .outputMapping(new TypeMapping(
                ClassName.get("org.pipelineframework.csv.common.domain", "PaymentStatus"),
                ClassName.get("org.pipelineframework.csv.common.mapper", "PaymentStatusMapper"),
                true))
            .build();

        String source = renderAndReadSource(
            new RestBinding(model, null),
            "org/pipelineframework/csv/service/pipeline/ProcessAckPaymentSentResource.java");
        assertTrue(source.contains("@Path(\"/api/v1/payment-status\")"));
        assertTrue(source.contains("@Path(\"/\")"));
    }

    @Test
    void derivesResourcefulPathFromInputTypeForUnaryStreaming() throws IOException {
        PipelineStepModel model = baseModelBuilder("ProcessAckPaymentSentService", StreamingShape.UNARY_STREAMING)
            .inputMapping(new TypeMapping(
                ClassName.get("org.pipelineframework.csv.common.domain", "AckPaymentSent"),
                ClassName.get("org.pipelineframework.csv.common.mapper", "AckPaymentSentMapper"),
                true))
            .outputMapping(new TypeMapping(
                ClassName.get("org.pipelineframework.csv.common.domain", "PaymentStatus"),
                ClassName.get("org.pipelineframework.csv.common.mapper", "PaymentStatusMapper"),
                true))
            .build();

        String source = renderAndReadSource(
            new RestBinding(model, null),
            "org/pipelineframework/csv/service/pipeline/ProcessAckPaymentSentResource.java");
        assertTrue(
            source.contains("@Path(\"/api/v1/ack-payment-sent\")"),
            "expected class-level @Path for ack-payment-sent not found");
        assertTrue(
            source.contains("@Path(\"/\")"),
            "expected method-level @Path for resourceful operation not found");
    }

    @Test
    void derivesPluginSegmentForSideEffectPaths() throws IOException {
        PipelineStepModel model = new PipelineStepModel.Builder()
            .serviceName("ObservePersistenceAckPaymentSentSideEffectService")
            .generatedName("PersistenceAckPaymentSentSideEffect")
            .servicePackage("org.pipelineframework.csv.service")
            .serviceClassName(ClassName.get(
                "org.pipelineframework.plugin.persistence",
                "PersistenceService"))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .executionMode(ExecutionMode.DEFAULT)
            .sideEffect(true)
            .inputMapping(new TypeMapping(
                ClassName.get("org.pipelineframework.csv.common.domain", "AckPaymentSent"),
                ClassName.get("org.pipelineframework.csv.common.mapper", "AckPaymentSentMapper"),
                true))
            .outputMapping(new TypeMapping(
                ClassName.get("org.pipelineframework.csv.common.domain", "AckPaymentSent"),
                ClassName.get("org.pipelineframework.csv.common.mapper", "AckPaymentSentMapper"),
                true))
            .build();

        String source = renderAndReadSource(
            new RestBinding(model, null),
            "org/pipelineframework/csv/service/pipeline/PersistenceAckPaymentSentSideEffectResource.java");
        assertTrue(source.contains("@Path(\"/api/v1/ack-payment-sent/persistence\")"));
    }

    private PipelineStepModel.Builder baseModelBuilder(String serviceName, StreamingShape shape) {
        return new PipelineStepModel.Builder()
            .serviceName(serviceName)
            .servicePackage("org.pipelineframework.csv.service")
            .serviceClassName(ClassName.get("org.pipelineframework.csv.service", serviceName))
            .streamingShape(shape)
            .executionMode(ExecutionMode.DEFAULT)
            .enabledTargets(Set.of(GenerationTarget.REST_RESOURCE));
    }

    private GenerationContext createContext() {
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        return new GenerationContext(processingEnv, tempDir, DeploymentRole.REST_SERVER, Set.of(), null, null);
    }

    private String renderAndReadSource(RestBinding binding, String resourceFileName) throws IOException {
        new RestResourceRenderer().render(binding, createContext());
        Path generatedSource = tempDir.resolve(resourceFileName);
        return Files.readString(generatedSource);
    }
}
