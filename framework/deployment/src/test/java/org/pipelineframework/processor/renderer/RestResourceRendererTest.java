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
import static org.junit.jupiter.api.Assertions.assertEquals;
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
        assertTrue(source.contains("Mapper<PaymentStatus, PaymentStatusDto> inboundMapper;"));
        assertTrue(source.contains("Mapper<PaymentOutput, PaymentOutputDto> outboundMapper;"));
        assertTrue(source.contains("@POST"));
        assertTrue(source.contains("@Path(\"/\")"));
        assertTrue(source.contains("public Uni<PaymentOutputDto> process(PaymentStatusDto inputDto)"));
        assertTrue(source.contains("PaymentStatus inputDomain = inboundMapper.fromExternal(inputDto);"));
        assertTrue(source.contains("return domainService.process(inputDomain).map(output -> outboundMapper.toExternal(output));"));
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

    @Test
    void rendersStreamingUnaryResourceWithListParameter() throws IOException {
        PipelineStepModel model = baseModelBuilder("AggregatePaymentsService", StreamingShape.STREAMING_UNARY)
            .inputMapping(new TypeMapping(
                ClassName.get("org.example.domain", "Payment"),
                ClassName.get("org.example.mapper", "PaymentMapper"),
                true))
            .outputMapping(new TypeMapping(
                ClassName.get("org.example.domain", "Summary"),
                ClassName.get("org.example.mapper", "SummaryMapper"),
                true))
            .build();

        String source = renderAndReadSource(
            new RestBinding(model, null),
            "org/pipelineframework/csv/service/pipeline/AggregatePaymentsResource.java");

        assertTrue(source.contains("public Uni<SummaryDto> process(List<PaymentDto> inputDtos)"));
        assertTrue(source.contains("Multi.createFrom().iterable(inputDtos)"));
        assertTrue(source.contains("RestReactiveStreamingClientServiceAdapter"));
    }

    @Test
    void rendersStreamingStreamingResourceWithNdjson() throws IOException {
        PipelineStepModel model = baseModelBuilder("TransformPaymentsService", StreamingShape.STREAMING_STREAMING)
            .inputMapping(new TypeMapping(
                ClassName.get("org.example.domain", "RawPayment"),
                ClassName.get("org.example.mapper", "RawPaymentMapper"),
                true))
            .outputMapping(new TypeMapping(
                ClassName.get("org.example.domain", "ProcessedPayment"),
                ClassName.get("org.example.mapper", "ProcessedPaymentMapper"),
                true))
            .build();

        String source = renderAndReadSource(
            new RestBinding(model, null),
            "org/pipelineframework/csv/service/pipeline/TransformPaymentsResource.java");

        assertTrue(source.contains("public Multi<ProcessedPaymentDto> process(Multi<RawPaymentDto> inputDtos)"));
        assertTrue(source.contains("@Consumes(\"application/x-ndjson\")"));
        assertTrue(source.contains("@Produces(\"application/x-ndjson\")"));
        assertTrue(source.contains("RestReactiveBidirectionalStreamingServiceAdapter"));
    }

    @Test
    void rendersResourceWithVirtualThreads() throws IOException {
        PipelineStepModel model = new PipelineStepModel.Builder()
            .serviceName("ProcessPaymentService")
            .servicePackage("org.example.service")
            .serviceClassName(ClassName.get("org.example.service", "ProcessPaymentService"))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .executionMode(ExecutionMode.VIRTUAL_THREADS)
            .inputMapping(new TypeMapping(
                ClassName.get("org.example.domain", "Payment"),
                ClassName.get("org.example.mapper", "PaymentMapper"),
                true))
            .outputMapping(new TypeMapping(
                ClassName.get("org.example.domain", "Receipt"),
                ClassName.get("org.example.mapper", "ReceiptMapper"),
                true))
            .enabledTargets(Set.of(GenerationTarget.REST_RESOURCE))
            .build();

        String source = renderAndReadSource(
            new RestBinding(model, null),
            "org/example/service/pipeline/ProcessPaymentResource.java");

        assertTrue(source.contains("@RunOnVirtualThread"));
    }

    @Test
    void verifyTargetReturnsRestResource() {
        RestResourceRenderer renderer = new RestResourceRenderer();
        assertEquals(GenerationTarget.REST_RESOURCE, renderer.target());
    }

    @Test
    void rendersResourceWithCustomPathOverride() throws IOException {
        PipelineStepModel model = baseModelBuilder("ProcessPaymentService", StreamingShape.UNARY_UNARY)
            .inputMapping(new TypeMapping(
                ClassName.get("org.example.domain", "Payment"),
                ClassName.get("org.example.mapper", "PaymentMapper"),
                true))
            .outputMapping(new TypeMapping(
                ClassName.get("org.example.domain", "Receipt"),
                ClassName.get("org.example.mapper", "ReceiptMapper"),
                true))
            .build();

        String source = renderAndReadSource(
            new RestBinding(model, "/custom/payment/path"),
            "org/pipelineframework/csv/service/pipeline/ProcessPaymentResource.java");

        assertTrue(source.contains("@Path(\"/custom/payment/path\")"));
    }

    @Test
    void rendersGetServiceMethodForUnaryUnary() throws IOException {
        PipelineStepModel model = baseModelBuilder("ProcessPaymentService", StreamingShape.UNARY_UNARY)
            .inputMapping(new TypeMapping(
                ClassName.get("org.example.domain", "Payment"),
                ClassName.get("org.example.mapper", "PaymentMapper"),
                true))
            .outputMapping(new TypeMapping(
                ClassName.get("org.example.domain", "Receipt"),
                ClassName.get("org.example.mapper", "ReceiptMapper"),
                true))
            .build();

        String source = renderAndReadSource(
            new RestBinding(model, null),
            "org/pipelineframework/csv/service/pipeline/ProcessPaymentResource.java");

        assertTrue(source.contains("protected ReactiveService<Payment, Receipt> getService()"));
        assertTrue(source.contains("return domainService"));
    }

    @Test
    void rendersFromDtoMethodWithMapping() throws IOException {
        PipelineStepModel model = baseModelBuilder("ProcessPaymentService", StreamingShape.UNARY_UNARY)
            .inputMapping(new TypeMapping(
                ClassName.get("org.example.domain", "Payment"),
                ClassName.get("org.example.mapper", "PaymentMapper"),
                true))
            .outputMapping(new TypeMapping(
                ClassName.get("org.example.domain", "Receipt"),
                ClassName.get("org.example.mapper", "ReceiptMapper"),
                true))
            .build();

        String source = renderAndReadSource(
            new RestBinding(model, null),
            "org/pipelineframework/csv/service/pipeline/ProcessPaymentResource.java");

        assertTrue(source.contains("protected Payment fromDto(PaymentDto dto)"));
        assertTrue(source.contains("return inboundMapper.fromExternal(dto)"));
    }

    @Test
    void rendersToDtoMethodWithMapping() throws IOException {
        PipelineStepModel model = baseModelBuilder("ProcessPaymentService", StreamingShape.UNARY_UNARY)
            .inputMapping(new TypeMapping(
                ClassName.get("org.example.domain", "Payment"),
                ClassName.get("org.example.mapper", "PaymentMapper"),
                true))
            .outputMapping(new TypeMapping(
                ClassName.get("org.example.domain", "Receipt"),
                ClassName.get("org.example.mapper", "ReceiptMapper"),
                true))
            .build();

        String source = renderAndReadSource(
            new RestBinding(model, null),
            "org/pipelineframework/csv/service/pipeline/ProcessPaymentResource.java");

        assertTrue(source.contains("protected ReceiptDto toDto(Receipt domain)"));
        assertTrue(source.contains("return outboundMapper.toExternal(domain)"));
    }

    @Test
    void rendersPostAnnotationForProcessMethod() throws IOException {
        PipelineStepModel model = baseModelBuilder("ProcessPaymentService", StreamingShape.UNARY_UNARY)
            .inputMapping(new TypeMapping(
                ClassName.get("org.example.domain", "Payment"),
                ClassName.get("org.example.mapper", "PaymentMapper"),
                true))
            .outputMapping(new TypeMapping(
                ClassName.get("org.example.domain", "Receipt"),
                ClassName.get("org.example.mapper", "ReceiptMapper"),
                true))
            .build();

        String source = renderAndReadSource(
            new RestBinding(model, null),
            "org/pipelineframework/csv/service/pipeline/ProcessPaymentResource.java");

        assertTrue(source.contains("@POST"));
    }

    @Test
    void rendersRestStreamElementTypeForUnaryStreaming() throws IOException {
        PipelineStepModel model = baseModelBuilder("StreamPaymentsService", StreamingShape.UNARY_STREAMING)
            .inputMapping(new TypeMapping(
                ClassName.get("org.example.domain", "Query"),
                ClassName.get("org.example.mapper", "QueryMapper"),
                true))
            .outputMapping(new TypeMapping(
                ClassName.get("org.example.domain", "Payment"),
                ClassName.get("org.example.mapper", "PaymentMapper"),
                true))
            .build();

        String source = renderAndReadSource(
            new RestBinding(model, null),
            "org/pipelineframework/csv/service/pipeline/StreamPaymentsResource.java");

        assertTrue(source.contains("@RestStreamElementType(\"application/json\")"));
    }
}
