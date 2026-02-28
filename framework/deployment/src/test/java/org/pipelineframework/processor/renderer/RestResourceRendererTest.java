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
import static org.junit.jupiter.api.Assertions.assertThrows;
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
    void rendersStreamingUnaryProcessMethod() throws IOException {
        PipelineStepModel model = baseModelBuilder("AggregateService", StreamingShape.STREAMING_UNARY)
            .inputMapping(new TypeMapping(
                ClassName.get("org.pipelineframework.csv.common.domain", "DataPoint"),
                ClassName.get("org.pipelineframework.csv.common.mapper", "DataPointMapper"),
                true))
            .outputMapping(new TypeMapping(
                ClassName.get("org.pipelineframework.csv.common.domain", "Summary"),
                ClassName.get("org.pipelineframework.csv.common.mapper", "SummaryMapper"),
                true))
            .build();

        String source = renderAndReadSource(
            new RestBinding(model, null),
            "org/pipelineframework/csv/service/pipeline/AggregateResource.java");
        assertTrue(source.contains("List<DataPointDto> inputDtos"));
        assertTrue(source.contains("Uni<SummaryDto> process"));
        assertTrue(source.contains("Multi.createFrom().iterable(inputDtos)"));
    }

    @Test
    void rendersBidirectionalStreamingProcessMethod() throws IOException {
        PipelineStepModel model = baseModelBuilder("StreamProcessService", StreamingShape.STREAMING_STREAMING)
            .inputMapping(new TypeMapping(
                ClassName.get("org.pipelineframework.csv.common.domain", "StreamInput"),
                ClassName.get("org.pipelineframework.csv.common.mapper", "StreamInputMapper"),
                true))
            .outputMapping(new TypeMapping(
                ClassName.get("org.pipelineframework.csv.common.domain", "StreamOutput"),
                ClassName.get("org.pipelineframework.csv.common.mapper", "StreamOutputMapper"),
                true))
            .build();

        String source = renderAndReadSource(
            new RestBinding(model, null),
            "org/pipelineframework/csv/service/pipeline/StreamProcessResource.java");
        assertTrue(source.contains("Multi<StreamInputDto> inputDtos"));
        assertTrue(source.contains("Multi<StreamOutputDto> process"));
        assertTrue(source.contains("@Consumes(\"application/x-ndjson\")"));
        assertTrue(source.contains("@Produces(\"application/x-ndjson\")"));
        assertTrue(source.contains("@RestStreamElementType(\"application/json\")"));
    }

    @Test
    void rendersResourceWithVirtualThreads() throws IOException {
        PipelineStepModel model = new PipelineStepModel.Builder()
            .serviceName("ProcessPaymentStatusReactiveService")
            .servicePackage("org.pipelineframework.csv.service")
            .serviceClassName(ClassName.get("org.pipelineframework.csv.service", "ProcessPaymentStatusReactiveService"))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .executionMode(ExecutionMode.VIRTUAL_THREADS)
            .inputMapping(new TypeMapping(
                ClassName.get("org.pipelineframework.csv.common.domain", "PaymentStatus"),
                ClassName.get("org.pipelineframework.csv.common.mapper", "PaymentStatusMapper"),
                true))
            .outputMapping(new TypeMapping(
                ClassName.get("org.pipelineframework.csv.common.domain", "PaymentOutput"),
                ClassName.get("org.pipelineframework.csv.common.mapper", "PaymentOutputMapper"),
                true))
            .enabledTargets(Set.of(GenerationTarget.REST_RESOURCE))
            .build();

        String source = renderAndReadSource(
            new RestBinding(model, null),
            "org/pipelineframework/csv/service/pipeline/ProcessPaymentStatusResource.java");
        assertTrue(source.contains("@RunOnVirtualThread"));
    }

    @Test
    void rendersWithCustomPathOverride() throws IOException {
        PipelineStepModel model = baseModelBuilder("CustomService", StreamingShape.UNARY_UNARY)
            .inputMapping(new TypeMapping(
                ClassName.get("org.pipelineframework.csv.common.domain", "Input"),
                ClassName.get("org.pipelineframework.csv.common.mapper", "InputMapper"),
                true))
            .outputMapping(new TypeMapping(
                ClassName.get("org.pipelineframework.csv.common.domain", "Output"),
                ClassName.get("org.pipelineframework.csv.common.mapper", "OutputMapper"),
                true))
            .build();

        String source = renderAndReadSource(
            new RestBinding(model, "/custom/path"),
            "org/pipelineframework/csv/service/pipeline/CustomResource.java");
        assertTrue(source.contains("@Path(\"/custom/path\")"));
    }

    @Test
    void validatesNonNullModelRequired() {
        RestResourceRenderer renderer = new RestResourceRenderer();
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        GenerationContext context = new GenerationContext(processingEnv, tempDir,
            DeploymentRole.REST_SERVER, Set.of(), null, null);

        PipelineStepModel invalidModel = null;
        RestBinding binding = new RestBinding(invalidModel, null);

        IllegalStateException error = assertThrows(IllegalStateException.class,
            () -> renderer.render(binding, context));
        assertTrue(error.getMessage().contains("non-null PipelineStepModel"));
    }

    @Test
    void validatesInputMappingRequired() {
        PipelineStepModel model = new PipelineStepModel.Builder()
            .serviceName("InvalidService")
            .servicePackage("org.example")
            .serviceClassName(ClassName.get("org.example", "InvalidService"))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .executionMode(ExecutionMode.DEFAULT)
            .inputMapping(null) // Missing
            .outputMapping(new TypeMapping(
                ClassName.get("org.example.domain", "Output"),
                ClassName.get("org.example.mapper", "OutputMapper"),
                true))
            .enabledTargets(Set.of(GenerationTarget.REST_RESOURCE))
            .build();

        RestResourceRenderer renderer = new RestResourceRenderer();
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        GenerationContext context = new GenerationContext(processingEnv, tempDir,
            DeploymentRole.REST_SERVER, Set.of(), null, null);

        RestBinding binding = new RestBinding(model, null);

        IllegalStateException error = assertThrows(IllegalStateException.class,
            () -> renderer.render(binding, context));
        assertTrue(error.getMessage().contains("requires input/output mappings"));
    }

    @Test
    void validatesInputDomainTypeRequired() {
        PipelineStepModel model = new PipelineStepModel.Builder()
            .serviceName("InvalidService")
            .servicePackage("org.example")
            .serviceClassName(ClassName.get("org.example", "InvalidService"))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .executionMode(ExecutionMode.DEFAULT)
            .inputMapping(new TypeMapping(null, null, false)) // Missing domain type
            .outputMapping(new TypeMapping(
                ClassName.get("org.example.domain", "Output"),
                ClassName.get("org.example.mapper", "OutputMapper"),
                true))
            .enabledTargets(Set.of(GenerationTarget.REST_RESOURCE))
            .build();

        RestResourceRenderer renderer = new RestResourceRenderer();
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        GenerationContext context = new GenerationContext(processingEnv, tempDir,
            DeploymentRole.REST_SERVER, Set.of(), null, null);

        RestBinding binding = new RestBinding(model, null);

        IllegalStateException error = assertThrows(IllegalStateException.class,
            () -> renderer.render(binding, context));
        assertTrue(error.getMessage().contains("requires a non-null input domain type"));
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