package org.pipelineframework.processor.renderer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import javax.annotation.processing.ProcessingEnvironment;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.ExecutionMode;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.ServiceApiKind;
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.processor.ir.TypeMapping;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class BlockingReactiveBridgeRendererTest {

    @TempDir
    Path tempDir;

    @Test
    void targetReturnsBlockingReactiveBridgeTarget() {
        assertEquals(GenerationTarget.BLOCKING_REACTIVE_BRIDGE, new BlockingReactiveBridgeRenderer().target());
    }

    @Test
    void rendersIteratorBridgeUsingEmitIterator() throws IOException {
        PipelineStepModel model = new PipelineStepModel.Builder()
            .serviceName("ProcessCsvPaymentsInputService")
            .generatedName("ProcessCsvPaymentsInput")
            .servicePackage("org.pipelineframework.csv.service")
            .serviceClassName(ClassName.get("org.pipelineframework.csv.service", "ProcessCsvPaymentsInputService"))
            .streamingShape(StreamingShape.UNARY_STREAMING)
            .executionMode(ExecutionMode.DEFAULT)
            .serviceApiKind(ServiceApiKind.BLOCKING_ITERATOR)
            .inputMapping(new TypeMapping(
                ClassName.get("org.pipelineframework.csv.common.domain", "CsvPaymentsInputFile"),
                null,
                false))
            .outputMapping(new TypeMapping(
                ClassName.get("org.pipelineframework.csv.common.domain", "PaymentRecord"),
                null,
                false))
            .enabledTargets(Set.of())
            .build();

        new BlockingReactiveBridgeRenderer().render(model, new GenerationContext(
            mock(ProcessingEnvironment.class),
            tempDir,
            DeploymentRole.PIPELINE_SERVER,
            Set.of(),
            null,
            null));

        String source = Files.readString(tempDir.resolve(
            "org/pipelineframework/csv/service/pipeline/ProcessCsvPaymentsInputBlockingReactiveBridge.java"));

        assertTrue(source.contains("implements ReactiveStreamingService<CsvPaymentsInputFile, PaymentRecord>"));
        assertTrue(source.contains("emitIterator(false, () -> blockingService.iterateBlocking(processableObj))"));
    }

    @Test
    void rendersUnaryBlockingBridgeUsingSupply() throws IOException {
        PipelineStepModel model = buildModel("UnaryBlockingService", "UnaryBlocking",
            StreamingShape.UNARY_UNARY, ServiceApiKind.BLOCKING, ExecutionMode.DEFAULT);

        new BlockingReactiveBridgeRenderer().render(model, generationContext());

        String source = readGeneratedSource("UnaryBlockingBlockingReactiveBridge.java");

        assertTrue(source.contains("implements ReactiveService<"), "should implement ReactiveService");
        assertTrue(source.contains("blockingExecutionSupport.supply(false,"), "should delegate to supply()");
    }

    @Test
    void rendersServerStreamingBlockingBridgeUsingEmitList() throws IOException {
        PipelineStepModel model = buildModel("StreamingBlockingService", "StreamingBlocking",
            StreamingShape.UNARY_STREAMING, ServiceApiKind.BLOCKING, ExecutionMode.DEFAULT);

        new BlockingReactiveBridgeRenderer().render(model, generationContext());

        String source = readGeneratedSource("StreamingBlockingBlockingReactiveBridge.java");

        assertTrue(source.contains("implements ReactiveStreamingService<"), "should implement ReactiveStreamingService");
        assertTrue(source.contains("blockingExecutionSupport.emitList(false,"), "should delegate to emitList()");
    }

    @Test
    void rendersClientStreamingBlockingBridgeUsingTransformToUni() throws IOException {
        PipelineStepModel model = buildModel("ClientStreamingService", "ClientStreaming",
            StreamingShape.STREAMING_UNARY, ServiceApiKind.BLOCKING, ExecutionMode.DEFAULT);

        new BlockingReactiveBridgeRenderer().render(model, generationContext());

        String source = readGeneratedSource("ClientStreamingBlockingReactiveBridge.java");

        assertTrue(source.contains("implements ReactiveStreamingClientService<"), "should implement ReactiveStreamingClientService");
        assertTrue(source.contains("transformToUni"), "should use transformToUni for client streaming");
        assertTrue(source.contains("blockingExecutionSupport.supply(false,"), "should delegate to supply()");
    }

    @Test
    void rendersBidirectionalStreamingBlockingBridgeUsingTransformToMulti() throws IOException {
        PipelineStepModel model = buildModel("BidiStreamingService", "BidiStreaming",
            StreamingShape.STREAMING_STREAMING, ServiceApiKind.BLOCKING, ExecutionMode.DEFAULT);

        new BlockingReactiveBridgeRenderer().render(model, generationContext());

        String source = readGeneratedSource("BidiStreamingBlockingReactiveBridge.java");

        assertTrue(source.contains("implements ReactiveBidirectionalStreamingService<"),
            "should implement ReactiveBidirectionalStreamingService");
        assertTrue(source.contains("transformToMulti"), "should use transformToMulti for bidi streaming");
        assertTrue(source.contains("blockingExecutionSupport.emitList(false,"), "should delegate to emitList()");
    }

    @Test
    void rendersVirtualThreadsBridgeWithTrueFlag() throws IOException {
        PipelineStepModel model = buildModel("VirtualBlockingService", "VirtualBlocking",
            StreamingShape.UNARY_UNARY, ServiceApiKind.BLOCKING, ExecutionMode.VIRTUAL_THREADS);

        new BlockingReactiveBridgeRenderer().render(model, generationContext());

        String source = readGeneratedSource("VirtualBlockingBlockingReactiveBridge.java");

        assertTrue(source.contains("blockingExecutionSupport.supply(true,"),
            "virtual threads mode should pass true to supply()");
    }

    @Test
    void rendersIteratorBridgeWithVirtualThreadsFlag() throws IOException {
        PipelineStepModel model = buildModel("VirtualIteratorService", "VirtualIterator",
            StreamingShape.UNARY_STREAMING, ServiceApiKind.BLOCKING_ITERATOR, ExecutionMode.VIRTUAL_THREADS);

        new BlockingReactiveBridgeRenderer().render(model, generationContext());

        String source = readGeneratedSource("VirtualIteratorBlockingReactiveBridge.java");

        assertTrue(source.contains("emitIterator(true,"),
            "virtual threads mode should pass true to emitIterator()");
    }

    @Test
    void renderedBridgeHasApplicationScopedAnnotation() throws IOException {
        PipelineStepModel model = buildModel("ScopedBlockingService", "ScopedBlocking",
            StreamingShape.UNARY_UNARY, ServiceApiKind.BLOCKING, ExecutionMode.DEFAULT);

        new BlockingReactiveBridgeRenderer().render(model, generationContext());

        String source = readGeneratedSource("ScopedBlockingBlockingReactiveBridge.java");

        assertTrue(source.contains("@ApplicationScoped"), "bridge should be application-scoped CDI bean");
    }

    @Test
    void renderedBridgeInjectsBlockingService() throws IOException {
        PipelineStepModel model = buildModel("InjectTestService", "InjectTest",
            StreamingShape.UNARY_UNARY, ServiceApiKind.BLOCKING, ExecutionMode.DEFAULT);

        new BlockingReactiveBridgeRenderer().render(model, generationContext());

        String source = readGeneratedSource("InjectTestBlockingReactiveBridge.java");

        assertTrue(source.contains("blockingService"), "bridge should inject the blocking service");
        assertTrue(source.contains("blockingExecutionSupport"), "bridge should inject blocking execution support");
    }

    private PipelineStepModel buildModel(
            String serviceName,
            String generatedName,
            StreamingShape shape,
            ServiceApiKind apiKind,
            ExecutionMode executionMode) {
        return new PipelineStepModel.Builder()
            .serviceName(serviceName)
            .generatedName(generatedName)
            .servicePackage("com.example.service")
            .serviceClassName(ClassName.get("com.example.service", serviceName))
            .streamingShape(shape)
            .executionMode(executionMode)
            .serviceApiKind(apiKind)
            .inputMapping(new TypeMapping(
                ClassName.get("com.example.domain", "Input"),
                null,
                false))
            .outputMapping(new TypeMapping(
                ClassName.get("com.example.domain", "Output"),
                null,
                false))
            .enabledTargets(Set.of())
            .build();
    }

    private GenerationContext generationContext() {
        return new GenerationContext(
            mock(ProcessingEnvironment.class),
            tempDir,
            DeploymentRole.PIPELINE_SERVER,
            Set.of(),
            null,
            null);
    }

    private String readGeneratedSource(String simpleFileName) throws IOException {
        return Files.readString(
            tempDir.resolve("com/example/service/pipeline/" + simpleFileName));
    }
}
