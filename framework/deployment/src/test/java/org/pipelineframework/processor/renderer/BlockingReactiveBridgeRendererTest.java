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
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.ServiceApiKind;
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.processor.ir.TypeMapping;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class BlockingReactiveBridgeRendererTest {

    @TempDir
    Path tempDir;

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
}
