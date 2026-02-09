package org.pipelineframework.processor.renderer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.annotation.processing.ProcessingEnvironment;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.ExecutionMode;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.OrchestratorBinding;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.StreamingShape;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OrchestratorFunctionHandlerRendererTest {

    @TempDir
    Path tempDir;

    @Test
    void rendersUnaryFunctionHandler() throws IOException {
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getFiler()).thenReturn(new TestFiler(tempDir));

        OrchestratorFunctionHandlerRenderer renderer = new OrchestratorFunctionHandlerRenderer();
        renderer.render(buildBinding(), new GenerationContext(processingEnv, tempDir, DeploymentRole.REST_SERVER,
            java.util.Set.of(), null, null));

        Path generatedSource = tempDir.resolve("com/example/orchestrator/service/PipelineRunFunctionHandler.java");
        String source = Files.readString(generatedSource);

        assertTrue(source.contains("implements RequestHandler<InputTypeDto, OutputTypeDto>"));
        assertTrue(source.contains("PipelineRunResource resource"));
        assertTrue(source.contains("handleRequest(InputTypeDto input, Context context)"));
        assertTrue(source.contains("return resource.run(input).await().indefinitely()"));
    }

    private OrchestratorBinding buildBinding() {
        PipelineStepModel model = new PipelineStepModel(
            "OrchestratorService",
            "OrchestratorService",
            "com.example.orchestrator.service",
            ClassName.get("com.example.orchestrator.service", "OrchestratorService"),
            null,
            null,
            StreamingShape.UNARY_UNARY,
            java.util.Set.of(GenerationTarget.REST_RESOURCE),
            ExecutionMode.DEFAULT,
            DeploymentRole.ORCHESTRATOR_CLIENT,
            false,
            null
        );
        return new OrchestratorBinding(
            model,
            "com.example",
            "REST",
            "InputType",
            "OutputType",
            false,
            false,
            "ProcessAlphaService",
            StreamingShape.UNARY_UNARY,
            null,
            null,
            null
        );
    }
}

