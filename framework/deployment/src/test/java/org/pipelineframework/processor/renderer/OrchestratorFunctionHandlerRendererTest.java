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

import static org.junit.jupiter.api.Assertions.assertThrows;
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
        assertTrue(source.contains("@Named(\"PipelineRunFunctionHandler\")"));
        assertTrue(source.contains("PipelineRunResource resource"));
        assertTrue(source.contains("handleRequest(InputTypeDto input, Context context)"));
        assertTrue(source.contains("return resource.run(input).await().indefinitely()"));
    }

    @Test
    void rejectsStreamingOrchestratorBinding() {
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getFiler()).thenReturn(new TestFiler(tempDir));

        OrchestratorFunctionHandlerRenderer renderer = new OrchestratorFunctionHandlerRenderer();
        OrchestratorBinding streamingBinding = new TestOrchestratorBindingBuilder()
            .model(new PipelineStepModel(
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
            ))
            .basePackage("com.example")
            .transport("REST")
            .inputTypeName("InputType")
            .outputTypeName("OutputType")
            .inputStreaming(true)
            .outputStreaming(false)
            .firstStepServiceName("ProcessAlphaService")
            .firstStepStreamingShape(StreamingShape.UNARY_UNARY)
            .build();

        assertThrows(
            IllegalStateException.class,
            () -> renderer.render(streamingBinding, new GenerationContext(
                processingEnv, tempDir, DeploymentRole.REST_SERVER, java.util.Set.of(), null, null)));
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
        return new TestOrchestratorBindingBuilder()
            .model(model)
            .basePackage("com.example")
            .transport("REST")
            .inputTypeName("InputType")
            .outputTypeName("OutputType")
            .inputStreaming(false)
            .outputStreaming(false)
            .firstStepServiceName("ProcessAlphaService")
            .firstStepStreamingShape(StreamingShape.UNARY_UNARY)
            .build();
    }

    private static final class TestOrchestratorBindingBuilder {
        private PipelineStepModel model;
        private String basePackage;
        private String transport;
        private String inputTypeName;
        private String outputTypeName;
        private boolean inputStreaming;
        private boolean outputStreaming;
        private String firstStepServiceName;
        private StreamingShape firstStepStreamingShape;
        private String cliName;
        private String cliDescription;
        private String cliVersion;

        private TestOrchestratorBindingBuilder model(PipelineStepModel value) {
            this.model = value;
            return this;
        }

        private TestOrchestratorBindingBuilder basePackage(String value) {
            this.basePackage = value;
            return this;
        }

        private TestOrchestratorBindingBuilder transport(String value) {
            this.transport = value;
            return this;
        }

        private TestOrchestratorBindingBuilder inputTypeName(String value) {
            this.inputTypeName = value;
            return this;
        }

        private TestOrchestratorBindingBuilder outputTypeName(String value) {
            this.outputTypeName = value;
            return this;
        }

        private TestOrchestratorBindingBuilder inputStreaming(boolean value) {
            this.inputStreaming = value;
            return this;
        }

        private TestOrchestratorBindingBuilder outputStreaming(boolean value) {
            this.outputStreaming = value;
            return this;
        }

        private TestOrchestratorBindingBuilder firstStepServiceName(String value) {
            this.firstStepServiceName = value;
            return this;
        }

        private TestOrchestratorBindingBuilder firstStepStreamingShape(StreamingShape value) {
            this.firstStepStreamingShape = value;
            return this;
        }

        private TestOrchestratorBindingBuilder cliName(String value) {
            this.cliName = value;
            return this;
        }

        private TestOrchestratorBindingBuilder cliDescription(String value) {
            this.cliDescription = value;
            return this;
        }

        private TestOrchestratorBindingBuilder cliVersion(String value) {
            this.cliVersion = value;
            return this;
        }

        private OrchestratorBinding build() {
            return new OrchestratorBinding(
                model,
                basePackage,
                transport,
                inputTypeName,
                outputTypeName,
                inputStreaming,
                outputStreaming,
                firstStepServiceName,
                firstStepStreamingShape,
                cliName,
                cliDescription,
                cliVersion);
        }
    }
}
