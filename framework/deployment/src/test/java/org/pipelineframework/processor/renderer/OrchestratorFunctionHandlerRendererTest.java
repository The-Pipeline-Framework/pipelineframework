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
        String source = renderAndReadSource(buildBinding());

        assertTrue(source.contains("implements RequestHandler<InputTypeDto, OutputTypeDto>"));
        assertTrue(source.contains("@Named(\"PipelineRunFunctionHandler\")"));
        assertTrue(source.contains("PipelineRunResource resource"));
        assertTrue(source.contains("handleRequest(InputTypeDto input, Context context)"));
        assertTrue(
            source.contains("FunctionTransportContext transportContext = FunctionTransportContext.of("),
            () -> "expected FunctionTransportContext fragment missing. source:\n" + source);
        assertTrue(
            source.contains("FunctionSourceAdapter<InputTypeDto, InputTypeDto> source"),
            () -> "expected FunctionSourceAdapter fragment missing. source:\n" + source);
        assertTrue(
            source.contains("FunctionInvokeAdapter<InputTypeDto, OutputTypeDto> invokeLocal"),
            () -> "expected invokeLocal adapter fragment missing. source:\n" + source);
        assertTrue(
            source.contains("FunctionInvokeAdapter<InputTypeDto, OutputTypeDto> invokeRemote = new HttpRemoteFunctionInvokeAdapter<>()"),
            () -> "expected invokeRemote adapter fragment missing. source:\n" + source);
        assertTrue(
            source.contains("FunctionInvokeAdapter<InputTypeDto, OutputTypeDto> invoke = new InvocationModeRoutingFunctionInvokeAdapter<>(invokeLocal, invokeRemote)"),
            () -> "expected invocation-mode routing adapter fragment missing. source:\n" + source);
        assertTrue(
            source.contains("FunctionSinkAdapter<OutputTypeDto, OutputTypeDto> sink"),
            () -> "expected FunctionSinkAdapter fragment missing. source:\n" + source);
        assertTrue(
            source.contains("return UnaryFunctionTransportBridge.invoke(input, transportContext, source, invoke, sink)"),
            () -> "expected UnaryFunctionTransportBridge invocation fragment missing. source:\n" + source);
    }

    @Test
    void rendersStreamingOrchestratorBinding() throws IOException {
        String source = renderAndReadSource(buildStreamingBinding(true, false));
        assertTrue(
            source.contains("implements RequestHandler<Multi<InputTypeDto>, OutputTypeDto>"),
            () -> "expected full RequestHandler signature missing. source:\n" + source);
        assertTrue(
            source.contains("return FunctionTransportBridge.invokeManyToOne(input, transportContext, source, invoke, sink)"),
            () -> "expected FunctionTransportBridge.invokeManyToOne invocation missing. source:\n" + source);
    }

    @Test
    void rendersOneToManyOrchestratorBinding() throws IOException {
        String source = renderAndReadSource(buildStreamingBinding(false, true));
        assertTrue(
            source.contains("implements RequestHandler<InputTypeDto, List<OutputTypeDto>>"),
            () -> "expected full RequestHandler signature missing. source:\n" + source);
        assertTrue(
            source.contains("return FunctionTransportBridge.invokeOneToMany(input, transportContext, source, invoke, sink)"),
            () -> "expected FunctionTransportBridge.invokeOneToMany invocation missing. source:\n" + source);
    }

    @Test
    void rendersStreamingManyToManyOrchestratorBinding() throws IOException {
        String source = renderAndReadSource(buildStreamingBinding(true, true));

        assertTrue(
            source.contains("implements RequestHandler<Multi<InputTypeDto>, List<OutputTypeDto>>"),
            () -> "expected full RequestHandler signature missing. source:\n" + source);
        assertTrue(
            source.contains("return FunctionTransportBridge.invokeManyToMany(input, transportContext, source, invoke, sink)"),
            () -> "expected FunctionTransportBridge.invokeManyToMany invocation missing. source:\n" + source);
    }

    private String renderAndReadSource(OrchestratorBinding binding) throws IOException {
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getFiler()).thenReturn(new TestFiler(tempDir));

        OrchestratorFunctionHandlerRenderer renderer = new OrchestratorFunctionHandlerRenderer();
        renderer.render(binding, new GenerationContext(
            processingEnv, tempDir, DeploymentRole.REST_SERVER, java.util.Set.of(), null, null));

        Path generatedSource = tempDir.resolve("com/example/orchestrator/service/PipelineRunFunctionHandler.java");
        return Files.readString(generatedSource);
    }

    private OrchestratorBinding buildBinding() {
        return buildStreamingBinding(false, false);
    }

    private OrchestratorBinding buildStreamingBinding(boolean inputStreaming, boolean outputStreaming) {
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
            .inputStreaming(inputStreaming)
            .outputStreaming(outputStreaming)
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
