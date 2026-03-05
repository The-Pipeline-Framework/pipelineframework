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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OrchestratorFunctionHandlerRendererTest {
    // Parity matrix mirrors RestFunctionHandlerRendererTest for orchestrator-generated handlers.

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
            source.contains("inputStream -> inputStream.collect().asList().onItem().transformToUni(resource::run)"),
            () -> "expected streaming-input lambda bridge missing. source:\n" + source);
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
            source.contains("invokeLocal = new LocalManyToManyFunctionInvokeAdapter<InputTypeDto, OutputTypeDto>(resource::run"),
            () -> "expected streaming-input many-to-many direct delegate missing. source:\n" + source);
        assertFalse(
            source.contains("inputStream -> resource.run(inputStream.collect().asList().await().indefinitely())"),
            () -> "unexpected blocking streaming-input lambda found. source:\n" + source);
        assertTrue(
            source.contains("return FunctionTransportBridge.invokeManyToMany(input, transportContext, source, invoke, sink)"),
            () -> "expected FunctionTransportBridge.invokeManyToMany invocation missing. source:\n" + source);
    }

    @Test
    void rendersAsyncFunctionHandlers() throws IOException {
        renderAndReadSource(buildBinding());

        Path runAsyncPath = tempDir.resolve("com/example/orchestrator/service/PipelineRunAsyncFunctionHandler.java");
        Path statusPath = tempDir.resolve("com/example/orchestrator/service/PipelineExecutionStatusFunctionHandler.java");
        Path resultPath = tempDir.resolve("com/example/orchestrator/service/PipelineExecutionResultFunctionHandler.java");
        Path runAsyncRequestPath = tempDir.resolve("com/example/orchestrator/service/PipelineRunAsyncRequest.java");
        Path lookupRequestPath = tempDir.resolve("com/example/orchestrator/service/PipelineExecutionLookupRequest.java");

        assertTrue(Files.exists(runAsyncPath));
        assertTrue(Files.exists(statusPath));
        assertTrue(Files.exists(resultPath));
        assertTrue(Files.exists(runAsyncRequestPath));
        assertTrue(Files.exists(lookupRequestPath));

        String runAsyncSource = Files.readString(runAsyncPath);
        String statusSource = Files.readString(statusPath);
        String resultSource = Files.readString(resultPath);
        assertTrue(runAsyncSource.contains("implements RequestHandler<PipelineRunAsyncRequest, RunAsyncAcceptedDto>"));
        assertTrue(runAsyncSource.contains("pipelineExecutionService.executePipelineAsync"));
        assertTrue(statusSource.contains("implements RequestHandler<PipelineExecutionLookupRequest, ExecutionStatusDto>"));
        assertTrue(statusSource.contains("pipelineExecutionService.getExecutionStatus"));
        assertTrue(resultSource.contains("pipelineExecutionService.<OutputTypeDto>getExecutionResult"));
        assertTrue(resultSource.contains("implements RequestHandler<PipelineExecutionLookupRequest,"));
        assertTrue(resultSource.contains("getExecutionResult("));
    }

    @Test
    void rendersRequestDTOsWithCorrectFields() throws IOException {
        renderAndReadSource(buildBinding());

        Path runAsyncRequestPath = tempDir.resolve("com/example/orchestrator/service/PipelineRunAsyncRequest.java");
        String runAsyncRequest = Files.readString(runAsyncRequestPath);

        assertTrue(runAsyncRequest.contains("public InputTypeDto input;"));
        assertTrue(runAsyncRequest.contains("public List<InputTypeDto> inputBatch;"));
        assertTrue(runAsyncRequest.contains("public String tenantId;"));
        assertTrue(runAsyncRequest.contains("public String idempotencyKey;"));
    }

    @Test
    void rendersLookupRequestWithCorrectFields() throws IOException {
        renderAndReadSource(buildBinding());

        Path lookupRequestPath = tempDir.resolve("com/example/orchestrator/service/PipelineExecutionLookupRequest.java");
        String lookupRequest = Files.readString(lookupRequestPath);

        assertTrue(lookupRequest.contains("public String tenantId;"));
        assertTrue(lookupRequest.contains("public String executionId;"));
    }

    @Test
    void rendersStatusHandlerWithValidation() throws IOException {
        renderAndReadSource(buildBinding());

        Path statusPath = tempDir.resolve("com/example/orchestrator/service/PipelineExecutionStatusFunctionHandler.java");
        String statusSource = Files.readString(statusPath);

        assertTrue(statusSource.contains("if (request == null || request.executionId == null || request.executionId.isBlank())"));
        assertTrue(statusSource.contains("throw new IllegalArgumentException(\"executionId is required\")"));
    }

    @Test
    void rendersResultHandlerWithValidation() throws IOException {
        renderAndReadSource(buildBinding());

        Path resultPath = tempDir.resolve("com/example/orchestrator/service/PipelineExecutionResultFunctionHandler.java");
        String resultSource = Files.readString(resultPath);

        assertTrue(resultSource.contains("if (request == null || request.executionId == null || request.executionId.isBlank())"));
        assertTrue(resultSource.contains("throw new IllegalArgumentException(\"executionId is required\")"));
    }

    @Test
    void handlerReturnsGenerationTargetRESTResource() {
        OrchestratorFunctionHandlerRenderer renderer = new OrchestratorFunctionHandlerRenderer();
        assertEquals(GenerationTarget.REST_RESOURCE, renderer.target());
    }

    @Test
    void rendersStreamingInputHandlerWithBatchSupport() throws IOException {
        renderAndReadSource(buildStreamingBinding(true, false));
        Path runAsyncPath = tempDir.resolve("com/example/orchestrator/service/PipelineRunAsyncFunctionHandler.java");
        String source = Files.readString(runAsyncPath);

        assertTrue(source.contains("if (request != null && request.inputBatch != null && !request.inputBatch.isEmpty())"));
        assertTrue(source.contains("executionInput = Multi.createFrom().iterable(request.inputBatch)"));
    }

    @Test
    void rendersStreamingOutputResultHandler() throws IOException {
        renderAndReadSource(buildStreamingBinding(false, true));

        Path resultPath = tempDir.resolve("com/example/orchestrator/service/PipelineExecutionResultFunctionHandler.java");
        String resultSource = Files.readString(resultPath);

        assertTrue(resultSource.contains("implements RequestHandler<PipelineExecutionLookupRequest, List<OutputTypeDto>>"));
        assertTrue(resultSource.contains("pipelineExecutionService.<List<OutputTypeDto>>getExecutionResult"));
    }

    @Test
    void rendersTransportContextWithAllAttributes() throws IOException {
        String source = renderAndReadSource(buildBinding());

        assertTrue(source.contains("FunctionTransportContext.ATTR_TRANSPORT_PROTOCOL"));
        assertTrue(source.contains("FunctionTransportContext.ATTR_CORRELATION_ID"));
        assertTrue(source.contains("FunctionTransportContext.ATTR_EXECUTION_ID"));
        assertTrue(source.contains("FunctionTransportContext.ATTR_RETRY_ATTEMPT"));
        assertTrue(source.contains("FunctionTransportContext.ATTR_DISPATCH_TS_EPOCH_MS"));
    }

    @Test
    void rendersErrorHandlingInHandleRequest() throws IOException {
        String source = renderAndReadSource(buildBinding());

        assertTrue(source.contains("catch (RuntimeException e)"));
        assertTrue(source.contains("throw new RuntimeException(\"Failed handleRequest -> resource.run for input DTO\", e)"));
    }

    @Test
    void staticMethodReturnsCorrectHandlerFqcn() {
        String fqcn = OrchestratorFunctionHandlerRenderer.handlerFqcn("com.example");
        assertEquals("com.example.orchestrator.service.PipelineRunFunctionHandler", fqcn);
    }

    @Test
    void staticMethodReturnsCorrectRunAsyncHandlerFqcn() {
        String fqcn = OrchestratorFunctionHandlerRenderer.runAsyncHandlerFqcn("com.example");
        assertEquals("com.example.orchestrator.service.PipelineRunAsyncFunctionHandler", fqcn);
    }

    @Test
    void staticMethodReturnsCorrectStatusHandlerFqcn() {
        String fqcn = OrchestratorFunctionHandlerRenderer.statusHandlerFqcn("com.example");
        assertEquals("com.example.orchestrator.service.PipelineExecutionStatusFunctionHandler", fqcn);
    }

    @Test
    void staticMethodReturnsCorrectResultHandlerFqcn() {
        String fqcn = OrchestratorFunctionHandlerRenderer.resultHandlerFqcn("com.example");
        assertEquals("com.example.orchestrator.service.PipelineExecutionResultFunctionHandler", fqcn);
    }

    @Test
    void rendersRunAsyncHandlerWithStreamingInputHandlesEmptyInput() throws IOException {
        renderAndReadSource(buildStreamingBinding(true, false));
        Path runAsyncPath = tempDir.resolve("com/example/orchestrator/service/PipelineRunAsyncFunctionHandler.java");
        String source = Files.readString(runAsyncPath);

        assertTrue(source.contains("executionInput = Multi.createFrom().empty()"));
    }

    @Test
    void rendersRunAsyncHandlerWithUnaryInputHandlesNullInput() throws IOException {
        renderAndReadSource(buildStreamingBinding(false, false));
        Path runAsyncPath = tempDir.resolve("com/example/orchestrator/service/PipelineRunAsyncFunctionHandler.java");
        String source = Files.readString(runAsyncPath);

        assertTrue(source.contains("executionInput = null"));
    }

    @Test
    void rendersRunAsyncHandlerWithUnaryInputHandlesBatchAsFirstItem() throws IOException {
        renderAndReadSource(buildStreamingBinding(false, false));
        Path runAsyncPath = tempDir.resolve("com/example/orchestrator/service/PipelineRunAsyncFunctionHandler.java");
        String source = Files.readString(runAsyncPath);

        assertTrue(source.contains("executionInput = request.inputBatch.get(0)"));
    }

    @Test
    void rendersRunAsyncHandlerPassesStreamingOutputFlag() throws IOException {
        renderAndReadSource(buildStreamingBinding(false, true));
        Path runAsyncPath = tempDir.resolve("com/example/orchestrator/service/PipelineRunAsyncFunctionHandler.java");
        String source = Files.readString(runAsyncPath);

        assertTrue(source.contains("executePipelineAsync(executionInput, tenantId, idempotencyKey, true)"));
    }

    @Test
    void rendersRunAsyncHandlerPassesNonStreamingOutputFlag() throws IOException {
        renderAndReadSource(buildStreamingBinding(false, false));
        Path runAsyncPath = tempDir.resolve("com/example/orchestrator/service/PipelineRunAsyncFunctionHandler.java");
        String source = Files.readString(runAsyncPath);

        assertTrue(source.contains("executePipelineAsync(executionInput, tenantId, idempotencyKey, false)"));
    }

    @Test
    void rendersResultHandlerWithCorrectOutputType() throws IOException {
        renderAndReadSource(buildStreamingBinding(false, false));
        Path resultPath = tempDir.resolve("com/example/orchestrator/service/PipelineExecutionResultFunctionHandler.java");
        String resultSource = Files.readString(resultPath);

        assertTrue(resultSource.contains("OutputTypeDto.class"));
    }

    @Test
    void rendersAllHandlersWithApplicationScopedAnnotation() throws IOException {
        renderAndReadSource(buildBinding());

        Path runAsyncPath = tempDir.resolve("com/example/orchestrator/service/PipelineRunAsyncFunctionHandler.java");
        Path statusPath = tempDir.resolve("com/example/orchestrator/service/PipelineExecutionStatusFunctionHandler.java");
        Path resultPath = tempDir.resolve("com/example/orchestrator/service/PipelineExecutionResultFunctionHandler.java");

        String runAsync = Files.readString(runAsyncPath);
        String status = Files.readString(statusPath);
        String result = Files.readString(resultPath);

        assertTrue(runAsync.contains("@ApplicationScoped"));
        assertTrue(status.contains("@ApplicationScoped"));
        assertTrue(result.contains("@ApplicationScoped"));
    }

    @Test
    void rendersAllHandlersWithGeneratedRoleAnnotation() throws IOException {
        renderAndReadSource(buildBinding());

        Path mainHandlerPath = tempDir.resolve("com/example/orchestrator/service/PipelineRunFunctionHandler.java");
        Path runAsyncPath = tempDir.resolve("com/example/orchestrator/service/PipelineRunAsyncFunctionHandler.java");
        Path statusPath = tempDir.resolve("com/example/orchestrator/service/PipelineExecutionStatusFunctionHandler.java");
        Path resultPath = tempDir.resolve("com/example/orchestrator/service/PipelineExecutionResultFunctionHandler.java");

        String mainHandler = Files.readString(mainHandlerPath);
        String runAsync = Files.readString(runAsyncPath);
        String status = Files.readString(statusPath);
        String result = Files.readString(resultPath);

        assertTrue(mainHandler.contains("@GeneratedRole"));
        assertTrue(runAsync.contains("@GeneratedRole"));
        assertTrue(status.contains("@GeneratedRole"));
        assertTrue(result.contains("@GeneratedRole"));
    }

    @Test
    void rendersHandlerWithInjectAnnotationOnResource() throws IOException {
        String source = renderAndReadSource(buildBinding());

        assertTrue(source.contains("@Inject"));
        assertTrue(source.contains("PipelineRunResource resource"));
    }

    @Test
    void rendersHandlerWithCorrectNamedAnnotationValue() throws IOException {
        String source = renderAndReadSource(buildBinding());

        assertTrue(source.contains("@Named(\"PipelineRunFunctionHandler\")"));
    }

    @Test
    void rendersHandlerUsesCorrectOrchestratorPrefix() throws IOException {
        String source = renderAndReadSource(buildBinding());

        assertTrue(source.contains("\"orchestrator.InputType\""));
        assertTrue(source.contains("\"orchestrator.OutputType\""));
    }

    @Test
    void rendersHandlerUsesApiVersionV1() throws IOException {
        String source = renderAndReadSource(buildBinding());

        assertTrue(source.contains("\"v1\""));
    }

    @Test
    void rendersStatusHandlerReturnsExecutionStatusDto() throws IOException {
        renderAndReadSource(buildBinding());
        Path statusPath = tempDir.resolve("com/example/orchestrator/service/PipelineExecutionStatusFunctionHandler.java");
        String statusSource = Files.readString(statusPath);

        assertTrue(statusSource.contains("ExecutionStatusDto"));
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