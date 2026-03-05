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
        assertTrue(runAsyncSource.contains("RunAsync unary handlers accept at most one item in inputBatch"));
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
    void handlerFqcnReturnsCorrectPath() {
        String fqcn = OrchestratorFunctionHandlerRenderer.handlerFqcn("com.example");
        assertEquals("com.example.orchestrator.service.PipelineRunFunctionHandler", fqcn);
    }

    @Test
    void runAsyncHandlerFqcnReturnsCorrectPath() {
        String fqcn = OrchestratorFunctionHandlerRenderer.runAsyncHandlerFqcn("com.example");
        assertEquals("com.example.orchestrator.service.PipelineRunAsyncFunctionHandler", fqcn);
    }

    @Test
    void statusHandlerFqcnReturnsCorrectPath() {
        String fqcn = OrchestratorFunctionHandlerRenderer.statusHandlerFqcn("com.example");
        assertEquals("com.example.orchestrator.service.PipelineExecutionStatusFunctionHandler", fqcn);
    }

    @Test
    void resultHandlerFqcnReturnsCorrectPath() {
        String fqcn = OrchestratorFunctionHandlerRenderer.resultHandlerFqcn("com.example");
        assertEquals("com.example.orchestrator.service.PipelineExecutionResultFunctionHandler", fqcn);
    }

    @Test
    void rendersRunAsyncHandlerWithStreamingInputBatchHandling() throws IOException {
        renderAndReadSource(buildStreamingBinding(true, false));
        Path runAsyncPath = tempDir.resolve("com/example/orchestrator/service/PipelineRunAsyncFunctionHandler.java");
        String source = Files.readString(runAsyncPath);

        assertTrue(source.contains("if (request != null && request.inputBatch != null && !request.inputBatch.isEmpty())"));
        assertTrue(source.contains("Multi.createFrom().iterable(request.inputBatch)"));
        assertTrue(source.contains("else if (request != null && request.input != null)"));
        assertTrue(source.contains("Multi.createFrom().item(request.input)"));
        assertTrue(source.contains("Multi.createFrom().empty()"));
    }

    @Test
    void rendersRunAsyncHandlerWithUnaryInputHandling() throws IOException {
        renderAndReadSource(buildBinding());
        Path runAsyncPath = tempDir.resolve("com/example/orchestrator/service/PipelineRunAsyncFunctionHandler.java");
        String source = Files.readString(runAsyncPath);

        assertTrue(source.contains("if (request != null && request.input != null)"));
        assertTrue(source.contains("executionInput = request.input"));
        assertTrue(source.contains("else if (request != null && request.inputBatch != null && !request.inputBatch.isEmpty())"));
        assertTrue(source.contains("request.inputBatch.get(0)"));
    }

    @Test
    void rendersRunAsyncHandlerWithIdempotencyKey() throws IOException {
        renderAndReadSource(buildBinding());
        Path runAsyncPath = tempDir.resolve("com/example/orchestrator/service/PipelineRunAsyncFunctionHandler.java");
        String source = Files.readString(runAsyncPath);

        assertTrue(source.contains("String tenantId = request == null ? null : request.tenantId"));
        assertTrue(source.contains("String idempotencyKey = request == null ? null : request.idempotencyKey"));
        assertTrue(source.contains("pipelineExecutionService.executePipelineAsync(executionInput, tenantId, idempotencyKey"));
    }

    @Test
    void rendersAllAsyncHandlersWithGeneratedRoleAnnotation() throws IOException {
        renderAndReadSource(buildBinding());

        Path runAsyncPath = tempDir.resolve("com/example/orchestrator/service/PipelineRunAsyncFunctionHandler.java");
        Path statusPath = tempDir.resolve("com/example/orchestrator/service/PipelineExecutionStatusFunctionHandler.java");
        Path resultPath = tempDir.resolve("com/example/orchestrator/service/PipelineExecutionResultFunctionHandler.java");

        String runAsyncSource = Files.readString(runAsyncPath);
        String statusSource = Files.readString(statusPath);
        String resultSource = Files.readString(resultPath);

        assertTrue(runAsyncSource.contains("@GeneratedRole(GeneratedRole.Role.REST_SERVER)"));
        assertTrue(statusSource.contains("@GeneratedRole(GeneratedRole.Role.REST_SERVER)"));
        assertTrue(resultSource.contains("@GeneratedRole(GeneratedRole.Role.REST_SERVER)"));
    }

    @Test
    void rendersHandlerWithNullContextHandling() throws IOException {
        String source = renderAndReadSource(buildBinding());

        assertTrue(source.contains("context != null ? context.getAwsRequestId() : \"unknown-request\""));
        assertTrue(source.contains("context != null ? context.getFunctionName() : \"PipelineRunFunctionHandler\""));
        assertTrue(source.contains("context != null && context.getLogStreamName() != null"));
    }

    @Test
    void rendersInvokeModeRoutingAdapter() throws IOException {
        String source = renderAndReadSource(buildBinding());

        assertTrue(source.contains("FunctionInvokeAdapter<InputTypeDto, OutputTypeDto> invokeLocal"));
        assertTrue(source.contains("FunctionInvokeAdapter<InputTypeDto, OutputTypeDto> invokeRemote = new HttpRemoteFunctionInvokeAdapter<>()"));
        assertTrue(source.contains("FunctionInvokeAdapter<InputTypeDto, OutputTypeDto> invoke = new InvocationModeRoutingFunctionInvokeAdapter<>(invokeLocal, invokeRemote)"));
    }

    @Test
    void rendersStreamingOutputWithListResult() throws IOException {
        String source = renderAndReadSource(buildStreamingBinding(false, true));

        assertTrue(source.contains("implements RequestHandler<InputTypeDto, List<OutputTypeDto>>"));
        assertTrue(source.contains("FunctionSinkAdapter<OutputTypeDto, List<OutputTypeDto>> sink = new CollectListFunctionSinkAdapter<>()"));
    }

    @Test
    void rendersStreamingInputWithMultiSourceAdapter() throws IOException {
        String source = renderAndReadSource(buildStreamingBinding(true, false));

        assertTrue(source.contains("implements RequestHandler<Multi<InputTypeDto>, OutputTypeDto>"));
        assertTrue(source.contains("new MultiFunctionSourceAdapter<>"));
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

    @Test
    void handlesEmptyInputBatchForUnaryHandlers() throws IOException {
        renderAndReadSource(buildBinding());

        Path runAsyncPath = tempDir.resolve("com/example/orchestrator/service/PipelineRunAsyncFunctionHandler.java");
        String runAsyncSource = java.nio.file.Files.readString(runAsyncPath);

        assertTrue(runAsyncSource.contains("request.inputBatch.isEmpty()"));
        assertTrue(runAsyncSource.contains("executionInput = null"));
    }

    @Test
    void handlesMultipleInputBatchItemsForStreamingInputHandlers() throws IOException {
        renderAndReadSource(buildStreamingBinding(true, false));

        Path runAsyncPath = tempDir.resolve("com/example/orchestrator/service/PipelineRunAsyncFunctionHandler.java");
        String runAsyncSource = java.nio.file.Files.readString(runAsyncPath);

        assertTrue(runAsyncSource.contains("request.inputBatch != null && !request.inputBatch.isEmpty()"));
        assertTrue(runAsyncSource.contains("Multi.createFrom().iterable(request.inputBatch)"));
    }

    @Test
    void rejectsMultipleInputBatchItemsForUnaryHandlers() throws IOException {
        renderAndReadSource(buildBinding());

        Path runAsyncPath = tempDir.resolve("com/example/orchestrator/service/PipelineRunAsyncFunctionHandler.java");
        String runAsyncSource = java.nio.file.Files.readString(runAsyncPath);

        assertTrue(runAsyncSource.contains("RunAsync unary handlers accept at most one item in inputBatch"));
        assertTrue(runAsyncSource.contains("IllegalArgumentException"));
    }

    @Test
    void executionLookupRequestRequiresExecutionId() throws IOException {
        renderAndReadSource(buildBinding());

        Path statusPath = tempDir.resolve("com/example/orchestrator/service/PipelineExecutionStatusFunctionHandler.java");
        String statusSource = java.nio.file.Files.readString(statusPath);

        assertTrue(statusSource.contains("request.executionId == null || request.executionId.isBlank()"));
        assertTrue(statusSource.contains("executionId is required"));

        Path resultPath = tempDir.resolve("com/example/orchestrator/service/PipelineExecutionResultFunctionHandler.java");
        String resultSource = java.nio.file.Files.readString(resultPath);

        assertTrue(resultSource.contains("request.executionId == null || request.executionId.isBlank()"));
        assertTrue(resultSource.contains("executionId is required"));
    }

    @Test
    void rendersTransportContextWithLambdaAttributes() throws IOException {
        String source = renderAndReadSource(buildBinding());

        assertTrue(source.contains("FunctionTransportContext.of"));
        assertTrue(source.contains("context.getAwsRequestId()"));
        assertTrue(source.contains("context.getFunctionName()"));
        assertTrue(source.contains("context.getLogStreamName()"));
        assertTrue(source.contains("ATTR_CORRELATION_ID"));
        assertTrue(source.contains("ATTR_EXECUTION_ID"));
        assertTrue(source.contains("ATTR_RETRY_ATTEMPT"));
        assertTrue(source.contains("ATTR_DISPATCH_TS_EPOCH_MS"));
    }

    @Test
    void handlesNullLambdaContext() throws IOException {
        String source = renderAndReadSource(buildBinding());

        assertTrue(source.contains("context != null ? context.getAwsRequestId() : \"unknown-request\""));
        assertTrue(source.contains("context != null ? context.getFunctionName() : \"PipelineRunFunctionHandler\""));
    }

    @Test
    void generatesRequestDtoClasses() throws IOException {
        renderAndReadSource(buildBinding());

        Path runAsyncRequestPath = tempDir.resolve("com/example/orchestrator/service/PipelineRunAsyncRequest.java");
        Path lookupRequestPath = tempDir.resolve("com/example/orchestrator/service/PipelineExecutionLookupRequest.java");

        String runAsyncRequest = java.nio.file.Files.readString(runAsyncRequestPath);
        String lookupRequest = java.nio.file.Files.readString(lookupRequestPath);

        assertTrue(runAsyncRequest.contains("public class PipelineRunAsyncRequest"));
        assertTrue(runAsyncRequest.contains("public InputTypeDto input"));
        assertTrue(runAsyncRequest.contains("public List<InputTypeDto> inputBatch"));
        assertTrue(runAsyncRequest.contains("public String tenantId"));
        assertTrue(runAsyncRequest.contains("public String idempotencyKey"));

        assertTrue(lookupRequest.contains("public class PipelineExecutionLookupRequest"));
        assertTrue(lookupRequest.contains("public String tenantId"));
        assertTrue(lookupRequest.contains("public String executionId"));
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