package org.pipelineframework.processor.renderer;

import java.io.IOException;
import java.util.List;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.OrchestratorBinding;

/**
 * Generates a native AWS Lambda RequestHandler for orchestrator unary execution.
 *
 * <p>The handler delegates to the generated orchestrator REST resource so the runtime behavior stays
 * consistent while avoiding API-Gateway HTTP bridge requirements for invocation.</p>
 */
public class OrchestratorFunctionHandlerRenderer implements PipelineRenderer<OrchestratorBinding> {

    public static final String HANDLER_CLASS = "PipelineRunFunctionHandler";
    public static final String RUN_ASYNC_HANDLER_CLASS = "PipelineRunAsyncFunctionHandler";
    public static final String STATUS_HANDLER_CLASS = "PipelineExecutionStatusFunctionHandler";
    public static final String RESULT_HANDLER_CLASS = "PipelineExecutionResultFunctionHandler";
    private static final String RUN_ASYNC_REQUEST_CLASS = "PipelineRunAsyncRequest";
    private static final String EXECUTION_LOOKUP_REQUEST_CLASS = "PipelineExecutionLookupRequest";
    private static final String API_VERSION = "v1";
    private static final String ORCHESTRATOR_PREFIX = "orchestrator.";
    private static final String UNKNOWN_REQUEST = "unknown-request";
    private static final String INGRESS = "ingress";
    private static final String RESOURCE_CLASS = "PipelineRunResource";
    private static final ClassName APPLICATION_SCOPED =
        ClassName.get("jakarta.enterprise.context", "ApplicationScoped");
    private static final ClassName INJECT =
        ClassName.get("jakarta.inject", "Inject");
    private static final ClassName NAMED =
        ClassName.get("jakarta.inject", "Named");
    private static final ClassName MULTI =
        ClassName.get("io.smallrye.mutiny", "Multi");
    private static final ClassName LAMBDA_CONTEXT =
        ClassName.get("com.amazonaws.services.lambda.runtime", "Context");
    private static final ClassName REQUEST_HANDLER =
        ClassName.get("com.amazonaws.services.lambda.runtime", "RequestHandler");
    private static final ClassName GENERATED_ROLE =
        ClassName.get("org.pipelineframework.annotation", "GeneratedRole");
    private static final ClassName ROLE_ENUM =
        ClassName.get("org.pipelineframework.annotation", "GeneratedRole", "Role");
    private static final ClassName FUNCTION_TRANSPORT_CONTEXT =
        ClassName.get("org.pipelineframework.transport.function", "FunctionTransportContext");
    private static final ClassName FUNCTION_TRANSPORT_BRIDGE =
        ClassName.get("org.pipelineframework.transport.function", "FunctionTransportBridge");
    private static final ClassName FUNCTION_SOURCE_ADAPTER =
        ClassName.get("org.pipelineframework.transport.function", "FunctionSourceAdapter");
    private static final ClassName FUNCTION_INVOKE_ADAPTER =
        ClassName.get("org.pipelineframework.transport.function", "FunctionInvokeAdapter");
    private static final ClassName FUNCTION_SINK_ADAPTER =
        ClassName.get("org.pipelineframework.transport.function", "FunctionSinkAdapter");
    private static final ClassName DEFAULT_UNARY_SOURCE_ADAPTER =
        ClassName.get("org.pipelineframework.transport.function", "DefaultUnaryFunctionSourceAdapter");
    private static final ClassName MULTI_SOURCE_ADAPTER =
        ClassName.get("org.pipelineframework.transport.function", "MultiFunctionSourceAdapter");
    private static final ClassName LOCAL_UNARY_INVOKE_ADAPTER =
        ClassName.get("org.pipelineframework.transport.function", "LocalUnaryFunctionInvokeAdapter");
    private static final ClassName LOCAL_ONE_TO_MANY_INVOKE_ADAPTER =
        ClassName.get("org.pipelineframework.transport.function", "LocalOneToManyFunctionInvokeAdapter");
    private static final ClassName LOCAL_MANY_TO_ONE_INVOKE_ADAPTER =
        ClassName.get("org.pipelineframework.transport.function", "LocalManyToOneFunctionInvokeAdapter");
    private static final ClassName LOCAL_MANY_TO_MANY_INVOKE_ADAPTER =
        ClassName.get("org.pipelineframework.transport.function", "LocalManyToManyFunctionInvokeAdapter");
    private static final ClassName INVOCATION_MODE_ROUTING_INVOKE_ADAPTER =
        ClassName.get("org.pipelineframework.transport.function", "InvocationModeRoutingFunctionInvokeAdapter");
    private static final ClassName HTTP_REMOTE_INVOKE_ADAPTER =
        ClassName.get("org.pipelineframework.transport.function", "HttpRemoteFunctionInvokeAdapter");
    private static final ClassName DEFAULT_UNARY_SINK_ADAPTER =
        ClassName.get("org.pipelineframework.transport.function", "DefaultUnaryFunctionSinkAdapter");
    private static final ClassName COLLECT_LIST_SINK_ADAPTER =
        ClassName.get("org.pipelineframework.transport.function", "CollectListFunctionSinkAdapter");
    private static final ClassName UNARY_FUNCTION_TRANSPORT_BRIDGE =
        ClassName.get("org.pipelineframework.transport.function", "UnaryFunctionTransportBridge");

    /**
     * Return the fully-qualified class name of the orchestrator function handler for the given base package.
     *
     * @param basePackage the base Java package used as the root for the generated handler
     * @return the fully-qualified class name of the generated orchestrator handler
     */
    public static String handlerFqcn(String basePackage) {
        return basePackage + ".orchestrator.service." + HANDLER_CLASS;
    }

    /**
     * Resolve the fully-qualified class name of the generated async run handler for the given base package.
     *
     * @param basePackage the base Java package where generated orchestrator classes are placed
     * @return the fully-qualified class name of the async run handler
     */
    public static String runAsyncHandlerFqcn(String basePackage) {
        return basePackage + ".orchestrator.service." + RUN_ASYNC_HANDLER_CLASS;
    }

    /**
     * Get the fully-qualified class name of the execution status handler for the given base package.
     *
     * @param basePackage the base Java package used for generated classes
     * @return the fully-qualified class name of the generated status handler
     */
    public static String statusHandlerFqcn(String basePackage) {
        return basePackage + ".orchestrator.service." + STATUS_HANDLER_CLASS;
    }

    /**
     * Builds the fully-qualified class name of the execution result handler for the given base package.
     *
     * @param basePackage the base package to use when composing the FQCN
     * @return the fully-qualified class name of the result handler
     */
    public static String resultHandlerFqcn(String basePackage) {
        return basePackage + ".orchestrator.service." + RESULT_HANDLER_CLASS;
    }

    /**
     * Creates a new OrchestratorFunctionHandlerRenderer.
     */
    public OrchestratorFunctionHandlerRenderer() {
    }

    @Override
    public GenerationTarget target() {
        return GenerationTarget.REST_RESOURCE;
    }

    /**
     * Generate and write a Lambda RequestHandler Java class that delegates orchestrator execution
     * to the generated REST resource according to the provided binding.
     *
     * The produced handler class adapts to the binding's input/output streaming configuration,
     * derives DTO and resource types from the binding's base package and type names, and selects
     * appropriate source/invoke/sink/bridge adapters before writing the resulting Java file to the
     * generation context's output directory.
     *
     * @param binding descriptor of the orchestrator (type names, base package, and streaming flags)
     * @param ctx     generation context providing the output directory and related I/O helpers
     * @throws IOException if writing the generated Java file fails
     */
    @Override
    public void render(OrchestratorBinding binding, GenerationContext ctx) throws IOException {
        boolean streamingInput = binding.inputStreaming();
        boolean streamingOutput = binding.outputStreaming();

        ClassName inputDto = ClassName.get(binding.basePackage() + ".common.dto", binding.inputTypeName() + "Dto");
        ClassName outputDto = ClassName.get(binding.basePackage() + ".common.dto", binding.outputTypeName() + "Dto");
        TypeName inputEventType = streamingInput
            ? ParameterizedTypeName.get(MULTI, inputDto)
            : inputDto;
        TypeName handlerOutputType = streamingOutput
            ? ParameterizedTypeName.get(ClassName.get(List.class), outputDto)
            : outputDto;
        ClassName resourceType = ClassName.get(binding.basePackage() + ".orchestrator.service", RESOURCE_CLASS);

        TypeSpec.Builder handler = TypeSpec.classBuilder(HANDLER_CLASS)
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(APPLICATION_SCOPED)
            .addAnnotation(AnnotationSpec.builder(NAMED)
                .addMember("value", "$S", HANDLER_CLASS)
                .build())
            .addAnnotation(AnnotationSpec.builder(GENERATED_ROLE)
                .addMember("value", "$T.$L", ROLE_ENUM, "REST_SERVER")
                .build())
            .addSuperinterface(ParameterizedTypeName.get(REQUEST_HANDLER, inputEventType, handlerOutputType))
            .addField(FieldSpec.builder(resourceType, "resource", Modifier.PRIVATE)
                .addAnnotation(INJECT)
                .build());

        String localInvokeDelegate = localInvokeDelegate(streamingInput, streamingOutput);

        MethodSpec handleRequest = MethodSpec.methodBuilder("handleRequest")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(handlerOutputType)
            .addParameter(inputEventType, "input")
            .addParameter(LAMBDA_CONTEXT, "context")
            .beginControlFlow("try")
            .addStatement("$T transportContext = $T.of("
                    + "context != null ? context.getAwsRequestId() : $S, "
                    + "context != null ? context.getFunctionName() : $S, "
                    + "$S, "
                    + "$T.of("
                    + "$T.ATTR_TRANSPORT_PROTOCOL, $T.getProperty($S, $S), "
                    + "$T.ATTR_CORRELATION_ID, context != null ? context.getAwsRequestId() : $S, "
                    + "$T.ATTR_EXECUTION_ID, (context != null && context.getLogStreamName() != null "
                    + "&& !context.getLogStreamName().isBlank()) ? context.getLogStreamName() : $T.randomUUID().toString(), "
                    + "$T.ATTR_RETRY_ATTEMPT, $T.getProperty($S, $S), "
                    + "$T.ATTR_DISPATCH_TS_EPOCH_MS, $T.toString($T.currentTimeMillis())))",
                FUNCTION_TRANSPORT_CONTEXT, FUNCTION_TRANSPORT_CONTEXT,
                UNKNOWN_REQUEST, HANDLER_CLASS, INGRESS,
                ClassName.get("java.util", "Map"),
                FUNCTION_TRANSPORT_CONTEXT, ClassName.get(System.class), "tpf.transport.protocol", "lambda",
                FUNCTION_TRANSPORT_CONTEXT, UNKNOWN_REQUEST,
                FUNCTION_TRANSPORT_CONTEXT, ClassName.get("java.util", "UUID"),
                FUNCTION_TRANSPORT_CONTEXT, ClassName.get(System.class), "tpf.transport.retry-attempt", "0",
                FUNCTION_TRANSPORT_CONTEXT, ClassName.get(Long.class), ClassName.get(System.class))
            .addStatement("$T<$T, $T> source = new $T<>($S, $S)",
                FUNCTION_SOURCE_ADAPTER, inputEventType, inputDto,
                selectSourceAdapter(streamingInput, DEFAULT_UNARY_SOURCE_ADAPTER, MULTI_SOURCE_ADAPTER),
                ORCHESTRATOR_PREFIX + binding.inputTypeName(), API_VERSION)
            .addStatement("$T<$T, $T> invokeLocal = new $T<$T, $T>($L, $S, $S)",
                FUNCTION_INVOKE_ADAPTER, inputDto, outputDto,
                selectInvokeAdapter(streamingInput, streamingOutput,
                    LOCAL_UNARY_INVOKE_ADAPTER,
                    LOCAL_ONE_TO_MANY_INVOKE_ADAPTER,
                    LOCAL_MANY_TO_ONE_INVOKE_ADAPTER,
                    LOCAL_MANY_TO_MANY_INVOKE_ADAPTER),
                inputDto, outputDto,
                localInvokeDelegate,
                ORCHESTRATOR_PREFIX + binding.outputTypeName(), API_VERSION)
            .addStatement("$T<$T, $T> invokeRemote = new $T<>()",
                FUNCTION_INVOKE_ADAPTER, inputDto, outputDto,
                HTTP_REMOTE_INVOKE_ADAPTER)
            .addStatement("$T<$T, $T> invoke = new $T<>(invokeLocal, invokeRemote)",
                FUNCTION_INVOKE_ADAPTER, inputDto, outputDto,
                INVOCATION_MODE_ROUTING_INVOKE_ADAPTER)
            .addStatement("$T<$T, $T> sink = new $T<>()",
                FUNCTION_SINK_ADAPTER, outputDto, handlerOutputType,
                selectSinkAdapter(streamingOutput, DEFAULT_UNARY_SINK_ADAPTER, COLLECT_LIST_SINK_ADAPTER))
            .addStatement("return $T.$L(input, transportContext, source, invoke, sink)",
                bridgeClass(streamingInput, streamingOutput, UNARY_FUNCTION_TRANSPORT_BRIDGE, FUNCTION_TRANSPORT_BRIDGE),
                bridgeMethodName(streamingInput, streamingOutput))
            .nextControlFlow("catch ($T e)", RuntimeException.class)
            .addStatement(
                "throw new $T(\"Failed handleRequest -> resource.run for input DTO\", e)",
                RuntimeException.class)
            .endControlFlow()
            .build();

        handler.addMethod(handleRequest);

        JavaFile.builder(binding.basePackage() + ".orchestrator.service", handler.build())
            .build()
            .writeTo(ctx.outputDir());

        renderAsyncHandlers(binding, ctx, inputDto, outputDto, streamingInput, streamingOutput);
    }

    /**
     * Generates auxiliary asynchronous orchestrator handlers and request DTOs (run-async, status, result)
     * and writes them to the orchestrator.service package.
     *
     * This creates:
     * - RunAsyncRequest and ExecutionLookupRequest DTOs.
     * - RunAsyncHandler, StatusHandler, and ResultHandler AWS Lambda RequestHandler implementations
     *   that delegate to PipelineExecutionService and respect streaming input/output configuration.
     *
     * @param binding the orchestrator binding containing base package information used for generated types
     * @param ctx the generation context providing the output directory for written Java files
     * @param inputDto the ClassName reference for the pipeline input DTO
     * @param outputDto the ClassName reference for the pipeline output DTO
     * @param streamingInput true when the pipeline accepts a streaming (multi) input
     * @param streamingOutput true when the pipeline produces a streaming (multi) output
     * @throws IOException if writing the generated Java files to the output directory fails
     */
    private void renderAsyncHandlers(
        OrchestratorBinding binding,
        GenerationContext ctx,
        ClassName inputDto,
        ClassName outputDto,
        boolean streamingInput,
        boolean streamingOutput
    ) throws IOException {
        ClassName executionService = ClassName.get("org.pipelineframework", "PipelineExecutionService");
        ClassName runAsyncAcceptedDto = ClassName.get("org.pipelineframework.orchestrator.dto", "RunAsyncAcceptedDto");
        ClassName executionStatusDto = ClassName.get("org.pipelineframework.orchestrator.dto", "ExecutionStatusDto");
        ClassName list = ClassName.get(List.class);
        TypeName runAsyncRequestType =
            ClassName.get(binding.basePackage() + ".orchestrator.service", RUN_ASYNC_REQUEST_CLASS);
        TypeName executionLookupRequestType =
            ClassName.get(binding.basePackage() + ".orchestrator.service", EXECUTION_LOOKUP_REQUEST_CLASS);
        TypeName asyncResultType = streamingOutput
            ? ParameterizedTypeName.get(list, outputDto)
            : outputDto;

        TypeSpec runAsyncRequest = TypeSpec.classBuilder(RUN_ASYNC_REQUEST_CLASS)
            .addModifiers(Modifier.PUBLIC)
            .addField(FieldSpec.builder(inputDto, "input", Modifier.PUBLIC).build())
            .addField(FieldSpec.builder(ParameterizedTypeName.get(list, inputDto), "inputBatch", Modifier.PUBLIC).build())
            .addField(FieldSpec.builder(String.class, "tenantId", Modifier.PUBLIC).build())
            .addField(FieldSpec.builder(String.class, "idempotencyKey", Modifier.PUBLIC).build())
            .build();
        TypeSpec executionLookupRequest = TypeSpec.classBuilder(EXECUTION_LOOKUP_REQUEST_CLASS)
            .addModifiers(Modifier.PUBLIC)
            .addField(FieldSpec.builder(String.class, "tenantId", Modifier.PUBLIC).build())
            .addField(FieldSpec.builder(String.class, "executionId", Modifier.PUBLIC).build())
            .build();

        MethodSpec.Builder runAsyncHandleRequestBuilder = MethodSpec.methodBuilder("handleRequest")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(runAsyncAcceptedDto)
            .addParameter(runAsyncRequestType, "request")
            .addParameter(LAMBDA_CONTEXT, "context")
            .addStatement("$T executionInput", Object.class);

        if (streamingInput) {
            runAsyncHandleRequestBuilder
                .beginControlFlow("if (request != null && request.inputBatch != null && !request.inputBatch.isEmpty())")
                .addStatement("executionInput = $T.createFrom().iterable(request.inputBatch)", MULTI)
                .nextControlFlow("else if (request != null && request.input != null)")
                .addStatement("executionInput = $T.createFrom().item(request.input)", MULTI)
                .nextControlFlow("else")
                .addStatement("executionInput = $T.createFrom().empty()", MULTI)
                .endControlFlow();
        } else {
            runAsyncHandleRequestBuilder
                .beginControlFlow("if (request != null && request.input != null)")
                .addStatement("executionInput = request.input")
                .nextControlFlow("else if (request != null && request.inputBatch != null && !request.inputBatch.isEmpty())")
                .addStatement("executionInput = request.inputBatch.get(0)")
                .nextControlFlow("else")
                .addStatement("executionInput = null")
                .endControlFlow();
        }

        MethodSpec runAsyncHandleRequest = runAsyncHandleRequestBuilder
            .addStatement("String tenantId = request == null ? null : request.tenantId")
            .addStatement("String idempotencyKey = request == null ? null : request.idempotencyKey")
            .addStatement(
                "return pipelineExecutionService.executePipelineAsync(executionInput, tenantId, idempotencyKey, $L)"
                    + ".await().indefinitely()",
                streamingOutput)
            .build();

        TypeSpec runAsyncHandler = TypeSpec.classBuilder(RUN_ASYNC_HANDLER_CLASS)
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(APPLICATION_SCOPED)
            .addAnnotation(AnnotationSpec.builder(NAMED)
                .addMember("value", "$S", RUN_ASYNC_HANDLER_CLASS)
                .build())
            .addAnnotation(AnnotationSpec.builder(GENERATED_ROLE)
                .addMember("value", "$T.$L", ROLE_ENUM, "REST_SERVER")
                .build())
            .addSuperinterface(ParameterizedTypeName.get(REQUEST_HANDLER, runAsyncRequestType, runAsyncAcceptedDto))
            .addField(FieldSpec.builder(executionService, "pipelineExecutionService", Modifier.PRIVATE)
                .addAnnotation(INJECT)
                .build())
            .addMethod(runAsyncHandleRequest)
            .build();

        MethodSpec statusHandleRequest = MethodSpec.methodBuilder("handleRequest")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(executionStatusDto)
            .addParameter(executionLookupRequestType, "request")
            .addParameter(LAMBDA_CONTEXT, "context")
            .beginControlFlow("if (request == null || request.executionId == null || request.executionId.isBlank())")
            .addStatement("throw new $T($S)", IllegalArgumentException.class, "executionId is required")
            .endControlFlow()
            .addStatement("return pipelineExecutionService.getExecutionStatus(request.tenantId, request.executionId).await().indefinitely()")
            .build();

        TypeSpec statusHandler = TypeSpec.classBuilder(STATUS_HANDLER_CLASS)
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(APPLICATION_SCOPED)
            .addAnnotation(AnnotationSpec.builder(NAMED)
                .addMember("value", "$S", STATUS_HANDLER_CLASS)
                .build())
            .addAnnotation(AnnotationSpec.builder(GENERATED_ROLE)
                .addMember("value", "$T.$L", ROLE_ENUM, "REST_SERVER")
                .build())
            .addSuperinterface(ParameterizedTypeName.get(REQUEST_HANDLER, executionLookupRequestType, executionStatusDto))
            .addField(FieldSpec.builder(executionService, "pipelineExecutionService", Modifier.PRIVATE)
                .addAnnotation(INJECT)
                .build())
            .addMethod(statusHandleRequest)
            .build();

        MethodSpec.Builder resultHandleRequestBuilder = MethodSpec.methodBuilder("handleRequest")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(asyncResultType)
            .addParameter(executionLookupRequestType, "request")
            .addParameter(LAMBDA_CONTEXT, "context")
            .beginControlFlow("if (request == null || request.executionId == null || request.executionId.isBlank())")
            .addStatement("throw new $T($S)", IllegalArgumentException.class, "executionId is required")
            .endControlFlow()
            .addStatement(
                "return pipelineExecutionService.<$T>getExecutionResult(request.tenantId, request.executionId, $T.class, $L)"
                    + ".await().indefinitely()",
                asyncResultType,
                outputDto,
                streamingOutput);
        MethodSpec resultHandleRequest = resultHandleRequestBuilder.build();

        TypeSpec resultHandler = TypeSpec.classBuilder(RESULT_HANDLER_CLASS)
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(APPLICATION_SCOPED)
            .addAnnotation(AnnotationSpec.builder(NAMED)
                .addMember("value", "$S", RESULT_HANDLER_CLASS)
                .build())
            .addAnnotation(AnnotationSpec.builder(GENERATED_ROLE)
                .addMember("value", "$T.$L", ROLE_ENUM, "REST_SERVER")
                .build())
            .addSuperinterface(ParameterizedTypeName.get(REQUEST_HANDLER, executionLookupRequestType, asyncResultType))
            .addField(FieldSpec.builder(executionService, "pipelineExecutionService", Modifier.PRIVATE)
                .addAnnotation(INJECT)
                .build())
            .addMethod(resultHandleRequest)
            .build();

        JavaFile.builder(binding.basePackage() + ".orchestrator.service", runAsyncRequest)
            .build()
            .writeTo(ctx.outputDir());
        JavaFile.builder(binding.basePackage() + ".orchestrator.service", executionLookupRequest)
            .build()
            .writeTo(ctx.outputDir());
        JavaFile.builder(binding.basePackage() + ".orchestrator.service", runAsyncHandler)
            .build()
            .writeTo(ctx.outputDir());
        JavaFile.builder(binding.basePackage() + ".orchestrator.service", statusHandler)
            .build()
            .writeTo(ctx.outputDir());
        JavaFile.builder(binding.basePackage() + ".orchestrator.service", resultHandler)
            .build()
            .writeTo(ctx.outputDir());
    }

    /**
     * Selects the appropriate local invoke adapter class for the given input/output streaming configuration.
     *
     * @param streamingInput           true if the pipeline input is a stream of records, false if unary
     * @param streamingOutput          true if the pipeline output is a stream of records, false if unary
     * @param localInvokeAdapter       adapter to use for unary->unary invocation
     * @param localOneToManyInvokeAdapter adapter to use when input is unary and output is streaming
     * @param localManyToOneInvokeAdapter adapter to use when input is streaming and output is unary
     * @param localManyToManyInvokeAdapter adapter to use when both input and output are streaming
     * @return                         the ClassName of the adapter appropriate for the specified streaming mode
     */
    private static ClassName selectInvokeAdapter(
            boolean streamingInput,
            boolean streamingOutput,
            ClassName localInvokeAdapter,
            ClassName localOneToManyInvokeAdapter,
            ClassName localManyToOneInvokeAdapter,
            ClassName localManyToManyInvokeAdapter) {
        if (!streamingInput && !streamingOutput) {
            return localInvokeAdapter;
        }
        if (!streamingInput) {
            return localOneToManyInvokeAdapter;
        }
        if (!streamingOutput) {
            return localManyToOneInvokeAdapter;
        }
        return localManyToManyInvokeAdapter;
    }

    private static ClassName selectSourceAdapter(
            boolean streamingInput,
            ClassName defaultSourceAdapter,
            ClassName multiSourceAdapter) {
        return streamingInput ? multiSourceAdapter : defaultSourceAdapter;
    }

    private static ClassName selectSinkAdapter(
            boolean streamingOutput,
            ClassName defaultSinkAdapter,
            ClassName collectListSinkAdapter) {
        return streamingOutput ? collectListSinkAdapter : defaultSinkAdapter;
    }

    private static ClassName bridgeClass(
            boolean streamingInput,
            boolean streamingOutput,
            ClassName unaryBridge,
            ClassName functionTransportBridge) {
        return (!streamingInput && !streamingOutput) ? unaryBridge : functionTransportBridge;
    }

    private static String bridgeMethodName(boolean streamingInput, boolean streamingOutput) {
        if (!streamingInput && !streamingOutput) {
            return "invoke";
        }
        if (!streamingInput) {
            return "invokeOneToMany";
        }
        if (!streamingOutput) {
            return "invokeManyToOne";
        }
        return "invokeManyToMany";
    }

    private static String localInvokeDelegate(boolean streamingInput, boolean streamingOutput) {
        if (streamingInput && !streamingOutput) {
            return "inputStream -> inputStream.collect().asList().onItem().transformToUni(resource::run)";
        }
        return "resource::run";
    }
}
