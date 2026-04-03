/*
 * Copyright (c) 2023-2026 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
 * Abstract base class for generating cloud function handler wrappers for orchestrator execution.
 *
 * <p>Concrete implementations generate handlers for specific cloud providers:
 * AWS Lambda, Azure Functions, or Google Cloud Functions.</p>
 */
public abstract class AbstractOrchestratorFunctionHandlerRenderer implements PipelineRenderer<OrchestratorBinding> {

    public static final String HANDLER_CLASS = "PipelineRunFunctionHandler";
    public static final String RUN_ASYNC_HANDLER_CLASS = "PipelineRunAsyncFunctionHandler";
    public static final String STATUS_HANDLER_CLASS = "PipelineExecutionStatusFunctionHandler";
    public static final String RESULT_HANDLER_CLASS = "PipelineExecutionResultFunctionHandler";
    protected static final String RUN_ASYNC_REQUEST_CLASS = "PipelineRunAsyncRequest";
    protected static final String EXECUTION_LOOKUP_REQUEST_CLASS = "PipelineExecutionLookupRequest";
    protected static final String API_VERSION = "v1";
    protected static final String ORCHESTRATOR_PREFIX = "orchestrator.";
    protected static final String UNKNOWN_REQUEST = "unknown-request";
    protected static final String INVOKE_STEP = "invoke-step";
    protected static final String INGRESS = "ingress";
    protected static final String RESOURCE_CLASS = "PipelineRunResource";

    protected static final ClassName APPLICATION_SCOPED = ClassName.get("jakarta.enterprise.context", "ApplicationScoped");
    protected static final ClassName INJECT = ClassName.get("jakarta.inject", "Inject");
    protected static final ClassName NAMED = ClassName.get("jakarta.inject", "Named");
    protected static final ClassName MULTI = ClassName.get("io.smallrye.mutiny", "Multi");
    protected static final ClassName GENERATED_ROLE = ClassName.get("org.pipelineframework.annotation", "GeneratedRole");
    protected static final ClassName ROLE_ENUM = ClassName.get("org.pipelineframework.annotation", "GeneratedRole", "Role");
    protected static final ClassName FUNCTION_TRANSPORT_CONTEXT = ClassName.get("org.pipelineframework.transport.function", "FunctionTransportContext");
    protected static final ClassName FUNCTION_TRANSPORT_BRIDGE = ClassName.get("org.pipelineframework.transport.function", "FunctionTransportBridge");
    protected static final ClassName FUNCTION_SOURCE_ADAPTER = ClassName.get("org.pipelineframework.transport.function", "FunctionSourceAdapter");
    protected static final ClassName FUNCTION_INVOKE_ADAPTER = ClassName.get("org.pipelineframework.transport.function", "FunctionInvokeAdapter");
    protected static final ClassName FUNCTION_SINK_ADAPTER = ClassName.get("org.pipelineframework.transport.function", "FunctionSinkAdapter");
    protected static final ClassName DEFAULT_UNARY_SOURCE_ADAPTER = ClassName.get("org.pipelineframework.transport.function", "DefaultUnaryFunctionSourceAdapter");
    protected static final ClassName MULTI_SOURCE_ADAPTER = ClassName.get("org.pipelineframework.transport.function", "MultiFunctionSourceAdapter");
    protected static final ClassName LOCAL_UNARY_INVOKE_ADAPTER = ClassName.get("org.pipelineframework.transport.function", "LocalUnaryFunctionInvokeAdapter");
    protected static final ClassName LOCAL_ONE_TO_MANY_INVOKE_ADAPTER = ClassName.get("org.pipelineframework.transport.function", "LocalOneToManyFunctionInvokeAdapter");
    protected static final ClassName LOCAL_MANY_TO_ONE_INVOKE_ADAPTER = ClassName.get("org.pipelineframework.transport.function", "LocalManyToOneFunctionInvokeAdapter");
    protected static final ClassName LOCAL_MANY_TO_MANY_INVOKE_ADAPTER = ClassName.get("org.pipelineframework.transport.function", "LocalManyToManyFunctionInvokeAdapter");
    protected static final ClassName INVOCATION_MODE_ROUTING_INVOKE_ADAPTER = ClassName.get("org.pipelineframework.transport.function", "InvocationModeRoutingFunctionInvokeAdapter");
    protected static final ClassName HTTP_REMOTE_INVOKE_ADAPTER = ClassName.get("org.pipelineframework.transport.function", "HttpRemoteFunctionInvokeAdapter");
    protected static final ClassName DEFAULT_UNARY_SINK_ADAPTER = ClassName.get("org.pipelineframework.transport.function", "DefaultUnaryFunctionSinkAdapter");
    protected static final ClassName COLLECT_LIST_SINK_ADAPTER = ClassName.get("org.pipelineframework.transport.function", "CollectListFunctionSinkAdapter");
    protected static final ClassName UNARY_FUNCTION_TRANSPORT_BRIDGE = ClassName.get("org.pipelineframework.transport.function", "UnaryFunctionTransportBridge");

    /**
 * Constructs a new AbstractOrchestratorFunctionHandlerRenderer for use by subclasses.
 */
protected AbstractOrchestratorFunctionHandlerRenderer() {}

    /**
     * Indicates the generation target for this renderer.
     *
     * @return `GenerationTarget.REST_RESOURCE` when generating REST resource handler classes.
     */
    @Override
    public GenerationTarget target() { return GenerationTarget.REST_RESOURCE; }

    /**
 * Identify the target cloud provider used for provider-specific rendering (for example, "aws", "azure", or "gcp").
 *
 * @return the cloud provider identifier string (e.g., "aws", "azure", "gcp")
 */
    protected abstract String getCloudProvider();

    /**
 * Provide the ClassName for the cloud provider's handler context type.
 *
 * @return the ClassName representing the handler context type used by generated handlers
 */
    protected abstract ClassName getContextClassName();

    /**
 * The handler interface that generated orchestrator handler classes must implement.
 *
 * @return the ClassName representing the handler interface to implement
 */
    protected abstract ClassName getHandlerInterfaceClassName();

    /**
 * Expression used to extract the request ID from the incoming input or handler context.
 *
 * @return a Java expression string that evaluates to the request ID, or null/blank if not available
 */
    protected abstract String getRequestIdExpression();

    /**
 * Expression used to obtain the function name from the incoming request or handler context.
 *
 * @return a string containing a Java expression that yields the function name when evaluated, or null/empty if not available
 */
    protected abstract String getFunctionNameExpression();

    /**
 * Provide a Java expression that extracts the execution ID from the incoming request or context.
 *
 * <p>If no execution ID can be extracted, the method may return {@code null} or an empty string;
 * the renderer will fall back to generating a random UUID when no expression is provided.
 *
 * @return a Java expression as a {@code String} that evaluates to the execution ID, or {@code null}
 *         / empty string if no execution ID extraction is available
 */
    protected abstract String getExecutionIdExpression();

    /**
     * Generates and writes the orchestrator REST handler class (PipelineRunFunctionHandler) and any provider-specific async handlers and DTOs based on the given binding.
     *
     * <p>Reads streaming input/output flags and base package from {@code binding}, computes DTO and transport types, builds a handler type implementing the provider-specific handler interface, writes the generated Java file to the generation context output directory, and delegates generation of async handlers to {@link #renderAsyncHandlers(OrchestratorBinding, GenerationContext, String, ClassName, ClassName, boolean, boolean)}.
     *
     * @param binding the orchestrator binding containing type names, base package, and streaming configuration
     * @param ctx the generation context that provides the output directory for generated files
     * @throws IOException if writing the generated Java files fails
     */
    @Override
    public void render(OrchestratorBinding binding, GenerationContext ctx) throws IOException {
        // Validate binding
        if (binding == null) {
            throw new IllegalArgumentException("OrchestratorBinding must not be null");
        }
        String basePackage = binding.basePackage();
        String inputTypeName = binding.inputTypeName();
        String outputTypeName = binding.outputTypeName();
        
        if (basePackage == null || basePackage.isBlank()) {
            throw new IllegalArgumentException("OrchestratorBinding.basePackage() must not be null or blank");
        }
        if (inputTypeName == null || inputTypeName.isBlank()) {
            throw new IllegalArgumentException("OrchestratorBinding.inputTypeName() must not be null or blank");
        }
        if (outputTypeName == null || outputTypeName.isBlank()) {
            throw new IllegalArgumentException("OrchestratorBinding.outputTypeName() must not be null or blank");
        }
        
        boolean streamingInput = binding.inputStreaming();
        boolean streamingOutput = binding.outputStreaming();

        ClassName inputDto = ClassName.get(basePackage + ".common.dto", binding.inputTypeName() + "Dto");
        ClassName outputDto = ClassName.get(basePackage + ".common.dto", binding.outputTypeName() + "Dto");
        TypeName inputEventType = streamingInput ? ParameterizedTypeName.get(MULTI, inputDto) : inputDto;
        TypeName handlerOutputType = streamingOutput ? ParameterizedTypeName.get(ClassName.get(List.class), outputDto) : outputDto;
        ClassName resourceType = ClassName.get(basePackage + ".orchestrator.service", RESOURCE_CLASS);

        TypeSpec.Builder handler = TypeSpec.classBuilder(HANDLER_CLASS)
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(APPLICATION_SCOPED)
            .addAnnotation(AnnotationSpec.builder(NAMED).addMember("value", "$S", HANDLER_CLASS).build())
            .addAnnotation(AnnotationSpec.builder(GENERATED_ROLE).addMember("value", "$T.$L", ROLE_ENUM, "REST_SERVER").build());
        
        // Only add superinterface if handler interface is defined (Azure Functions handlers are POJOs)
        ClassName handlerInterface = getHandlerInterfaceClassName();
        if (handlerInterface != null) {
            handler.addSuperinterface(ParameterizedTypeName.get(handlerInterface, inputEventType, handlerOutputType));
        }
        
        handler.addField(FieldSpec.builder(resourceType, "resource", Modifier.PRIVATE).addAnnotation(INJECT).build());

        String localInvokeDelegate = localInvokeDelegate(streamingInput, streamingOutput);
        MethodSpec handleRequest = buildHandlerMethod(basePackage, binding.inputTypeName(), binding.outputTypeName(), inputDto, outputDto, inputEventType, handlerOutputType, resourceType, localInvokeDelegate, streamingInput, streamingOutput);
        handler.addMethod(handleRequest);

        JavaFile.builder(basePackage + ".orchestrator.service", handler.build()).build().writeTo(ctx.outputDir());
        renderAsyncHandlers(binding, ctx, basePackage, inputDto, outputDto, streamingInput, streamingOutput);
    }

    /**
     * Builds the generated `handleRequest` method used by the orchestrator handler class.
     *
     * Constructs a MethodSpec that:
     * - declares the method signature (input event and provider-specific context),
     * - builds a transport context,
     * - selects and instantiates source, invoke (local/remote routed) and sink adapters based on streaming flags,
     * - bridges the adapters to the function transport bridge and returns the bridged result,
     * - wraps runtime failures with a RuntimeException.
     *
     * @param basePackage        base Java package used to resolve generated types
     * @param inputTypeName      simple name of the input DTO type
     * @param outputTypeName     simple name of the output DTO type
     * @param inputDto           ClassName of the input DTO
     * @param outputDto          ClassName of the output DTO
     * @param inputEventType     event type accepted by the handler (may be a streaming wrapper)
     * @param handlerOutputType  type returned by the handler (may be a collection wrapper for streaming output)
     * @param resourceType       ClassName of the orchestrator resource implementation injected into the handler
     * @param localInvokeDelegate expression or method reference used to invoke the local resource (may include stream-to-list collection)
     * @param streamingInput     true when input is a streaming/multi-item event (affects adapter selection)
     * @param streamingOutput    true when output is streaming/multi-item (affects adapter/bridge selection)
     * @return                   the Javadoc-parsable MethodSpec representing the generated `handleRequest` method
     */
    protected MethodSpec buildHandlerMethod(String basePackage, String inputTypeName, String outputTypeName, ClassName inputDto, ClassName outputDto, TypeName inputEventType, TypeName handlerOutputType, ClassName resourceType, String localInvokeDelegate, boolean streamingInput, boolean streamingOutput) {
        return MethodSpec.methodBuilder("handleRequest")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(handlerOutputType)
            .addParameter(inputEventType, "input")
            .addParameter(getContextClassName(), "context")
            .beginControlFlow("try")
            .addStatement("$T transportContext = $T.of("
                + getRequestIdExpression() + ", "
                + getFunctionNameExpression() + ", $S, $T.of("
                + "$T.ATTR_CORRELATION_ID, " + getRequestIdExpression() + ", "
                + "$T.ATTR_EXECUTION_ID, " + buildExecutionIdExpression() + ", "
                + "$T.ATTR_RETRY_ATTEMPT, $T.getProperty($S, $S), "
                + "$T.ATTR_DISPATCH_TS_EPOCH_MS, $T.toString($T.currentTimeMillis())))",
                FUNCTION_TRANSPORT_CONTEXT, FUNCTION_TRANSPORT_CONTEXT,
                UNKNOWN_REQUEST, UNKNOWN_REQUEST, INVOKE_STEP, ClassName.get("java.util", "Map"),
                FUNCTION_TRANSPORT_CONTEXT, UNKNOWN_REQUEST,
                FUNCTION_TRANSPORT_CONTEXT, ClassName.get("java.util", "UUID"),
                FUNCTION_TRANSPORT_CONTEXT, ClassName.get(System.class), "tpf.transport.retry-attempt", "0",
                FUNCTION_TRANSPORT_CONTEXT, ClassName.get(Long.class), ClassName.get(System.class))
            .addStatement("$T<$T, $T> source = new $T<>($S, $S)", FUNCTION_SOURCE_ADAPTER, inputEventType, inputDto, selectSourceAdapter(streamingInput, DEFAULT_UNARY_SOURCE_ADAPTER, MULTI_SOURCE_ADAPTER), ORCHESTRATOR_PREFIX + inputTypeName, API_VERSION)
            .addStatement("$T<$T, $T> invokeLocal = new $T<$T, $T>($L, $S, $S)", FUNCTION_INVOKE_ADAPTER, inputDto, outputDto, selectInvokeAdapter(streamingInput, streamingOutput, LOCAL_UNARY_INVOKE_ADAPTER, LOCAL_ONE_TO_MANY_INVOKE_ADAPTER, LOCAL_MANY_TO_ONE_INVOKE_ADAPTER, LOCAL_MANY_TO_MANY_INVOKE_ADAPTER), inputDto, outputDto, localInvokeDelegate, ORCHESTRATOR_PREFIX + outputTypeName, API_VERSION)
            .addStatement("$T<$T, $T> invokeRemote = new $T<>()", FUNCTION_INVOKE_ADAPTER, inputDto, outputDto, HTTP_REMOTE_INVOKE_ADAPTER)
            .addStatement("$T<$T, $T> invoke = new $T<>(invokeLocal, invokeRemote)", FUNCTION_INVOKE_ADAPTER, inputDto, outputDto, INVOCATION_MODE_ROUTING_INVOKE_ADAPTER)
            .addStatement("$T<$T, $T> sink = new $T<>()", FUNCTION_SINK_ADAPTER, outputDto, handlerOutputType, selectSinkAdapter(streamingOutput, DEFAULT_UNARY_SINK_ADAPTER, COLLECT_LIST_SINK_ADAPTER))
            .addStatement("return $T.$L(input, transportContext, source, invoke, sink)", bridgeClass(streamingInput, streamingOutput, UNARY_FUNCTION_TRANSPORT_BRIDGE, FUNCTION_TRANSPORT_BRIDGE), bridgeMethodName(streamingInput, streamingOutput))
            .nextControlFlow("catch (Exception e)")
            .addStatement("throw new RuntimeException(\"Failed handleRequest -> resource.run for input DTO\", e)")
            .endControlFlow()
            .build();
    }

    /**
     * Builds the Java expression used to obtain the execution ID at runtime.
     *
     * @return a String containing a Java expression that evaluates to the execution ID: it uses the value from the provider-specific execution ID expression when that expression is present and not blank, otherwise it uses `UUID.randomUUID().toString()`.
     */
    protected String buildExecutionIdExpression() {
        String expr = getExecutionIdExpression();
        if (expr != null && !expr.isBlank()) {
            return "((" + expr + ") != null && !(" + expr + ").isBlank()) ? (" + expr + ") : $T.randomUUID().toString()";
        } else {
            return "$T.randomUUID().toString()";
        }
    }

    /**
 * Generate provider-specific asynchronous orchestrator handlers (run-async, status, result) and any
 * required request DTO classes.
 *
 * Implementations must emit the generated source files into the provided GenerationContext using the
 * given base package and DTO types. The streaming flags indicate whether input and/or output are
 * streaming, which affects the handler and DTO shapes.
 *
 * @param binding the orchestrator binding containing configuration and declared input/output type names
 * @param ctx the generation context used to write generated source files
 * @param basePackage the base Java package under which generated classes should be placed
 * @param inputDto the ClassName of the input DTO type
 * @param outputDto the ClassName of the output DTO type
 * @param streamingInput true if the orchestrator input is streaming
 * @param streamingOutput true if the orchestrator output is streaming
 * @throws IOException if writing generated files fails
 */
    protected abstract void renderAsyncHandlers(OrchestratorBinding binding, GenerationContext ctx, String basePackage, ClassName inputDto, ClassName outputDto, boolean streamingInput, boolean streamingOutput) throws IOException;

    /**
     * Selects the local invocation delegate expression based on input/output streaming modes.
     *
     * @param streamingInput  true if the handler input is a stream
     * @param streamingOutput true if the handler output is a stream
     * @return the delegate expression as a string: `"inputStream -> inputStream.collect().asList().onItem().transformToUni(resource::run)"`
     *         when `streamingInput` is true and `streamingOutput` is false, otherwise `"resource::run"`.
     */
    private static String localInvokeDelegate(boolean streamingInput, boolean streamingOutput) {
        return (streamingInput && !streamingOutput) ? "inputStream -> inputStream.collect().asList().onItem().transformToUni(resource::run)" : "resource::run";
    }

    /**
 * Selects the source adapter class appropriate for the input streaming mode.
 *
 * @param streaming     true if the source input is streaming, false for single-value input
 * @param defaultAdapter the adapter class to use for non-streaming (unary) input
 * @param multiAdapter   the adapter class to use for streaming (multi) input
 * @return               `multiAdapter` when `streaming` is true, otherwise `defaultAdapter`
 */
protected static ClassName selectSourceAdapter(boolean streaming, ClassName defaultAdapter, ClassName multiAdapter) { return streaming ? multiAdapter : defaultAdapter; }

    /**
     * Selects the appropriate invoke adapter class based on whether the function input and output are streaming.
     *
     * @param streamingInput  true if the function input is a stream, false if unary
     * @param streamingOutput true if the function output is a stream, false if unary
     * @param unary           adapter to use when both input and output are unary
     * @param oneToMany       adapter to use when input is unary and output is streaming
     * @param manyToOne       adapter to use when input is streaming and output is unary
     * @param manyToMany      adapter to use when both input and output are streaming
     * @return                the selected adapter ClassName for the given streaming combination
     */
    protected static ClassName selectInvokeAdapter(boolean streamingInput, boolean streamingOutput, ClassName unary, ClassName oneToMany, ClassName manyToOne, ClassName manyToMany) {
        if (!streamingInput && !streamingOutput) return unary;
        if (!streamingInput) return oneToMany;
        if (!streamingOutput) return manyToOne;
        return manyToMany;
    }

    /**
 * Selects the sink adapter class to use for the function's output based on whether output is streaming.
 *
 * @param streaming       true if the function output is streaming; false otherwise
 * @param defaultAdapter  adapter class to use when output is not streaming
 * @param collectAdapter  adapter class to use when output is streaming (collect/aggregate semantics)
 * @return                the chosen adapter class: `collectAdapter` when `streaming` is true, otherwise `defaultAdapter`
 */
protected static ClassName selectSinkAdapter(boolean streaming, ClassName defaultAdapter, ClassName collectAdapter) { return streaming ? collectAdapter : defaultAdapter; }

    /**
     * Selects the appropriate bridge class based on whether the function input or output is streaming.
     *
     * @param streamingInput true if the function input is streaming
     * @param streamingOutput true if the function output is streaming
     * @param unaryBridge bridge class to use for one-to-one (non-streaming) invocation
     * @param multiBridge bridge class to use when either input or output is streaming
     * @return the chosen `ClassName`: `multiBridge` if either streaming flag is true, otherwise `unaryBridge`
     */
    protected static ClassName bridgeClass(boolean streamingInput, boolean streamingOutput, ClassName unaryBridge, ClassName multiBridge) {
        return (streamingInput || streamingOutput) ? multiBridge : unaryBridge;
    }

    /**
     * Selects the bridge invocation method name for the given input/output streaming modes.
     *
     * @return `invoke` when neither input nor output is streaming; `invokeOneToMany` when input is unary and output is streaming; `invokeManyToOne` when input is streaming and output is unary; `invokeManyToMany` when both input and output are streaming.
     */
    protected static String bridgeMethodName(boolean streamingInput, boolean streamingOutput) {
        if (!streamingInput && !streamingOutput) return "invoke";
        if (!streamingInput) return "invokeOneToMany";
        if (!streamingOutput) return "invokeManyToOne";
        return "invokeManyToMany";
    }
}
