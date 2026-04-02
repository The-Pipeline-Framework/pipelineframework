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

    protected AbstractOrchestratorFunctionHandlerRenderer() {}

    @Override
    public GenerationTarget target() { return GenerationTarget.REST_RESOURCE; }

    /** Returns the cloud provider name (e.g., "aws", "azure", "gcp"). */
    protected abstract String getCloudProvider();

    /** Returns the cloud provider's context type class name. */
    protected abstract ClassName getContextClassName();

    /** Returns the handler interface class name. */
    protected abstract ClassName getHandlerInterfaceClassName();

    /** Returns the request ID extraction expression. */
    protected abstract String getRequestIdExpression();

    /** Returns the function name extraction expression. */
    protected abstract String getFunctionNameExpression();

    /** Returns the execution ID extraction expression. */
    protected abstract String getExecutionIdExpression();

    @Override
    public void render(OrchestratorBinding binding, GenerationContext ctx) throws IOException {
        boolean streamingInput = binding.inputStreaming();
        boolean streamingOutput = binding.outputStreaming();
        String basePackage = binding.basePackage();

        ClassName inputDto = ClassName.get(basePackage + ".common.dto", binding.inputTypeName() + "Dto");
        ClassName outputDto = ClassName.get(basePackage + ".common.dto", binding.outputTypeName() + "Dto");
        TypeName inputEventType = streamingInput ? ParameterizedTypeName.get(MULTI, inputDto) : inputDto;
        TypeName handlerOutputType = streamingOutput ? ParameterizedTypeName.get(ClassName.get(List.class), outputDto) : outputDto;
        ClassName resourceType = ClassName.get(basePackage + ".orchestrator.service", RESOURCE_CLASS);

        TypeSpec.Builder handler = TypeSpec.classBuilder(HANDLER_CLASS)
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(APPLICATION_SCOPED)
            .addAnnotation(AnnotationSpec.builder(NAMED).addMember("value", "$S", HANDLER_CLASS).build())
            .addAnnotation(AnnotationSpec.builder(GENERATED_ROLE).addMember("value", "$T.$L", ROLE_ENUM, "REST_SERVER").build())
            .addSuperinterface(ParameterizedTypeName.get(getHandlerInterfaceClassName(), inputEventType, handlerOutputType))
            .addField(FieldSpec.builder(resourceType, "resource", Modifier.PRIVATE).addAnnotation(INJECT).build());

        String localInvokeDelegate = localInvokeDelegate(streamingInput, streamingOutput);
        MethodSpec handleRequest = buildHandlerMethod(basePackage, binding.inputTypeName(), binding.outputTypeName(), inputDto, outputDto, inputEventType, handlerOutputType, resourceType, localInvokeDelegate, streamingInput, streamingOutput);
        handler.addMethod(handleRequest);

        JavaFile.builder(basePackage + ".orchestrator.service", handler.build()).build().writeTo(ctx.outputDir());
        renderAsyncHandlers(binding, ctx, basePackage, inputDto, outputDto, streamingInput, streamingOutput);
    }

    protected MethodSpec buildHandlerMethod(String basePackage, String inputTypeName, String outputTypeName, ClassName inputDto, ClassName outputDto, TypeName inputEventType, TypeName handlerOutputType, ClassName resourceType, String localInvokeDelegate, boolean streamingInput, boolean streamingOutput) {
        return MethodSpec.methodBuilder("handleRequest")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(handlerOutputType)
            .addParameter(inputEventType, "input")
            .addParameter(getContextClassName(), "context")
            .beginControlFlow("try")
            .addStatement(buildTransportContextStatement())
            .addStatement("$T<$T, $T> source = new $T<>($S, $S)", FUNCTION_SOURCE_ADAPTER, inputEventType, inputDto, selectSourceAdapter(streamingInput, DEFAULT_UNARY_SOURCE_ADAPTER, MULTI_SOURCE_ADAPTER), ORCHESTRATOR_PREFIX + inputTypeName, API_VERSION)
            .addStatement("$T<$T, $T> invokeLocal = new $T<$T, $T>($L, $S, $S)", FUNCTION_INVOKE_ADAPTER, inputDto, outputDto, selectInvokeAdapter(streamingInput, streamingOutput, LOCAL_UNARY_INVOKE_ADAPTER, LOCAL_ONE_TO_MANY_INVOKE_ADAPTER, LOCAL_MANY_TO_ONE_INVOKE_ADAPTER, LOCAL_MANY_TO_MANY_INVOKE_ADAPTER), inputDto, outputDto, localInvokeDelegate, ORCHESTRATOR_PREFIX + outputTypeName, API_VERSION)
            .addStatement("$T<$T, $T> invokeRemote = new $T<>()", FUNCTION_INVOKE_ADAPTER, inputDto, outputDto, HTTP_REMOTE_INVOKE_ADAPTER)
            .addStatement("$T<$T, $T> invoke = new $T<>(invokeLocal, invokeRemote)", FUNCTION_INVOKE_ADAPTER, inputDto, outputDto, INVOCATION_MODE_ROUTING_INVOKE_ADAPTER)
            .addStatement("$T<$T, $T> sink = new $T<>()", FUNCTION_SINK_ADAPTER, outputDto, handlerOutputType, selectSinkAdapter(streamingOutput, DEFAULT_UNARY_SINK_ADAPTER, COLLECT_LIST_SINK_ADAPTER))
            .addStatement("return $T.$L(input, transportContext, source, invoke, sink)", bridgeClass(streamingInput, streamingOutput, UNARY_FUNCTION_TRANSPORT_BRIDGE, FUNCTION_TRANSPORT_BRIDGE), bridgeMethodName(streamingInput, streamingOutput))
            .nextControlFlow("catch (RuntimeException e)")
            .addStatement("throw new RuntimeException(\"Failed handleRequest -> resource.run for input DTO\", e)")
            .endControlFlow()
            .build();
    }

    protected String buildTransportContextStatement() {
        return "$T transportContext = $T.of(" + getRequestIdExpression() + ", " + getFunctionNameExpression() + ", $S, $T.of(" + "$T.ATTR_CORRELATION_ID, " + getRequestIdExpression() + ", $T.ATTR_EXECUTION_ID, " + buildExecutionIdExpression() + ", $T.ATTR_RETRY_ATTEMPT, $T.getProperty($S, $S), $T.ATTR_DISPATCH_TS_EPOCH_MS, $T.toString($T.currentTimeMillis())))";
    }

    protected String buildExecutionIdExpression() {
        String expr = getExecutionIdExpression();
        return (expr != null && !expr.isBlank()) ? "(" + expr + " != null && !" + expr + ".isBlank()) ? " + expr + " : $T.randomUUID().toString()" : "$T.randomUUID().toString()";
    }

    /** Generates async handlers (run-async, status, result) and request DTOs. */
    protected abstract void renderAsyncHandlers(OrchestratorBinding binding, GenerationContext ctx, String basePackage, ClassName inputDto, ClassName outputDto, boolean streamingInput, boolean streamingOutput) throws IOException;

    private static String localInvokeDelegate(boolean streamingInput, boolean streamingOutput) {
        return (streamingInput && !streamingOutput) ? "inputStream -> inputStream.collect().asList().onItem().transformToUni(resource::run)" : "resource::run";
    }

    protected static ClassName selectSourceAdapter(boolean streaming, ClassName defaultAdapter, ClassName multiAdapter) { return streaming ? multiAdapter : defaultAdapter; }

    protected static ClassName selectInvokeAdapter(boolean streamingInput, boolean streamingOutput, ClassName unary, ClassName oneToMany, ClassName manyToOne, ClassName manyToMany) {
        if (!streamingInput && !streamingOutput) return unary;
        if (!streamingInput) return oneToMany;
        if (!streamingOutput) return manyToOne;
        return manyToMany;
    }

    protected static ClassName selectSinkAdapter(boolean streaming, ClassName defaultAdapter, ClassName collectAdapter) { return streaming ? collectAdapter : defaultAdapter; }

    protected static ClassName bridgeClass(boolean streamingInput, boolean streamingOutput, ClassName unaryBridge, ClassName multiBridge) {
        return (streamingInput || streamingOutput) ? multiBridge : unaryBridge;
    }

    protected static String bridgeMethodName(boolean streamingInput, boolean streamingOutput) {
        if (!streamingInput && !streamingOutput) return "invokeOneToOne";
        if (!streamingInput) return "invokeOneToMany";
        if (!streamingOutput) return "invokeManyToOne";
        return "invokeManyToMany";
    }
}
