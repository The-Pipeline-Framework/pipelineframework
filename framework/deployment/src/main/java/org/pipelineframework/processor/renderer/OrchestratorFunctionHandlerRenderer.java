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
    private static final ClassName UNSUPPORTED_REMOTE_INVOKE_ADAPTER =
        ClassName.get("org.pipelineframework.transport.function", "UnsupportedRemoteFunctionInvokeAdapter");
    private static final ClassName DEFAULT_UNARY_SINK_ADAPTER =
        ClassName.get("org.pipelineframework.transport.function", "DefaultUnaryFunctionSinkAdapter");
    private static final ClassName COLLECT_LIST_SINK_ADAPTER =
        ClassName.get("org.pipelineframework.transport.function", "CollectListFunctionSinkAdapter");
    private static final ClassName UNARY_FUNCTION_TRANSPORT_BRIDGE =
        ClassName.get("org.pipelineframework.transport.function", "UnaryFunctionTransportBridge");

    /**
     * Resolve fully-qualified orchestrator function handler class name for a base package.
     *
     * @param basePackage base package
     * @return generated handler FQCN
     */
    public static String handlerFqcn(String basePackage) {
        return basePackage + ".orchestrator.service." + HANDLER_CLASS;
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
                    + "$S)",
                FUNCTION_TRANSPORT_CONTEXT, FUNCTION_TRANSPORT_CONTEXT, UNKNOWN_REQUEST, HANDLER_CLASS, INGRESS)
            .addStatement("$T<$T, $T> source = new $T<>($S, $S)",
                FUNCTION_SOURCE_ADAPTER, inputEventType, inputDto,
                selectSourceAdapter(streamingInput, DEFAULT_UNARY_SOURCE_ADAPTER, MULTI_SOURCE_ADAPTER),
                ORCHESTRATOR_PREFIX + binding.inputTypeName(), API_VERSION)
            .addStatement("$T<$T, $T> invokeLocal = new $T<>(resource::run, $S, $S)",
                FUNCTION_INVOKE_ADAPTER, inputDto, outputDto,
                selectInvokeAdapter(streamingInput, streamingOutput,
                    LOCAL_UNARY_INVOKE_ADAPTER,
                    LOCAL_ONE_TO_MANY_INVOKE_ADAPTER,
                    LOCAL_MANY_TO_ONE_INVOKE_ADAPTER,
                    LOCAL_MANY_TO_MANY_INVOKE_ADAPTER),
                ORCHESTRATOR_PREFIX + binding.outputTypeName(), API_VERSION)
            .addStatement("$T<$T, $T> invokeRemote = new $T<>()",
                FUNCTION_INVOKE_ADAPTER, inputDto, outputDto,
                UNSUPPORTED_REMOTE_INVOKE_ADAPTER)
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
    }

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
}
