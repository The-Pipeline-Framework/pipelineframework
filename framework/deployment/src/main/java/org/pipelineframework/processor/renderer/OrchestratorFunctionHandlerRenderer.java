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

        ClassName applicationScoped = ClassName.get("jakarta.enterprise.context", "ApplicationScoped");
        ClassName inject = ClassName.get("jakarta.inject", "Inject");
        ClassName named = ClassName.get("jakarta.inject", "Named");
        ClassName multi = ClassName.get("io.smallrye.mutiny", "Multi");
        ClassName lambdaContext = ClassName.get("com.amazonaws.services.lambda.runtime", "Context");
        ClassName requestHandler = ClassName.get("com.amazonaws.services.lambda.runtime", "RequestHandler");
        ClassName generatedRole = ClassName.get("org.pipelineframework.annotation", "GeneratedRole");
        ClassName roleEnum = ClassName.get("org.pipelineframework.annotation", "GeneratedRole", "Role");
        ClassName functionTransportContext =
            ClassName.get("org.pipelineframework.transport.function", "FunctionTransportContext");
        ClassName functionTransportBridge =
            ClassName.get("org.pipelineframework.transport.function", "FunctionTransportBridge");
        ClassName sourceAdapter =
            ClassName.get("org.pipelineframework.transport.function", "FunctionSourceAdapter");
        ClassName invokeAdapter =
            ClassName.get("org.pipelineframework.transport.function", "FunctionInvokeAdapter");
        ClassName sinkAdapter =
            ClassName.get("org.pipelineframework.transport.function", "FunctionSinkAdapter");
        ClassName defaultSourceAdapter =
            ClassName.get("org.pipelineframework.transport.function", "DefaultUnaryFunctionSourceAdapter");
        ClassName multiSourceAdapter =
            ClassName.get("org.pipelineframework.transport.function", "MultiFunctionSourceAdapter");
        ClassName localInvokeAdapter =
            ClassName.get("org.pipelineframework.transport.function", "LocalUnaryFunctionInvokeAdapter");
        ClassName localOneToManyInvokeAdapter =
            ClassName.get("org.pipelineframework.transport.function", "LocalOneToManyFunctionInvokeAdapter");
        ClassName localManyToOneInvokeAdapter =
            ClassName.get("org.pipelineframework.transport.function", "LocalManyToOneFunctionInvokeAdapter");
        ClassName localManyToManyInvokeAdapter =
            ClassName.get("org.pipelineframework.transport.function", "LocalManyToManyFunctionInvokeAdapter");
        ClassName defaultSinkAdapter =
            ClassName.get("org.pipelineframework.transport.function", "DefaultUnaryFunctionSinkAdapter");
        ClassName collectListSinkAdapter =
            ClassName.get("org.pipelineframework.transport.function", "CollectListFunctionSinkAdapter");
        ClassName unaryBridge =
            ClassName.get("org.pipelineframework.transport.function", "UnaryFunctionTransportBridge");

        ClassName inputDto = ClassName.get(binding.basePackage() + ".common.dto", binding.inputTypeName() + "Dto");
        ClassName outputDto = ClassName.get(binding.basePackage() + ".common.dto", binding.outputTypeName() + "Dto");
        TypeName inputEventType = streamingInput
            ? ParameterizedTypeName.get(multi, inputDto)
            : inputDto;
        TypeName handlerOutputType = streamingOutput
            ? ParameterizedTypeName.get(ClassName.get(List.class), outputDto)
            : outputDto;
        ClassName resourceType = ClassName.get(binding.basePackage() + ".orchestrator.service", RESOURCE_CLASS);

        TypeSpec.Builder handler = TypeSpec.classBuilder(HANDLER_CLASS)
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(applicationScoped)
            .addAnnotation(AnnotationSpec.builder(named)
                .addMember("value", "$S", HANDLER_CLASS)
                .build())
            .addAnnotation(AnnotationSpec.builder(generatedRole)
                .addMember("value", "$T.$L", roleEnum, "REST_SERVER")
                .build())
            .addSuperinterface(ParameterizedTypeName.get(requestHandler, inputEventType, handlerOutputType))
            .addField(FieldSpec.builder(resourceType, "resource", Modifier.PRIVATE)
                .addAnnotation(inject)
                .build());

        MethodSpec handleRequest = MethodSpec.methodBuilder("handleRequest")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(handlerOutputType)
            .addParameter(inputEventType, "input")
            .addParameter(lambdaContext, "context")
            .beginControlFlow("try")
            .addStatement("$T transportContext = $T.of("
                    + "context != null ? context.getAwsRequestId() : $S, "
                    + "context != null ? context.getFunctionName() : $S, "
                    + "$S)",
                functionTransportContext, functionTransportContext, UNKNOWN_REQUEST, HANDLER_CLASS, INGRESS)
            .addStatement("$T<$T, $T> source = new $T<>($S, $S)",
                sourceAdapter, inputEventType, inputDto, streamingInput ? multiSourceAdapter : defaultSourceAdapter,
                ORCHESTRATOR_PREFIX + binding.inputTypeName(), API_VERSION)
            .addStatement("$T<$T, $T> invoke = new $T<>(resource::run, $S, $S)",
                invokeAdapter, inputDto, outputDto,
                selectInvokeAdapter(streamingInput, streamingOutput,
                    localInvokeAdapter,
                    localOneToManyInvokeAdapter,
                    localManyToOneInvokeAdapter,
                    localManyToManyInvokeAdapter),
                ORCHESTRATOR_PREFIX + binding.outputTypeName(), API_VERSION)
            .addStatement("$T<$T, $T> sink = new $T<>()",
                sinkAdapter, outputDto, handlerOutputType,
                streamingOutput ? collectListSinkAdapter : defaultSinkAdapter)
            .addStatement("return $T.$L(input, transportContext, source, invoke, sink)",
                bridgeClass(streamingInput, streamingOutput, unaryBridge, functionTransportBridge),
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
