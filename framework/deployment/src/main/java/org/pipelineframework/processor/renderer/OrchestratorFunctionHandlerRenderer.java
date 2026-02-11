package org.pipelineframework.processor.renderer;

import java.io.IOException;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
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
        if (binding.inputStreaming() || binding.outputStreaming()) {
            throw new IllegalStateException(
                "Function orchestrator handler currently supports only unary-unary shape.");
        }

        ClassName applicationScoped = ClassName.get("jakarta.enterprise.context", "ApplicationScoped");
        ClassName inject = ClassName.get("jakarta.inject", "Inject");
        ClassName named = ClassName.get("jakarta.inject", "Named");
        ClassName lambdaContext = ClassName.get("com.amazonaws.services.lambda.runtime", "Context");
        ClassName requestHandler = ClassName.get("com.amazonaws.services.lambda.runtime", "RequestHandler");
        ClassName generatedRole = ClassName.get("org.pipelineframework.annotation", "GeneratedRole");
        ClassName roleEnum = ClassName.get("org.pipelineframework.annotation", "GeneratedRole", "Role");
        ClassName functionTransportContext =
            ClassName.get("org.pipelineframework.transport.function", "FunctionTransportContext");
        ClassName sourceAdapter =
            ClassName.get("org.pipelineframework.transport.function", "FunctionSourceAdapter");
        ClassName invokeAdapter =
            ClassName.get("org.pipelineframework.transport.function", "FunctionInvokeAdapter");
        ClassName sinkAdapter =
            ClassName.get("org.pipelineframework.transport.function", "FunctionSinkAdapter");
        ClassName defaultSourceAdapter =
            ClassName.get("org.pipelineframework.transport.function", "DefaultUnaryFunctionSourceAdapter");
        ClassName localInvokeAdapter =
            ClassName.get("org.pipelineframework.transport.function", "LocalUnaryFunctionInvokeAdapter");
        ClassName defaultSinkAdapter =
            ClassName.get("org.pipelineframework.transport.function", "DefaultUnaryFunctionSinkAdapter");
        ClassName unaryBridge =
            ClassName.get("org.pipelineframework.transport.function", "UnaryFunctionTransportBridge");

        ClassName inputDto = ClassName.get(binding.basePackage() + ".common.dto", binding.inputTypeName() + "Dto");
        ClassName outputDto = ClassName.get(binding.basePackage() + ".common.dto", binding.outputTypeName() + "Dto");
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
            .addSuperinterface(ParameterizedTypeName.get(requestHandler, inputDto, outputDto))
            .addField(FieldSpec.builder(resourceType, "resource", Modifier.PRIVATE)
                .addAnnotation(inject)
                .build());

        MethodSpec handleRequest = MethodSpec.methodBuilder("handleRequest")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(outputDto)
            .addParameter(inputDto, "input")
            .addParameter(lambdaContext, "context")
            .beginControlFlow("try")
            .addStatement("$T transportContext = $T.of("
                    + "context != null ? context.getAwsRequestId() : $S, "
                    + "context != null ? context.getFunctionName() : $S, "
                    + "$S)",
                functionTransportContext, functionTransportContext, UNKNOWN_REQUEST, HANDLER_CLASS, INGRESS)
            .addStatement("$T<$T, $T> source = new $T<>($S, $S)",
                sourceAdapter, inputDto, inputDto, defaultSourceAdapter,
                ORCHESTRATOR_PREFIX + binding.inputTypeName(), API_VERSION)
            .addStatement("$T<$T, $T> invoke = new $T<>(payload -> resource.run(payload), $S, $S)",
                invokeAdapter, inputDto, outputDto, localInvokeAdapter,
                ORCHESTRATOR_PREFIX + binding.outputTypeName(), API_VERSION)
            .addStatement("$T<$T, $T> sink = new $T<>()",
                sinkAdapter, outputDto, outputDto, defaultSinkAdapter)
            .addStatement("return $T.invoke(input, transportContext, source, invoke, sink)", unaryBridge)
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
}
