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
import com.squareup.javapoet.TypeVariableName;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.WildcardTypeName;
import org.pipelineframework.processor.PipelineStepProcessor;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.RestBinding;
import org.pipelineframework.processor.ir.StreamingShape;

/**
 * Generates native AWS Lambda RequestHandler wrappers for unary REST resources.
 */
public class RestFunctionHandlerRenderer implements PipelineRenderer<RestBinding> {
    private static final String API_VERSION = "v1";
    private static final String UNKNOWN_REQUEST = "unknown-request";
    private static final String INVOKE_STEP = "invoke-step";
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
     * Creates a new RestFunctionHandlerRenderer.
     */
    public RestFunctionHandlerRenderer() {
    }

    @Override
    public GenerationTarget target() {
        return GenerationTarget.REST_RESOURCE;
    }

    @Override
    public void render(RestBinding binding, GenerationContext ctx) throws IOException {
        PipelineStepModel model = binding.model();
        StreamingShape shape = model.streamingShape() == null ? StreamingShape.UNARY_UNARY : model.streamingShape();
        boolean streamingInput = shape == StreamingShape.STREAMING_UNARY || shape == StreamingShape.STREAMING_STREAMING;
        boolean streamingOutput = shape == StreamingShape.UNARY_STREAMING || shape == StreamingShape.STREAMING_STREAMING;

        String serviceClassName = model.generatedName();
        String baseName = removeSuffix(removeSuffix(serviceClassName, "Service"), "Reactive");
        String resourceClassName = baseName + PipelineStepProcessor.REST_RESOURCE_SUFFIX;
        String handlerClassName = baseName + "FunctionHandler";

        TypeName inputDto = model.inboundDomainType() != null
            ? convertDomainToDtoType(model.inboundDomainType())
            : ClassName.OBJECT;
        TypeName outputDto = model.outboundDomainType() != null
            ? convertDomainToDtoType(model.outboundDomainType())
            : ClassName.OBJECT;
        TypeName inputEventType = streamingInput
            ? ParameterizedTypeName.get(MULTI, inputDto)
            : inputDto;
        TypeName handlerOutputType = streamingOutput
            ? ParameterizedTypeName.get(ClassName.get(List.class), outputDto)
            : outputDto;
        ClassName resourceType = ClassName.get(
            binding.servicePackage() + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX,
            resourceClassName);

        TypeSpec.Builder handler = TypeSpec.classBuilder(handlerClassName)
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(APPLICATION_SCOPED)
            .addAnnotation(AnnotationSpec.builder(NAMED)
                .addMember("value", "$S", handlerClassName)
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
                FUNCTION_TRANSPORT_CONTEXT, FUNCTION_TRANSPORT_CONTEXT, UNKNOWN_REQUEST, handlerClassName, INVOKE_STEP)
            .addStatement("$T<$T, $T> source = new $T<>($S, $S)",
                FUNCTION_SOURCE_ADAPTER, inputEventType, inputDto,
                streamingInput ? MULTI_SOURCE_ADAPTER : DEFAULT_UNARY_SOURCE_ADAPTER,
                baseName + ".input",
                API_VERSION)
            .addStatement("$T<$T, $T> invokeLocal = new $T<$T, $T>(resource::process, $S, $S)",
                FUNCTION_INVOKE_ADAPTER, inputDto, outputDto,
                selectInvokeAdapterForShape(shape,
                    LOCAL_UNARY_INVOKE_ADAPTER,
                    LOCAL_ONE_TO_MANY_INVOKE_ADAPTER,
                    LOCAL_MANY_TO_ONE_INVOKE_ADAPTER,
                    LOCAL_MANY_TO_MANY_INVOKE_ADAPTER),
                inputDto, outputDto,
                baseName + ".output",
                API_VERSION)
            .addStatement("$T<$T, $T> invokeRemote = new $T<>()",
                FUNCTION_INVOKE_ADAPTER, inputDto, outputDto,
                HTTP_REMOTE_INVOKE_ADAPTER)
            .addStatement("$T<$T, $T> invoke = new $T<>(invokeLocal, invokeRemote)",
                FUNCTION_INVOKE_ADAPTER, inputDto, outputDto,
                INVOCATION_MODE_ROUTING_INVOKE_ADAPTER)
            .addStatement("$T<$T, $T> sink = new $T<>()",
                FUNCTION_SINK_ADAPTER, outputDto, handlerOutputType,
                streamingOutput ? COLLECT_LIST_SINK_ADAPTER : DEFAULT_UNARY_SINK_ADAPTER)
            .addStatement(
                bridgeInvocationFormat(shape),
                shape == StreamingShape.UNARY_UNARY ? UNARY_FUNCTION_TRANSPORT_BRIDGE : FUNCTION_TRANSPORT_BRIDGE)
            .nextControlFlow("catch (Exception e)")
            .addStatement("$T inputType = (input == null) ? \"null\" : input.getClass().getName()", String.class)
            .addStatement(
                "throw new $T($S + inputType, e)",
                RuntimeException.class,
                "Failed handleRequest -> resource.process for input type: ")
            .endControlFlow()
            .build();
        handler.addMethod(handleRequest);

        JavaFile.builder(binding.servicePackage() + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX, handler.build())
            .build()
            .writeTo(ctx.outputDir());
    }

    private TypeName convertDomainToDtoType(TypeName domainType) {
        if (domainType == null) {
            return ClassName.OBJECT;
        }
        if (domainType instanceof ParameterizedTypeName parameterizedTypeName) {
            TypeName convertedRawType = convertDomainToDtoType(parameterizedTypeName.rawType);
            if (!(convertedRawType instanceof ClassName convertedRawClassName)) {
                throw new IllegalArgumentException(
                    "Unsupported parameterized raw domain type for DTO conversion: "
                        + parameterizedTypeName.rawType);
            }
            TypeName[] convertedArguments = new TypeName[parameterizedTypeName.typeArguments.size()];
            for (int i = 0; i < parameterizedTypeName.typeArguments.size(); i++) {
                convertedArguments[i] = convertDomainToDtoType(parameterizedTypeName.typeArguments.get(i));
            }
            return ParameterizedTypeName.get(convertedRawClassName, convertedArguments);
        }
        if (domainType instanceof ClassName className) {
            String dtoSimpleName = className.simpleName().endsWith("Dto")
                ? className.simpleName()
                : className.simpleName() + "Dto";
            return ClassName.get(rewritePackageToDto(className.packageName()), dtoSimpleName);
        }
        if (domainType instanceof TypeVariableName
            || domainType instanceof WildcardTypeName
            || domainType.isPrimitive()) {
            throw new IllegalArgumentException(
                "Unsupported domain type for DTO conversion: " + domainType);
        }
        throw new IllegalArgumentException(
            "Unsupported domain type for DTO conversion: " + domainType);
    }

    private String rewritePackageToDto(String packageName) {
        if (packageName == null || packageName.isBlank()) {
            return "";
        }
        String[] segments = packageName.split("\\.");
        for (int i = 0; i < segments.length; i++) {
            if ("domain".equals(segments[i]) || "service".equals(segments[i])) {
                segments[i] = "dto";
                break;
            }
        }
        return String.join(".", segments);
    }

    /**
     * Returns the generated REST function handler fully-qualified class name.
     *
     * @param servicePackage service package
     * @param generatedName generated service name
     * @return handler FQCN
     */
    public static String handlerFqcn(String servicePackage, String generatedName) {
        String baseName = removeSuffix(removeSuffix(generatedName, "Service"), "Reactive");
        return servicePackage + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX + "." + baseName + "FunctionHandler";
    }

    private static ClassName selectInvokeAdapterForShape(
            StreamingShape shape,
            ClassName localInvokeAdapter,
            ClassName localOneToManyInvokeAdapter,
            ClassName localManyToOneInvokeAdapter,
            ClassName localManyToManyInvokeAdapter) {
        return switch (shape) {
            case UNARY_UNARY -> localInvokeAdapter;
            case UNARY_STREAMING -> localOneToManyInvokeAdapter;
            case STREAMING_UNARY -> localManyToOneInvokeAdapter;
            case STREAMING_STREAMING -> localManyToManyInvokeAdapter;
        };
    }

    private static String bridgeInvocationFormat(StreamingShape shape) {
        return switch (shape) {
            case UNARY_UNARY -> "return $T.invoke(input, transportContext, source, invoke, sink)";
            case UNARY_STREAMING -> "return $T.invokeOneToMany(input, transportContext, source, invoke, sink)";
            case STREAMING_UNARY -> "return $T.invokeManyToOne(input, transportContext, source, invoke, sink)";
            case STREAMING_STREAMING -> "return $T.invokeManyToMany(input, transportContext, source, invoke, sink)";
        };
    }

    private static String removeSuffix(String value, String suffix) {
        if (value == null || suffix == null || suffix.isBlank()) {
            return value;
        }
        return value.endsWith(suffix) ? value.substring(0, value.length() - suffix.length()) : value;
    }
}
