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
            ? ParameterizedTypeName.get(multi, inputDto)
            : inputDto;
        TypeName handlerOutputType = streamingOutput
            ? ParameterizedTypeName.get(ClassName.get(List.class), outputDto)
            : outputDto;
        ClassName resourceType = ClassName.get(
            binding.servicePackage() + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX,
            resourceClassName);

        TypeSpec.Builder handler = TypeSpec.classBuilder(handlerClassName)
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(applicationScoped)
            .addAnnotation(AnnotationSpec.builder(named)
                .addMember("value", "$S", handlerClassName)
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
                functionTransportContext, functionTransportContext, UNKNOWN_REQUEST, handlerClassName, INVOKE_STEP)
            .addStatement("$T<$T, $T> source = new $T<>($S, $S)",
                sourceAdapter, inputEventType, inputDto,
                streamingInput ? multiSourceAdapter : defaultSourceAdapter,
                baseName + ".input",
                API_VERSION)
            .addStatement("$T<$T, $T> invoke = new $T<>(resource::process, $S, $S)",
                invokeAdapter, inputDto, outputDto,
                selectInvokeAdapterForShape(shape,
                    localInvokeAdapter,
                    localOneToManyInvokeAdapter,
                    localManyToOneInvokeAdapter,
                    localManyToManyInvokeAdapter),
                baseName + ".output",
                API_VERSION)
            .addStatement("$T<$T, $T> sink = new $T<>()",
                sinkAdapter, outputDto, handlerOutputType,
                streamingOutput ? collectListSinkAdapter : defaultSinkAdapter)
            .addStatement(
                bridgeInvocationFormat(shape),
                shape == StreamingShape.UNARY_UNARY ? unaryBridge : functionTransportBridge)
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
