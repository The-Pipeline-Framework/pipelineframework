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
import com.squareup.javapoet.TypeVariableName;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.WildcardTypeName;
import org.pipelineframework.processor.PipelineStepProcessor;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.RestBinding;
import org.pipelineframework.processor.ir.StreamingShape;

/**
 * Abstract base class for generating cloud function handler wrappers for REST resources.
 *
 * <p>Concrete implementations generate handlers for specific cloud providers:
 * AWS Lambda, Azure Functions, or Google Cloud Functions.</p>
 *
 * <p>The generated handler:</p>
 * <ul>
 *     <li>Implements the cloud provider's function interface</li>
 *     <li>Injects the pipeline REST resource</li>
 *     <li>Constructs a {@code FunctionTransportContext} with provider-specific metadata</li>
 *     <li>Selects input/output adapters based on streaming shape</li>
 *     <li>Delegates to the {@code FunctionTransportBridge} for execution</li>
 * </ul>
 */
public abstract class AbstractFunctionHandlerRenderer implements PipelineRenderer<RestBinding> {
    protected static final String API_VERSION = "v1";
    protected static final String UNKNOWN_REQUEST = "unknown-request";
    protected static final String INVOKE_STEP = "invoke-step";

    protected static final ClassName APPLICATION_SCOPED =
        ClassName.get("jakarta.enterprise.context", "ApplicationScoped");
    protected static final ClassName INJECT =
        ClassName.get("jakarta.inject", "Inject");
    protected static final ClassName NAMED =
        ClassName.get("jakarta.inject", "Named");
    protected static final ClassName MULTI =
        ClassName.get("io.smallrye.mutiny", "Multi");
    protected static final ClassName GENERATED_ROLE =
        ClassName.get("org.pipelineframework.annotation", "GeneratedRole");
    protected static final ClassName ROLE_ENUM =
        ClassName.get("org.pipelineframework.annotation", "GeneratedRole", "Role");
    protected static final ClassName FUNCTION_TRANSPORT_CONTEXT =
        ClassName.get("org.pipelineframework.transport.function", "FunctionTransportContext");
    protected static final ClassName FUNCTION_TRANSPORT_BRIDGE =
        ClassName.get("org.pipelineframework.transport.function", "FunctionTransportBridge");
    protected static final ClassName FUNCTION_SOURCE_ADAPTER =
        ClassName.get("org.pipelineframework.transport.function", "FunctionSourceAdapter");
    protected static final ClassName FUNCTION_INVOKE_ADAPTER =
        ClassName.get("org.pipelineframework.transport.function", "FunctionInvokeAdapter");
    protected static final ClassName FUNCTION_SINK_ADAPTER =
        ClassName.get("org.pipelineframework.transport.function", "FunctionSinkAdapter");
    protected static final ClassName DEFAULT_UNARY_SOURCE_ADAPTER =
        ClassName.get("org.pipelineframework.transport.function", "DefaultUnaryFunctionSourceAdapter");
    protected static final ClassName MULTI_SOURCE_ADAPTER =
        ClassName.get("org.pipelineframework.transport.function", "MultiFunctionSourceAdapter");
    protected static final ClassName LOCAL_UNARY_INVOKE_ADAPTER =
        ClassName.get("org.pipelineframework.transport.function", "LocalUnaryFunctionInvokeAdapter");
    protected static final ClassName LOCAL_ONE_TO_MANY_INVOKE_ADAPTER =
        ClassName.get("org.pipelineframework.transport.function", "LocalOneToManyFunctionInvokeAdapter");
    protected static final ClassName LOCAL_MANY_TO_ONE_INVOKE_ADAPTER =
        ClassName.get("org.pipelineframework.transport.function", "LocalManyToOneFunctionInvokeAdapter");
    protected static final ClassName LOCAL_MANY_TO_MANY_INVOKE_ADAPTER =
        ClassName.get("org.pipelineframework.transport.function", "LocalManyToManyFunctionInvokeAdapter");
    protected static final ClassName INVOCATION_MODE_ROUTING_INVOKE_ADAPTER =
        ClassName.get("org.pipelineframework.transport.function", "InvocationModeRoutingFunctionInvokeAdapter");
    protected static final ClassName HTTP_REMOTE_INVOKE_ADAPTER =
        ClassName.get("org.pipelineframework.transport.function", "HttpRemoteFunctionInvokeAdapter");
    protected static final ClassName DEFAULT_UNARY_SINK_ADAPTER =
        ClassName.get("org.pipelineframework.transport.function", "DefaultUnaryFunctionSinkAdapter");
    protected static final ClassName COLLECT_LIST_SINK_ADAPTER =
        ClassName.get("org.pipelineframework.transport.function", "CollectListFunctionSinkAdapter");
    protected static final ClassName UNARY_FUNCTION_TRANSPORT_BRIDGE =
        ClassName.get("org.pipelineframework.transport.function", "UnaryFunctionTransportBridge");

    /**
     * Creates a new AbstractFunctionHandlerRenderer.
     */
    protected AbstractFunctionHandlerRenderer() {
    }

    @Override
    public GenerationTarget target() {
        return GenerationTarget.REST_RESOURCE;
    }

    /**
     * Returns the cloud provider name for this renderer (e.g., "aws", "azure", "gcp").
     */
    protected abstract String getCloudProvider();

    /**
     * Returns the fully-qualified class name of the cloud provider's context type.
     * For example, {@code com.amazonaws.services.lambda.runtime.Context} for AWS Lambda.
     */
    protected abstract ClassName getContextClassName();

    /**
     * Returns the fully-qualified interface name that handlers implement.
     * For example, {@code com.amazonaws.services.lambda.runtime.RequestHandler} for AWS Lambda.
     */
    protected abstract ClassName getHandlerInterfaceClassName();

    /**
     * Returns the expression to extract the request ID from the cloud context.
     * For example, {@code context.getAwsRequestId()} for AWS Lambda.
     */
    protected abstract String getRequestIdExpression();

    /**
     * Returns the expression to extract the function name from the cloud context.
     * For example, {@code context.getFunctionName()} for AWS Lambda.
     */
    protected abstract String getFunctionNameExpression();

    /**
     * Returns the expression to extract execution-scoped identifier from the cloud context.
     * For example, {@code context.getLogStreamName()} for AWS Lambda.
     * Returns empty string if not applicable.
     */
    protected abstract String getExecutionIdExpression();

    /**
     * Returns additional context attributes as key-value pairs.
     * Subclasses can override to add provider-specific attributes.
     * @deprecated Not currently used - reserved for future extension
     */
    @Deprecated
    protected List<String> getAdditionalContextAttributes() {
        return List.of();
    }

    /**
     * Generates and writes a REST-oriented cloud function handler class for the given pipeline binding.
     *
     * <p>The generated class implements the cloud provider's function interface for the binding's pipeline
     * step, adapts domain types to DTOs, selects input/output adapters based on the step's streaming shape,
     * constructs a function transport context, and emits a handler method that delegates processing to the
     * pipeline resource.</p>
     *
     * @param binding the REST binding describing the pipeline step and service package
     * @param ctx the generation context providing the output directory and generation utilities
     * @throws IOException if writing the generated Java file to the output directory fails
     * @throws IllegalStateException if the binding's PipelineStepModel has a null streamingShape
     */
    @Override
    public void render(RestBinding binding, GenerationContext ctx) throws IOException {
        PipelineStepModel model = binding.model();
        StreamingShape shape = model.streamingShape();
        if (shape == null) {
            throw new IllegalStateException("Function handler generation requires non-null streamingShape for " + model.serviceName());
        }
        boolean streamingInput = shape == StreamingShape.STREAMING_UNARY || shape == StreamingShape.STREAMING_STREAMING;
        boolean streamingOutput = shape == StreamingShape.UNARY_STREAMING || shape == StreamingShape.STREAMING_STREAMING;

        String serviceClassName = model.generatedName();
        String baseName = removeSuffix(removeSuffix(serviceClassName, "Service"), "Reactive");
        String resourceClassName = baseName + PipelineStepProcessor.REST_RESOURCE_SUFFIX;
        String handlerClassName = baseName + getHandlerSuffix();

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
            .addSuperinterface(ParameterizedTypeName.get(getHandlerInterfaceClassName(), inputEventType, handlerOutputType))
            .addField(FieldSpec.builder(resourceType, "resource", Modifier.PRIVATE)
                .addAnnotation(INJECT)
                .build());

        String localInvokeDelegate = localInvokeDelegate(shape);

        MethodSpec handleRequest = buildHandlerMethod(
            baseName,
            inputDto, outputDto,
            inputEventType, handlerOutputType, resourceType,
            localInvokeDelegate, shape, streamingInput, streamingOutput);

        handler.addMethod(handleRequest);

        JavaFile.builder(binding.servicePackage() + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX, handler.build())
            .build()
            .writeTo(ctx.outputDir());
    }

    /**
     * Builds the handler method specification.
     */
    protected MethodSpec buildHandlerMethod(
            String baseName,
            TypeName inputDto,
            TypeName outputDto,
            TypeName inputEventType,
            TypeName handlerOutputType,
            ClassName resourceType,
            String localInvokeDelegate,
            StreamingShape shape,
            boolean streamingInput,
            boolean streamingOutput) {

        ClassName contextClass = getContextClassName();

        MethodSpec.Builder methodBuilder = MethodSpec.methodBuilder("handleRequest")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(handlerOutputType)
            .addParameter(inputEventType, "input")
            .addParameter(contextClass, "context");

        methodBuilder
            .beginControlFlow("try")
            .addStatement("$T transportContext = $T.of("
                + "$S, $S, $S, "  // requestId, functionName, stage
                + "$T.of("
                + "$T.ATTR_CORRELATION_ID, " + getRequestIdExpression() + ", "
                + "$T.ATTR_EXECUTION_ID, " + buildExecutionIdExpression() + ", "
                + "$T.ATTR_RETRY_ATTEMPT, $T.getProperty($S, $S), "
                + "$T.ATTR_DISPATCH_TS_EPOCH_MS, $T.toString($T.currentTimeMillis())))",
                FUNCTION_TRANSPORT_CONTEXT, FUNCTION_TRANSPORT_CONTEXT,
                UNKNOWN_REQUEST, getHandlerSuffix(), INVOKE_STEP,
                ClassName.get("java.util", "Map"),
                FUNCTION_TRANSPORT_CONTEXT,
                FUNCTION_TRANSPORT_CONTEXT, buildExecutionIdFallbackExpression(),
                FUNCTION_TRANSPORT_CONTEXT, ClassName.get(System.class), "tpf.transport.retry-attempt", "0",
                FUNCTION_TRANSPORT_CONTEXT, ClassName.get(Long.class), ClassName.get(System.class))
            .addStatement("$T<$T, $T> source = new $T<>($S, $S)",
                FUNCTION_SOURCE_ADAPTER, inputEventType, inputDto,
                streamingInput ? MULTI_SOURCE_ADAPTER : DEFAULT_UNARY_SOURCE_ADAPTER,
                baseName + ".input",
                API_VERSION)
            .addStatement("$T<$T, $T> invokeLocal = new $T<$T, $T>($L, $S, $S)",
                FUNCTION_INVOKE_ADAPTER, inputDto, outputDto,
                selectInvokeAdapterForShape(shape,
                    LOCAL_UNARY_INVOKE_ADAPTER,
                    LOCAL_ONE_TO_MANY_INVOKE_ADAPTER,
                    LOCAL_MANY_TO_ONE_INVOKE_ADAPTER,
                    LOCAL_MANY_TO_MANY_INVOKE_ADAPTER),
                inputDto, outputDto,
                localInvokeDelegate,
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
            .endControlFlow();

        return methodBuilder.build();
    }

    /**
     * Builds the execution ID expression for the FunctionTransportContext.
     * Handles null/empty execution ID expressions from subclasses.
     * @return Java expression for execution ID
     */
    protected String buildExecutionIdExpression() {
        String executionIdExpr = getExecutionIdExpression();
        if (executionIdExpr != null && !executionIdExpr.isBlank()) {
            return "(" + executionIdExpr + " != null && !" + executionIdExpr + ".isBlank()) ? " + executionIdExpr + " : $T.randomUUID().toString()";
        } else {
            return "$T.randomUUID().toString()";
        }
    }

    /**
     * Returns the fallback expression for execution ID when primary is unavailable.
     * @return JavaPoet format string for UUID generation
     */
    protected String buildExecutionIdFallbackExpression() {
        return "java.util.UUID";
    }

    /**
     * Builds the FunctionTransportContext construction statement.
     * @deprecated Use inline statement construction in buildHandlerMethod
     */
    @Deprecated
    protected String buildTransportContextStatement() {
        StringBuilder sb = new StringBuilder();
        sb.append("$T transportContext = $T.of(");
        sb.append(getRequestIdExpression()).append(", ");
        sb.append(getFunctionNameExpression()).append(", ");
        sb.append("$S, ");
        sb.append("$T.of(");
        sb.append("$T.ATTR_TRANSPORT_PROTOCOL, $S, ");
        sb.append("$T.ATTR_CORRELATION_ID, ").append(getRequestIdExpression()).append(", ");
        sb.append("$T.ATTR_EXECUTION_ID, ");

        String executionIdExpr = getExecutionIdExpression();
        if (executionIdExpr != null && !executionIdExpr.isBlank()) {
            sb.append("(").append(executionIdExpr).append(" != null && !").append(executionIdExpr).append(".isBlank()) ? ").append(executionIdExpr).append(" : $T.randomUUID().toString()");
        } else {
            sb.append("$T.randomUUID().toString()");
        }

        sb.append(", ");
        sb.append("$T.ATTR_RETRY_ATTEMPT, $T.getProperty($S, $S), ");
        sb.append("$T.ATTR_DISPATCH_TS_EPOCH_MS, $T.toString($T.currentTimeMillis()))");

        return sb.toString();
    }

    /**
     * Returns the handler suffix for this cloud provider (e.g., "FunctionHandler").
     */
    protected String getHandlerSuffix() {
        return "FunctionHandler";
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
                "Unsupported domain type for DTO conversion: " + domainType + 
                ". Expected a concrete class type or parameterized type with concrete class arguments.");
        }
        // This should never happen - all cases covered above
        throw new IllegalStateException(
            "Unexpected domain type for DTO conversion: " + domainType + 
            " (class: " + domainType.getClass().getName() + ")");
    }

    private String rewritePackageToDto(String packageName) {
        if (packageName == null || packageName.isBlank()) {
            return "";
        }
        String[] segments = packageName.split("\\.");
        boolean replaced = false;
        for (int i = 0; i < segments.length; i++) {
            if ("domain".equals(segments[i]) || "service".equals(segments[i])) {
                segments[i] = "dto";
                replaced = true;
                break;
            }
        }
        if (!replaced) {
            throw new IllegalArgumentException(
                "DTO package rewrite failed: expected 'domain' or 'service' segment in package '" + packageName + "'. " +
                "Unable to determine DTO package for generated handler imports.");
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
    public String handlerFqcn(String servicePackage, String generatedName) {
        String baseName = removeSuffix(removeSuffix(generatedName, "Service"), "Reactive");
        return servicePackage + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX + "." + baseName + getHandlerSuffix();
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

    private static String localInvokeDelegate(StreamingShape shape) {
        return switch (shape) {
            case UNARY_UNARY, UNARY_STREAMING, STREAMING_STREAMING -> "resource::process";
            case STREAMING_UNARY ->
                "inputStream -> inputStream.collect().asList().onItem().transformToUni(resource::process)";
        };
    }

    private static String removeSuffix(String value, String suffix) {
        if (value == null || suffix == null || suffix.isBlank()) {
            return value;
        }
        return value.endsWith(suffix) ? value.substring(0, value.length() - suffix.length()) : value;
    }
}
