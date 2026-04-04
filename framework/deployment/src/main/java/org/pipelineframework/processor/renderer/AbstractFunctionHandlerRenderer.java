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
import com.squareup.javapoet.CodeBlock;
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
     * Constructs an AbstractFunctionHandlerRenderer used as a base for generating REST-oriented cloud function handler wrappers.
     */
    protected AbstractFunctionHandlerRenderer() {
    }

    /**
     * Specifies the generation target for this renderer.
     *
     * @return the generation target: {@link GenerationTarget#REST_RESOURCE}
     */
    @Override
    public GenerationTarget target() {
        return GenerationTarget.REST_RESOURCE;
    }

    /**
 * Provide the cloud provider identifier used by this renderer.
 *
 * @return the cloud provider identifier, e.g. "aws", "azure", or "gcp"
 */
    protected abstract String getCloudProvider();

    /**
 * Supply the fully qualified ClassName of the cloud provider's function invocation context.
 *
 * @return the ClassName representing the provider-specific context type (for example, {@code com.amazonaws.services.lambda.runtime.Context})
 */
    protected abstract ClassName getContextClassName();

    /**
 * Fully-qualified handler interface ClassName implemented by the generated handlers.
 *
 * @return the handler interface ClassName, e.g. {@code com.amazonaws.services.lambda.runtime.RequestHandler}
 */
    protected abstract ClassName getHandlerInterfaceClassName();

    /**
 * Provide the Java expression that extracts the request ID from the cloud function context.
 *
 * <p>Example: {@code context.getAwsRequestId()} for AWS Lambda.
 *
 * @return a String containing a Java expression which, when evaluated against the handler's context parameter, yields the request ID
 */
    protected abstract CodeBlock getRequestIdExpression();

    /**
 * Provide the Java expression used to obtain the function name from the provider's execution context.
 *
 * <p>Example: {@code context.getFunctionName()} for AWS Lambda.</p>
 *
 * @return a Java expression string that evaluates to the function name from the cloud context
 */
    protected abstract CodeBlock getFunctionNameExpression();

    /**
 * Provide a Java expression that yields an execution-scoped identifier from the cloud context.
 *
 * <p>Example: {@code context.getLogStreamName()} for AWS Lambda. Return an empty string if no
 * execution-scoped identifier is available for the provider.
 *
 * @return a Java expression that evaluates to the execution-scoped identifier, or an empty string
 *         if not applicable
 */
    protected abstract CodeBlock getExecutionIdExpression();

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
         * Generate a REST-oriented cloud function handler class for the provided pipeline binding and write it to the output directory.
         *
         * The generated handler adapts domain types to DTOs, selects adapters and bridge logic based on the step's streaming shape,
         * and implements the provider-specific function interface delegating processing to the pipeline resource.
         *
         * @param binding the REST binding describing the pipeline step and target service package
         * @param ctx the generation context supplying the output directory and generation utilities
         * @throws IOException if writing the generated Java file fails
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
                .build());

        // GCP HttpFunction is not a generic interface - add it raw without parameterization
        ClassName handlerInterface = getHandlerInterfaceClassName();
        boolean isGcp = handlerInterface != null && "HttpFunction".equals(handlerInterface.simpleName());
        if (isGcp) {
            handler.addSuperinterface(handlerInterface);
        } else if (handlerInterface != null) {
            handler.addSuperinterface(ParameterizedTypeName.get(handlerInterface, inputEventType, handlerOutputType));
        }

        handler.addField(FieldSpec.builder(resourceType, "resource", Modifier.PRIVATE)
            .addAnnotation(INJECT)
            .build());

        String localInvokeDelegate = localInvokeDelegate(shape);

        MethodSpec handlerMethod = isGcp
            ? buildGcpServiceMethod(baseName, inputDto, outputDto, inputEventType, handlerOutputType, resourceType, localInvokeDelegate, shape, streamingInput, streamingOutput)
            : buildHandlerMethod(
                baseName,
                inputDto, outputDto,
                inputEventType, handlerOutputType, resourceType,
                localInvokeDelegate, shape, streamingInput, streamingOutput);

        handler.addMethod(handlerMethod);

        JavaFile.builder(binding.servicePackage() + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX, handler.build())
            .build()
            .writeTo(ctx.outputDir());
    }

    /**
     * Constructs the generated handler method `handleRequest` that wires source, invoke, sink adapters
     * and a transport bridge according to the provided streaming shape and DTO types.
     *
     * @param baseName               base logical name of the function (used for adapter identifiers)
     * @param inputDto               DTO type for individual input elements
     * @param outputDto              DTO type for individual output elements
     * @param inputEventType         declared type of the incoming event parameter (may be a stream wrapper)
     * @param handlerOutputType      declared return type of the handler method (may be a collection wrapper)
     * @param resourceType           the injected REST resource class used for local delegation
     * @param localInvokeDelegate    expression or method reference used to delegate local invocation (e.g., "resource::process" or a transformation expression)
     * @param shape                  the StreamingShape describing unary/streaming input and output combinations
     * @param streamingInput         true when the handler should treat input as a streaming source
     * @param streamingOutput        true when the handler should produce streaming output
     * @return                       a JavaPoet MethodSpec representing the complete `handleRequest` method
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
            .addStatement("$L", buildTransportContextCodeBlock())
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
     * Constructs the GCP-specific `service` method for HttpFunction handlers.
     *
     * @param baseName               base logical name of the function
     * @param inputDto               DTO type for individual input elements
     * @param outputDto              DTO type for individual output elements
     * @param inputEventType         declared type of the incoming event parameter
     * @param handlerOutputType      declared return type of the handler method
     * @param resourceType           the injected REST resource class
     * @param localInvokeDelegate    expression for local delegation
     * @param shape                  the StreamingShape
     * @param streamingInput         true for streaming input
     * @param streamingOutput        true for streaming output
     * @return                       a JavaPoet MethodSpec for the GCP service method
     */
    protected MethodSpec buildGcpServiceMethod(
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

        ClassName httpRequest = ClassName.get("com.google.cloud.functions", "HttpRequest");
        ClassName httpResponse = ClassName.get("com.google.cloud.functions", "HttpResponse");
        ClassName objectMapper = ClassName.get("com.fasterxml.jackson.databind", "ObjectMapper");

        // Determine input type for deserialization based on streaming mode
        TypeName deserializeType = streamingInput ? inputEventType : inputDto;
        // Determine result type based on streaming mode
        TypeName resultType = handlerOutputType;

        return MethodSpec.methodBuilder("service")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(TypeName.VOID)
            .addParameter(httpRequest, "request")
            .addParameter(httpResponse, "response")
            .addException(Exception.class)
            .addStatement("$T mapper = new $T()", objectMapper, objectMapper)
            .beginControlFlow("try")
            .addStatement("$T input = mapper.readValue(request.getReader(), $T.class)", deserializeType, deserializeType)
            .addStatement("$T transportContext = $T.of("
                + "request.getFirstHeader($S).orElse($T.randomUUID().toString()), "
                + "$S, $S, $T.of("
                + "$T.ATTR_CORRELATION_ID, request.getFirstHeader($S).orElse($T.randomUUID().toString()), "
                + "$T.ATTR_EXECUTION_ID, request.getFirstHeader($S).orElse($T.randomUUID().toString()), "
                + "$T.ATTR_RETRY_ATTEMPT, $T.getProperty($S, $S), "
                + "$T.ATTR_DISPATCH_TS_EPOCH_MS, $T.toString($T.currentTimeMillis())))",
                FUNCTION_TRANSPORT_CONTEXT, FUNCTION_TRANSPORT_CONTEXT,
                "X-Cloud-Trace-Context", ClassName.get("java.util", "UUID"),
                baseName + ".input", INVOKE_STEP,
                ClassName.get("java.util", "Map"),
                "X-Cloud-Trace-Context", ClassName.get("java.util", "UUID"),
                "X-Cloud-Trace-Context", ClassName.get("java.util", "UUID"),
                FUNCTION_TRANSPORT_CONTEXT, ClassName.get(System.class), "tpf.transport.retry-attempt", "0",
                FUNCTION_TRANSPORT_CONTEXT, ClassName.get(Long.class), ClassName.get(System.class))
            .addStatement("$T<$T, $T> source = new $T<>($S, $S)",
                FUNCTION_SOURCE_ADAPTER, inputEventType, inputDto,
                streamingInput ? MULTI_SOURCE_ADAPTER : DEFAULT_UNARY_SOURCE_ADAPTER,
                baseName + ".input", API_VERSION)
            .addStatement("$T<$T, $T> invokeLocal = new $T<$T, $T>($L, $S, $S)",
                FUNCTION_INVOKE_ADAPTER, inputDto, outputDto,
                selectInvokeAdapterForShape(shape,
                    LOCAL_UNARY_INVOKE_ADAPTER,
                    LOCAL_ONE_TO_MANY_INVOKE_ADAPTER,
                    LOCAL_MANY_TO_ONE_INVOKE_ADAPTER,
                    LOCAL_MANY_TO_MANY_INVOKE_ADAPTER),
                inputDto, outputDto,
                localInvokeDelegate,
                baseName + ".output", API_VERSION)
            .addStatement("$T<$T, $T> invokeRemote = new $T<>()",
                FUNCTION_INVOKE_ADAPTER, inputDto, outputDto, HTTP_REMOTE_INVOKE_ADAPTER)
            .addStatement("$T<$T, $T> invoke = new $T<>(invokeLocal, invokeRemote)",
                FUNCTION_INVOKE_ADAPTER, inputDto, outputDto, INVOCATION_MODE_ROUTING_INVOKE_ADAPTER)
            .addStatement("$T<$T, $T> sink = new $T<>()",
                FUNCTION_SINK_ADAPTER, outputDto, handlerOutputType,
                streamingOutput ? COLLECT_LIST_SINK_ADAPTER : DEFAULT_UNARY_SINK_ADAPTER)
            .addStatement("$T result = $T.$L(input, transportContext, source, invoke, sink)",
                resultType,
                shape == StreamingShape.UNARY_UNARY ? UNARY_FUNCTION_TRANSPORT_BRIDGE : FUNCTION_TRANSPORT_BRIDGE,
                bridgeMethodName(shape))
            .addStatement("response.getWriter().write(mapper.writeValueAsString(result))")
            .addStatement("response.setStatusCode(200)")
            .nextControlFlow("catch (Exception e)")
            .addStatement("response.setStatusCode(500)")
            .addStatement("response.getWriter().write(mapper.writeValueAsString($T.of(\"error\", e.getMessage())))",
                ClassName.get("java.util", "Map"))
            .endControlFlow()
            .build();
    }

    /**
     * Produces a Java expression that yields the execution id for FunctionTransportContext, using the subclass-provided execution id expression when non-blank and falling back to java.util.UUID.randomUUID().toString().
     *
     * @return a Java expression string that evaluates to the execution id
     */
    protected CodeBlock buildExecutionIdExpression() {
        CodeBlock executionIdExpr = getExecutionIdExpression();
        if (executionIdExpr != null && !executionIdExpr.toString().isBlank()) {
            return CodeBlock.builder()
                .add("((")
                .add(executionIdExpr)
                .add(") != null && !(")
                .add(executionIdExpr)
                .add(").isBlank()) ? (")
                .add(executionIdExpr)
                .add(") : $T.randomUUID().toString()", ClassName.get("java.util", "UUID"))
                .build();
        }
        return CodeBlock.of("$T.randomUUID().toString()", ClassName.get("java.util", "UUID"));
    }

    protected CodeBlock buildTransportContextCodeBlock() {
        CodeBlock.Builder builder = CodeBlock.builder();
        builder.add("$T transportContext = $T.of(", FUNCTION_TRANSPORT_CONTEXT, FUNCTION_TRANSPORT_CONTEXT);
        builder.add(getRequestIdExpression());
        builder.add(", ");
        builder.add(getFunctionNameExpression());
        builder.add(", $S, $T.of(", INVOKE_STEP, ClassName.get("java.util", "Map"));
        builder.add("$T.ATTR_CORRELATION_ID, ", FUNCTION_TRANSPORT_CONTEXT);
        builder.add(getRequestIdExpression());
        builder.add(", ");
        builder.add("$T.ATTR_EXECUTION_ID, ", FUNCTION_TRANSPORT_CONTEXT);
        builder.add(buildExecutionIdExpression());
        builder.add(", ");
        builder.add(
            "$T.ATTR_RETRY_ATTEMPT, $T.getProperty($S, $S), ",
            FUNCTION_TRANSPORT_CONTEXT,
            ClassName.get(System.class),
            "tpf.transport.retry-attempt",
            "0");
        builder.add(
            "$T.ATTR_DISPATCH_TS_EPOCH_MS, $T.toString($T.currentTimeMillis()))",
            FUNCTION_TRANSPORT_CONTEXT,
            ClassName.get(Long.class),
            ClassName.get(System.class));
        return builder.build();
    }

    /**
     * Provide the JavaPoet format fragment used as a fallback execution-id expression.
     *
     * @return a JavaPoet format string representing `java.util.UUID`
     */
    protected String buildExecutionIdFallbackExpression() {
        return "java.util.UUID";
    }

    /**
     * Creates a JavaPoet format string fragment for instantiating a FunctionTransportContext with placeholders for request id, function name, stage, and transport attributes.
     *
     * @deprecated Inline context construction is performed in buildHandlerMethod; this template is retained only for backwards compatibility.
     * @return a JavaPoet-compatible format string that, when rendered, produces code to construct a `FunctionTransportContext` including protocol, correlation id, execution id (with UUID fallback), retry-attempt, and dispatch timestamp attributes
     */
    @Deprecated
    protected String buildTransportContextStatement() {
        return buildTransportContextCodeBlock().toString();
    }

    /**
         * Provider-specific suffix appended to generated handler class names.
         *
         * @return the handler class name suffix, for example "FunctionHandler"
         */
    protected String getHandlerSuffix() {
        return "FunctionHandler";
    }

    /**
     * Convert a domain type representation to the corresponding DTO type representation.
     *
     * @param domainType the domain TypeName to convert; may be null
     * @return the DTO TypeName corresponding to the domain type, or `ClassName.OBJECT` if `domainType` is null
     * @throws IllegalArgumentException if the domain type is a type variable, wildcard, primitive, or a parameterized type whose raw type cannot be resolved to a concrete class
     * @throws IllegalStateException if an unexpected TypeName implementation is encountered
     */
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

    /**
     * Rewrite a package name by replacing the first "domain" or "service" segment with "dto".
     *
     * @param packageName the original package name; may be null or blank
     * @return the rewritten package name with the matched segment replaced by "dto", or an empty string if {@code packageName} is null or blank
     * @throws IllegalArgumentException if neither "domain" nor "service" appears as a segment in {@code packageName}
     */
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
     * Compute the fully qualified class name of the generated REST function handler.
     *
     * @param servicePackage the base package of the service
     * @param generatedName the generated service class name (may include "Service" or "Reactive" suffix)
     * @return the fully qualified class name of the generated handler
     */
    public String handlerFqcn(String servicePackage, String generatedName) {
        String baseName = removeSuffix(removeSuffix(generatedName, "Service"), "Reactive");
        return servicePackage + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX + "." + baseName + getHandlerSuffix();
    }

    /**
     * Selects the appropriate local invoke adapter ClassName for the given streaming shape.
     *
     * @param shape the streaming shape that determines which adapter to use
     * @param localInvokeAdapter adapter for UNARY_UNARY shape
     * @param localOneToManyInvokeAdapter adapter for UNARY_STREAMING shape
     * @param localManyToOneInvokeAdapter adapter for STREAMING_UNARY shape
     * @param localManyToManyInvokeAdapter adapter for STREAMING_STREAMING shape
     * @return the ClassName of the adapter that corresponds to the provided streaming shape
     */
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

    /**
     * Selects the transport bridge invocation format string that matches the given streaming shape.
     *
     * @param shape the streaming shape of the function invocation
     * @return a JavaPoet format string for invoking the appropriate bridge method:
     *         "`invoke`" for UNARY_UNARY, "`invokeOneToMany`" for UNARY_STREAMING,
     *         "`invokeManyToOne`" for STREAMING_UNARY, and "`invokeManyToMany`" for STREAMING_STREAMING.
     */
    private static String bridgeInvocationFormat(StreamingShape shape) {
        return switch (shape) {
            case UNARY_UNARY -> "return $T.invoke(input, transportContext, source, invoke, sink)";
            case UNARY_STREAMING -> "return $T.invokeOneToMany(input, transportContext, source, invoke, sink)";
            case STREAMING_UNARY -> "return $T.invokeManyToOne(input, transportContext, source, invoke, sink)";
            case STREAMING_STREAMING -> "return $T.invokeManyToMany(input, transportContext, source, invoke, sink)";
        };
    }

    /**
     * Returns the bridge method name for the given streaming shape.
     *
     * @param shape the streaming shape
     * @return the bridge method name
     */
    protected static String bridgeMethodName(StreamingShape shape) {
        return switch (shape) {
            case UNARY_UNARY -> "invoke";
            case UNARY_STREAMING -> "invokeOneToMany";
            case STREAMING_UNARY -> "invokeManyToOne";
            case STREAMING_STREAMING -> "invokeManyToMany";
        };
    }

    /**
     * Selects the local invocation delegate expression corresponding to the provided streaming shape.
     *
     * @param shape the streaming shape of the pipeline step
     * @return {@code "resource::process"} for {@code UNARY_UNARY}, {@code UNARY_STREAMING}, and {@code STREAMING_STREAMING}; otherwise the lambda
     *         {@code "inputStream -> inputStream.collect().asList().onItem().transformToUni(resource::process)"} for {@code STREAMING_UNARY}
     */
    private static String localInvokeDelegate(StreamingShape shape) {
        return switch (shape) {
            case UNARY_UNARY, UNARY_STREAMING, STREAMING_STREAMING -> "resource::process";
            case STREAMING_UNARY ->
                "inputStream -> inputStream.collect().asList().onItem().transformToUni(resource::process)";
        };
    }

    /**
     * Removes the specified suffix from the given string if it is present.
     *
     * @param value  the input string, may be {@code null}
     * @param suffix the suffix to remove; if {@code null} or blank no removal is attempted
     * @return the input string with the suffix removed if it ended with the suffix; otherwise the original string (may be {@code null})
     */
    private static String removeSuffix(String value, String suffix) {
        if (value == null || suffix == null || suffix.isBlank()) {
            return value;
        }
        return value.endsWith(suffix) ? value.substring(0, value.length() - suffix.length()) : value;
    }
}
