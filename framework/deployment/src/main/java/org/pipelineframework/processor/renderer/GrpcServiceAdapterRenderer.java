package org.pipelineframework.processor.renderer;

import java.io.IOException;
import javax.annotation.processing.Messager;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.*;
import io.quarkus.arc.Unremovable;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.processor.PipelineStepProcessor;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.GrpcBinding;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.util.GrpcJavaTypeResolver;

/**
 * Renderer for gRPC service adapters based on PipelineStepModel and GrpcBinding.
 * Supports both regular gRPC services and plugin gRPC services
 *
 * @param target The generation target for this renderer
 */
public record GrpcServiceAdapterRenderer(GenerationTarget target) implements PipelineRenderer<GrpcBinding> {
    private static final GrpcJavaTypeResolver GRPC_TYPE_RESOLVER = new GrpcJavaTypeResolver();

    /**
     * Generate and write a gRPC service adapter class for the provided binding into the generation context.
     *
     * The generated Java file is placed in the package formed by binding.servicePackage() plus the pipeline suffix
     * and written to the writer supplied by the generation context.
     *
     * @param binding the gRPC binding describing the service and its pipeline step model
     * @param ctx the generation context providing output directories and processing environment
     * @throws IOException if an error occurs while writing the generated file
     */
    @Override
    public void render(GrpcBinding binding, GenerationContext ctx) throws IOException {
        TypeSpec grpcServiceClass = buildGrpcServiceClass(binding, ctx.processingEnv().getMessager(), ctx.role());

        // Write the generated class
        JavaFile javaFile = JavaFile.builder(
                        binding.servicePackage() + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX,
                        grpcServiceClass)
                .build();

        javaFile.writeTo(ctx.outputDir());
    }

    /**
     * Builds a JavaPoet TypeSpec for the gRPC service adapter class corresponding to the given binding.
     *
     * The generated class is annotated as a gRPC service, registered as a singleton and unremovable,
     * given a generated role indicating a pipeline server, extends the resolved gRPC base class,
     * conditionally declares injected mapper fields, and implements the appropriate remote method
     * for the model's streaming shape.
     *
     * @param binding   the gRPC binding containing the pipeline step model and service metadata used to generate the class
     * @param messager  a Messager for reporting diagnostics and assisting type resolution during generation
     * @return          a TypeSpec representing the complete gRPC service adapter class to be written to a Java file
     */
    private TypeSpec buildGrpcServiceClass(GrpcBinding binding, Messager messager, org.pipelineframework.processor.ir.DeploymentRole role) {
        PipelineStepModel model = binding.model();
        String simpleClassName;
        // For gRPC services: ${ServiceName}GrpcService
        simpleClassName = model.generatedName() + PipelineStepProcessor.GRPC_SERVICE_SUFFIX;

        // Determine the appropriate gRPC service base class based on configuration
        ClassName grpcBaseClassName = determineGrpcBaseClass(binding, messager);

        TypeSpec.Builder grpcServiceBuilder = TypeSpec.classBuilder(simpleClassName)
                .addModifiers(Modifier.PUBLIC)
                .addAnnotation(AnnotationSpec.builder(ClassName.get(GrpcService.class)).build())
                .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.inject", "Singleton")).build())
                .addAnnotation(AnnotationSpec.builder(Unremovable.class).build())
                // Add the GeneratedRole annotation to indicate the target role
                .addAnnotation(AnnotationSpec.builder(ClassName.get("org.pipelineframework.annotation", "GeneratedRole"))
                        .addMember("value", "$T.$L",
                            ClassName.get("org.pipelineframework.annotation.GeneratedRole", "Role"),
                            role.name())
                        .build())
                .superclass(grpcBaseClassName); // Extend the actual gRPC service base class

        // Add mapper fields with @Inject if they exist
        if (model.inputMapping().hasMapper()) {
            FieldSpec inboundMapperField = FieldSpec.builder(
                            model.inputMapping().mapperType(),
                            "inboundMapper")
                    .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.inject", "Inject")).build())
                    .build();
            grpcServiceBuilder.addField(inboundMapperField);
        }

        if (model.outputMapping().hasMapper()) {
            FieldSpec outboundMapperField = FieldSpec.builder(
                            model.outputMapping().mapperType(),
                            "outboundMapper")
                    .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.inject", "Inject")).build())
                    .build();
            grpcServiceBuilder.addField(outboundMapperField);
        }

        TypeName serviceType = resolveServiceType(model);

        FieldSpec serviceField = FieldSpec.builder(serviceType, "service")
                .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.inject", "Inject")).build())
                .build();
        grpcServiceBuilder.addField(serviceField);

        // Add the required gRPC service method implementation based on streaming shape
        switch (model.streamingShape()) {
            case UNARY_UNARY:
                addUnaryUnaryMethod(grpcServiceBuilder, binding, messager);
                break;
            case UNARY_STREAMING:
                addUnaryStreamingMethod(grpcServiceBuilder, binding, messager);
                break;
            case STREAMING_UNARY:
                addStreamingUnaryMethod(grpcServiceBuilder, binding, messager);
                break;
            case STREAMING_STREAMING:
                addStreamingStreamingMethod(grpcServiceBuilder, binding, messager);
                break;
        }

        return grpcServiceBuilder.build();
    }

    /**
     * Resolve the gRPC implementation base class for the given binding.
     *
     * @param binding the gRPC binding containing pipeline and service metadata
     * @param messager a compiler messager used for type resolution diagnostics
     * @return the ClassName representing the gRPC implementation base class for the binding
     */
    private ClassName determineGrpcBaseClass(GrpcBinding binding, Messager messager) {
        // Use the new GrpcJavaTypeResolver to determine the gRPC implementation base class
        GrpcJavaTypeResolver.GrpcJavaTypes types = GRPC_TYPE_RESOLVER.resolve(binding, messager);
        return types.implBase();
    }

    /**
     * Add a unary-to-unary gRPC remoteProcess method to the provided class builder for the given binding.
     *
     * The generated method constructs an inline GrpcReactiveServiceAdapter tailored to the binding's
     * gRPC parameter/return types and domain types, and delegates request handling to that adapter.
     *
     * @param builder the JavaPoet TypeSpec.Builder representing the class being generated
     * @param binding provides pipeline step model and gRPC type metadata used to build the method
     * @param messager used to report messages during type resolution
     * @throws IllegalStateException if required gRPC parameter/return types or domain input/output types are missing for the service
     */
    private void addUnaryUnaryMethod(TypeSpec.Builder builder, GrpcBinding binding, Messager messager) {
        PipelineStepModel model = binding.model();
        ClassName grpcAdapterClassName =
                ClassName.get("org.pipelineframework.grpc", "GrpcReactiveServiceAdapter");

        // Use the GrpcJavaTypeResolver to get the proper gRPC types from the binding
        GrpcJavaTypeResolver.GrpcJavaTypes grpcTypes = GRPC_TYPE_RESOLVER.resolve(binding, messager);

        // Validate that required gRPC types are available
        if (grpcTypes.grpcParameterType() == null || grpcTypes.grpcReturnType() == null) {
            throw new IllegalStateException("Missing required gRPC parameter or return type for service: " + binding.serviceName());
        }

        // Create the inline adapter
        TypeSpec inlineAdapter = inlineAdapterBuilder(binding, grpcAdapterClassName, messager);

        TypeName inputDomainTypeUnary = model.inboundDomainType();
        TypeName outputDomainTypeUnary = model.outboundDomainType();

        // Validate that required domain types are available
        if (inputDomainTypeUnary == null || outputDomainTypeUnary == null) {
            throw new IllegalStateException("Missing required domain parameter or return type for service: " + binding.serviceName());
        }

        MethodSpec.Builder remoteProcessMethodBuilder = MethodSpec.methodBuilder("remoteProcess")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(ParameterizedTypeName.get(ClassName.get(Uni.class),
                        grpcTypes.grpcReturnType()))
                .addParameter(grpcTypes.grpcParameterType(), "request")
                .addStatement("$T adapter = $L",
                        ParameterizedTypeName.get(grpcAdapterClassName,
                                grpcTypes.grpcParameterType(),
                                grpcTypes.grpcReturnType(),
                                inputDomainTypeUnary,
                                outputDomainTypeUnary),
                        inlineAdapter)
                .addStatement("long startTime = System.nanoTime()")
                .addCode("""
                    return adapter.remoteProcess($N)
                        .onTermination().invoke((item, failure, cancelled) -> {
                            $T status = cancelled ? $T.CANCELLED
                                : failure != null ? $T.fromThrowable(failure)
                                : $T.OK;
                            $T.recordGrpcServer($S, $S, status, System.nanoTime() - startTime);
                        });
                    """,
                    "request",
                    ClassName.get("io.grpc", "Status"),
                    ClassName.get("io.grpc", "Status"),
                    ClassName.get("io.grpc", "Status"),
                    ClassName.get("io.grpc", "Status"),
                    ClassName.get("org.pipelineframework.telemetry", "RpcMetrics"),
                    model.serviceName(),
                    "remoteProcess");

        // Add @RunOnVirtualThread annotation if the property is enabled
        if (model.executionMode() == org.pipelineframework.processor.ir.ExecutionMode.VIRTUAL_THREADS) {
            remoteProcessMethodBuilder.addAnnotation(
                    ClassName.get("io.smallrye.common.annotation", "RunOnVirtualThread"));
        }

        builder.addMethod(remoteProcessMethodBuilder.build());
    }

    /**
     * Add a unary-to-streaming gRPC service method implementation to the generated class.
     *
     * <p>Constructs and appends a public {@code remoteProcess} method that accepts a single gRPC
     * request and returns a reactive stream of gRPC responses. The method instantiates an inline
     * streaming adapter (mapping between gRPC and domain types) and delegates processing to it.
     * If the step's execution mode is configured for virtual threads, the method is annotated with
     * {@code @RunOnVirtualThread}.</p>
     *
     * @param builder the TypeSpec.Builder for the service class being generated
     * @param binding the gRPC binding containing pipeline and type information for this service
     * @param messager the annotation processing messager used during type resolution
     * @throws IllegalStateException if required gRPC parameter/return types or domain input/output types are missing for the service
     */
    private void addUnaryStreamingMethod(TypeSpec.Builder builder, GrpcBinding binding, Messager messager) {
        PipelineStepModel model = binding.model();
        ClassName grpcAdapterClassName =
                ClassName.get("org.pipelineframework.grpc", "GrpcServiceStreamingAdapter");

        // Use the GrpcJavaTypeResolver to get the proper gRPC types from the binding
        GrpcJavaTypeResolver.GrpcJavaTypes grpcTypes = GRPC_TYPE_RESOLVER.resolve(binding, messager);

        // Validate that required gRPC types are available
        if (grpcTypes.grpcParameterType() == null || grpcTypes.grpcReturnType() == null) {
            throw new IllegalStateException("Missing required gRPC parameter or return type for service: " + binding.serviceName());
        }

        // Create the inline adapter
        TypeSpec inlineAdapter = inlineAdapterBuilder(binding, grpcAdapterClassName, messager);

        TypeName inputDomainTypeUnaryStreaming = model.inboundDomainType();
        TypeName outputDomainTypeUnaryStreaming = model.outboundDomainType();

        // Validate that required domain types are available
        if (inputDomainTypeUnaryStreaming == null || outputDomainTypeUnaryStreaming == null) {
            throw new IllegalStateException("Missing required domain parameter or return type for service: " + binding.serviceName());
        }

        MethodSpec.Builder remoteProcessMethodBuilder = MethodSpec.methodBuilder("remoteProcess")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(ParameterizedTypeName.get(ClassName.get(Multi.class),
                        grpcTypes.grpcReturnType()))
                .addParameter(grpcTypes.grpcParameterType(), "request")
                .addStatement("$T adapter = $L",
                        ParameterizedTypeName.get(grpcAdapterClassName,
                                grpcTypes.grpcParameterType(),
                                grpcTypes.grpcReturnType(),
                                inputDomainTypeUnaryStreaming,
                                outputDomainTypeUnaryStreaming),
                        inlineAdapter)
                .addStatement("long startTime = System.nanoTime()")
                .addCode("""
                    return adapter.remoteProcess($N)
                        .onTermination().invoke((failure, cancelled) -> {
                            $T status = cancelled ? $T.CANCELLED
                                : failure != null ? $T.fromThrowable(failure)
                                : $T.OK;
                            $T.recordGrpcServer($S, $S, status, System.nanoTime() - startTime);
                        });
                    """,
                    "request",
                    ClassName.get("io.grpc", "Status"),
                    ClassName.get("io.grpc", "Status"),
                    ClassName.get("io.grpc", "Status"),
                    ClassName.get("io.grpc", "Status"),
                    ClassName.get("org.pipelineframework.telemetry", "RpcMetrics"),
                    model.serviceName(),
                    "remoteProcess");

        // Add @RunOnVirtualThread annotation if the property is enabled
        if (model.executionMode() == org.pipelineframework.processor.ir.ExecutionMode.VIRTUAL_THREADS) {
            remoteProcessMethodBuilder.addAnnotation(
                    ClassName.get("io.smallrye.common.annotation", "RunOnVirtualThread"));
        }

        builder.addMethod(remoteProcessMethodBuilder.build());
    }

    /**
     * Add the streaming-unary gRPC service method implementation to the provided class builder.
     *
     * Builds and appends a public `remoteProcess(Multi<GrpcRequest>) : Uni<GrpcResponse>` method that
     * instantiates an inline client-streaming adapter bridging gRPC message types and domain types,
     * and delegates processing to that adapter.
     *
     * @param builder the TypeSpec.Builder to which the generated method will be added
     * @param binding source metadata containing the pipeline step model and service name used to
     *                resolve gRPC and domain types
     *
     * @throws IllegalStateException if the binding does not resolve to required gRPC parameter or
     *                               return types, or if the model lacks the necessary inbound or
     *                               outbound domain types
     */
    private void addStreamingUnaryMethod(TypeSpec.Builder builder, GrpcBinding binding, Messager messager) {
        PipelineStepModel model = binding.model();
        ClassName grpcAdapterClassName =
                ClassName.get("org.pipelineframework.grpc", "GrpcServiceClientStreamingAdapter");

        // Use the GrpcJavaTypeResolver to get the proper gRPC types from the binding
        GrpcJavaTypeResolver.GrpcJavaTypes grpcTypes = GRPC_TYPE_RESOLVER.resolve(binding, messager);

        // Validate that required gRPC types are available
        if (grpcTypes.grpcParameterType() == null || grpcTypes.grpcReturnType() == null) {
            throw new IllegalStateException("Missing required gRPC parameter or return type for service: " + binding.serviceName());
        }

        // Create the inline adapter
        TypeSpec inlineAdapter = inlineAdapterBuilder(binding, grpcAdapterClassName, messager);

        TypeName inputDomainTypeStreamingUnary = model.inboundDomainType();
        TypeName outputDomainTypeStreamingUnary = model.outboundDomainType();

        // Validate that required domain types are available
        if (inputDomainTypeStreamingUnary == null || outputDomainTypeStreamingUnary == null) {
            throw new IllegalStateException("Missing required domain parameter or return type for service: " + binding.serviceName());
        }

        MethodSpec.Builder remoteProcessMethodBuilder = MethodSpec.methodBuilder("remoteProcess")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(ParameterizedTypeName.get(ClassName.get(Uni.class),
                        grpcTypes.grpcReturnType()))
                .addParameter(ParameterizedTypeName.get(ClassName.get(Multi.class),
                        grpcTypes.grpcParameterType()), "request")
                .addStatement("$T adapter = $L",
                        ParameterizedTypeName.get(grpcAdapterClassName,
                                grpcTypes.grpcParameterType(),
                                grpcTypes.grpcReturnType(),
                                inputDomainTypeStreamingUnary,
                                outputDomainTypeStreamingUnary),
                        inlineAdapter)
                .addStatement("long startTime = System.nanoTime()")
                .addCode("""
                    return adapter.remoteProcess($N)
                        .onTermination().invoke((item, failure, cancelled) -> {
                            $T status = cancelled ? $T.CANCELLED
                                : failure != null ? $T.fromThrowable(failure)
                                : $T.OK;
                            $T.recordGrpcServer($S, $S, status, System.nanoTime() - startTime);
                        });
                    """,
                    "request",
                    ClassName.get("io.grpc", "Status"),
                    ClassName.get("io.grpc", "Status"),
                    ClassName.get("io.grpc", "Status"),
                    ClassName.get("io.grpc", "Status"),
                    ClassName.get("org.pipelineframework.telemetry", "RpcMetrics"),
                    model.serviceName(),
                    "remoteProcess");

        // Add @RunOnVirtualThread annotation if the property is enabled
        if (model.executionMode() == org.pipelineframework.processor.ir.ExecutionMode.VIRTUAL_THREADS) {
            remoteProcessMethodBuilder.addAnnotation(
                    ClassName.get("io.smallrye.common.annotation", "RunOnVirtualThread"));
        }

        builder.addMethod(remoteProcessMethodBuilder.build());
    }

    /**
     * Adds a bidirectional gRPC `remoteProcess` method to the generated service class.
     *
     * The generated method overrides `remoteProcess`, accepts a `Multi` of gRPC request messages,
     * and returns a `Multi` of gRPC response messages by delegating to an inline streaming adapter.
     *
     * @param builder the TypeSpec builder for the service class being generated
     * @param binding the gRPC binding containing the pipeline step model and service metadata
     * @param messager a processing messager used for type resolution diagnostics
     * @throws IllegalStateException if required gRPC parameter/return types or domain types are missing for the service
     */
    private void addStreamingStreamingMethod(TypeSpec.Builder builder, GrpcBinding binding, Messager messager) {
        PipelineStepModel model = binding.model();
        ClassName grpcAdapterClassName =
                ClassName.get("org.pipelineframework.grpc", "GrpcServiceBidirectionalStreamingAdapter");

        // Use the GrpcJavaTypeResolver to get the proper gRPC types from the binding
        GrpcJavaTypeResolver.GrpcJavaTypes grpcTypes = GRPC_TYPE_RESOLVER.resolve(binding, messager);

        // Validate that required gRPC types are available
        if (grpcTypes.grpcParameterType() == null || grpcTypes.grpcReturnType() == null) {
            throw new IllegalStateException("Missing required gRPC parameter or return type for service: " + binding.serviceName());
        }

        // Create the inline adapter
        TypeSpec inlineAdapterStreaming = inlineAdapterBuilder(binding, grpcAdapterClassName, messager);

        TypeName inputDomainTypeStreamingStreaming = model.inboundDomainType();
        TypeName outputDomainTypeStreamingStreaming = model.outboundDomainType();

        // Validate that required domain types are available
        if (inputDomainTypeStreamingStreaming == null || outputDomainTypeStreamingStreaming == null) {
            throw new IllegalStateException("Missing required domain parameter or return type for service: " + binding.serviceName());
        }

        MethodSpec.Builder remoteProcessMethodBuilder = MethodSpec.methodBuilder("remoteProcess")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(ParameterizedTypeName.get(ClassName.get(Multi.class),
                        grpcTypes.grpcReturnType()))
                .addParameter(ParameterizedTypeName.get(ClassName.get(Multi.class),
                        grpcTypes.grpcParameterType()), "request")
                .addStatement("$T adapter = $L",
                        ParameterizedTypeName.get(grpcAdapterClassName,
                                grpcTypes.grpcParameterType(),
                                grpcTypes.grpcReturnType(),
                                inputDomainTypeStreamingStreaming,
                                outputDomainTypeStreamingStreaming),
                        inlineAdapterStreaming)
                .addStatement("long startTime = System.nanoTime()")
                .addCode("""
                    return adapter.remoteProcess($N)
                        .onTermination().invoke((failure, cancelled) -> {
                            $T status = cancelled ? $T.CANCELLED
                                : failure != null ? $T.fromThrowable(failure)
                                : $T.OK;
                            $T.recordGrpcServer($S, $S, status, System.nanoTime() - startTime);
                        });
                    """,
                    "request",
                    ClassName.get("io.grpc", "Status"),
                    ClassName.get("io.grpc", "Status"),
                    ClassName.get("io.grpc", "Status"),
                    ClassName.get("io.grpc", "Status"),
                    ClassName.get("org.pipelineframework.telemetry", "RpcMetrics"),
                    model.serviceName(),
                    "remoteProcess");

        // Add @RunOnVirtualThread annotation if the property is enabled
        if (model.executionMode() == org.pipelineframework.processor.ir.ExecutionMode.VIRTUAL_THREADS) {
            remoteProcessMethodBuilder.addAnnotation(
                    ClassName.get("io.smallrye.common.annotation", "RunOnVirtualThread"));
        }

        builder.addMethod(remoteProcessMethodBuilder.build());
    }

    /**
     * Builds an anonymous subclass of the given gRPC adapter type that bridges between gRPC DTOs and domain types.
     *
     * @param binding               the gRPC binding containing the pipeline step model and service metadata
     * @param grpcAdapterClassName  the adapter base class to extend (parameterised with input/output gRPC and domain types)
     * @param messager              a Messager passed to the type resolver for diagnostics
     * @return                      a TypeSpec for an anonymous class implementing `getService`, `fromGrpc` and `toGrpc`
     * @throws IllegalStateException if required gRPC parameter/return types or domain input/output types are missing for the binding
     */
    private TypeSpec inlineAdapterBuilder(
            GrpcBinding binding,
            ClassName grpcAdapterClassName,
            Messager messager
    ) {
        PipelineStepModel model = binding.model();

        // Use the GrpcJavaTypeResolver to get the proper gRPC types from the binding
        GrpcJavaTypeResolver.GrpcJavaTypes grpcTypes = GRPC_TYPE_RESOLVER.resolve(binding, messager);

        // Validate that required gRPC types are available
        if (grpcTypes.grpcParameterType() == null || grpcTypes.grpcReturnType() == null) {
            throw new IllegalStateException("Missing required gRPC parameter or return type for service: " + binding.serviceName());
        }

        TypeName inputGrpcType = grpcTypes.grpcParameterType(); // Get the correct input gRPC type
        TypeName outputGrpcType = grpcTypes.grpcReturnType(); // Get the correct output gRPC type
        TypeName inputDomainType = model.inboundDomainType();
        TypeName outputDomainType = model.outboundDomainType();

        // Validate that required domain types are available
        if (inputDomainType == null || outputDomainType == null) {
            throw new IllegalStateException("Missing required domain parameter or return type for service: " + binding.serviceName());
        }

        boolean hasInboundMapper = model.inputMapping().hasMapper();
        boolean hasOutboundMapper = model.outputMapping().hasMapper();

        MethodSpec.Builder fromGrpcMethodBuilder = MethodSpec.methodBuilder("fromGrpc")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PROTECTED)
                .returns(inputDomainType)
                .addParameter(inputGrpcType, "grpcIn");
        if (hasInboundMapper) {
            fromGrpcMethodBuilder.addStatement("return inboundMapper.fromGrpcFromDto(grpcIn)");
        } else {
            fromGrpcMethodBuilder.addStatement("return ($T) grpcIn", inputDomainType);
        }

        MethodSpec.Builder toGrpcMethodBuilder = MethodSpec.methodBuilder("toGrpc")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PROTECTED)
                .returns(outputGrpcType)
                .addParameter(outputDomainType, "output");
        if (hasOutboundMapper) {
            toGrpcMethodBuilder.addStatement("return outboundMapper.toDtoToGrpc(output)");
        } else {
            toGrpcMethodBuilder.addStatement("return ($T) output", outputGrpcType);
        }

        MethodSpec.Builder getServiceBuilder = MethodSpec.methodBuilder("getService")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PROTECTED)
                .returns(resolveServiceType(model));

        getServiceBuilder.addStatement("return service");

        return TypeSpec.anonymousClassBuilder("")
                .superclass(ParameterizedTypeName.get(
                        grpcAdapterClassName,
                        inputGrpcType,
                        outputGrpcType,
                        inputDomainType,
                        outputDomainType
                ))
                .addMethod(getServiceBuilder.build())
                .addMethod(fromGrpcMethodBuilder.build())
                .addMethod(toGrpcMethodBuilder.build())
                .build();
    }

    private TypeName resolveServiceType(PipelineStepModel model) {
        if (!model.sideEffect()) {
            return model.serviceClassName();
        }
        return ClassName.get(
            model.servicePackage() + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX,
            model.serviceName());
    }

}
