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
     * Adds a unary-to-unary gRPC `remoteProcess` method to the generated class for the given binding.
     *
     * The generated method delegates request handling to an inline `GrpcReactiveServiceAdapter` adapted to the
     * binding's gRPC parameter/return and domain types.
     *
     * @param builder the JavaPoet TypeSpec.Builder for the class being generated
     * @param binding supplies the PipelineStepModel and gRPC binding metadata used to build the method
     * @param messager used to report messages during gRPC type resolution
     * @throws IllegalStateException if required gRPC parameter/return types or required domain input/output types are missing for the service
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

        boolean cacheSideEffect = isCacheSideEffect(model);
        TypeName inputDomainTypeUnary = cacheSideEffect
            ? grpcTypes.grpcParameterType()
            : model.inboundDomainType();
        TypeName outputDomainTypeUnary = cacheSideEffect
            ? grpcTypes.grpcReturnType()
            : model.outboundDomainType();

        // Validate that required domain types are available
        if (!cacheSideEffect && (inputDomainTypeUnary == null || outputDomainTypeUnary == null)) {
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
     * Generate and append a public gRPC method named `remoteProcess` that accepts a single gRPC request
     * and returns a reactive stream of gRPC responses for a unary-to-streaming pipeline step.
     *
     * The generated method delegates processing to an inline streaming adapter and records RPC
     * telemetry on termination. If the step's execution mode is set to virtual threads, the method
     * is annotated with `@RunOnVirtualThread`.
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

        boolean cacheSideEffect = isCacheSideEffect(model);
        TypeName inputDomainTypeUnaryStreaming = cacheSideEffect
            ? grpcTypes.grpcParameterType()
            : model.inboundDomainType();
        TypeName outputDomainTypeUnaryStreaming = cacheSideEffect
            ? grpcTypes.grpcReturnType()
            : model.outboundDomainType();

        // Validate that required domain types are available
        if (!cacheSideEffect && (inputDomainTypeUnaryStreaming == null || outputDomainTypeUnaryStreaming == null)) {
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
     * Adds a public gRPC client-streaming (streaming→unary) `remoteProcess(Multi<Req>) : Uni<Resp>` method to the given class builder.
     *
     * The generated method instantiates an inline client-streaming adapter that bridges gRPC DTOs and domain types, delegates the incoming request stream to that adapter, and records RPC telemetry on termination.
     *
     * @param builder the TypeSpec.Builder to which the generated method will be added
     * @param binding source metadata containing the pipeline step model and service name used to resolve gRPC and domain types
     * @throws IllegalStateException if required gRPC parameter/return types or (when not a cache-side-effect) inbound/outbound domain types are missing for the binding's service
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

        boolean cacheSideEffect = isCacheSideEffect(model);
        TypeName inputDomainTypeStreamingUnary = cacheSideEffect
            ? grpcTypes.grpcParameterType()
            : model.inboundDomainType();
        TypeName outputDomainTypeStreamingUnary = cacheSideEffect
            ? grpcTypes.grpcReturnType()
            : model.outboundDomainType();

        // Validate that required domain types are available
        if (!cacheSideEffect && (inputDomainTypeStreamingUnary == null || outputDomainTypeStreamingUnary == null)) {
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

        boolean cacheSideEffect = isCacheSideEffect(model);
        TypeName inputDomainTypeStreamingStreaming = cacheSideEffect
            ? grpcTypes.grpcParameterType()
            : model.inboundDomainType();
        TypeName outputDomainTypeStreamingStreaming = cacheSideEffect
            ? grpcTypes.grpcReturnType()
            : model.outboundDomainType();

        // Validate that required domain types are available
        if (!cacheSideEffect && (inputDomainTypeStreamingStreaming == null || outputDomainTypeStreamingStreaming == null)) {
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
         * Create an anonymous subclass of the specified gRPC adapter that bridges between gRPC DTO types and domain types.
         *
         * @param binding              the gRPC binding containing the pipeline step model and service metadata
         * @param grpcAdapterClassName the adapter base class to extend (parameterized with input/output gRPC and domain types)
         * @param messager             a Messager used by the type resolver for diagnostics
         * @return                     a TypeSpec for an anonymous class that implements `getService`, `fromGrpc`, and `toGrpc`
         * @throws IllegalStateException if required gRPC parameter/return types or required domain input/output types are missing for the binding
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
        boolean cacheSideEffect = isCacheSideEffect(model);
        TypeName inputDomainType = cacheSideEffect ? inputGrpcType : model.inboundDomainType();
        TypeName outputDomainType = cacheSideEffect ? outputGrpcType : model.outboundDomainType();

        // Validate that required domain types are available
        if (!cacheSideEffect && (inputDomainType == null || outputDomainType == null)) {
            throw new IllegalStateException("Missing required domain parameter or return type for service: " + binding.serviceName());
        }

        boolean hasInboundMapper = !cacheSideEffect && model.inputMapping().hasMapper();
        boolean hasOutboundMapper = !cacheSideEffect && model.outputMapping().hasMapper();

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

    /**
     * Determine the service type to use for the pipeline step.
     *
     * @param model the pipeline step model to inspect
     * @return the service TypeName: the model's declared service class when the step is not a side effect;
     *         otherwise the generated pipeline service class located in the model's package plus the pipeline package suffix
     */
    private TypeName resolveServiceType(PipelineStepModel model) {
        if (!model.sideEffect()) {
            return model.serviceClassName();
        }
        return ClassName.get(
            model.servicePackage() + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX,
            model.serviceName());
    }

    /**
     * Determines whether the given pipeline step represents a cache-backed side effect.
     *
     * @param model the pipeline step model to inspect
     * @return `true` if the model indicates a side effect implemented by
     *         `org.pipelineframework.plugin.cache.CacheService`, `false` otherwise
     */
    private boolean isCacheSideEffect(PipelineStepModel model) {
        if (model == null || !model.sideEffect() || model.serviceClassName() == null) {
            return false;
        }
        return "org.pipelineframework.plugin.cache.CacheService".equals(
            model.serviceClassName().canonicalName());
    }

}