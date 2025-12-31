package org.pipelineframework.processor.renderer;

import java.io.IOException;
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

    @Override
    public void render(GrpcBinding binding, GenerationContext ctx) throws IOException {
        TypeSpec grpcServiceClass = buildGrpcServiceClass(binding);

        // Write the generated class
        JavaFile javaFile = JavaFile.builder(
                        binding.servicePackage() + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX,
                        grpcServiceClass)
                .build();

        try (var writer = ctx.builderFile().openWriter()) {
            javaFile.writeTo(writer);
        }
    }

    private TypeSpec buildGrpcServiceClass(GrpcBinding binding) {
        PipelineStepModel model = binding.model();
        String simpleClassName;
        // For gRPC services: ${ServiceName}GrpcService
        simpleClassName = binding.serviceName() + PipelineStepProcessor.GRPC_SERVICE_SUFFIX;

        // Determine the appropriate gRPC service base class based on configuration
        ClassName grpcBaseClassName = determineGrpcBaseClass(binding);

        TypeSpec.Builder grpcServiceBuilder = TypeSpec.classBuilder(simpleClassName)
                .addModifiers(Modifier.PUBLIC)
                .addAnnotation(AnnotationSpec.builder(ClassName.get(GrpcService.class)).build())
                .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.inject", "Singleton")).build())
                .addAnnotation(AnnotationSpec.builder(Unremovable.class).build())
                // Add the GeneratedRole annotation to indicate this is a pipeline server
                .addAnnotation(AnnotationSpec.builder(ClassName.get("org.pipelineframework.annotation", "GeneratedRole"))
                        .addMember("value", "$T.$L",
                            ClassName.get("org.pipelineframework.annotation.GeneratedRole", "Role"),
                            determineRoleForGrpcService(binding))
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

        TypeName serviceType = model.serviceClassName();

        FieldSpec serviceField = FieldSpec.builder(
                        serviceType,
                        "service")
                .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.inject", "Inject")).build())
                .build();
        grpcServiceBuilder.addField(serviceField);

        // Add the required gRPC service method implementation based on streaming shape
        switch (model.streamingShape()) {
            case UNARY_UNARY:
                addUnaryUnaryMethod(grpcServiceBuilder, binding);
                break;
            case UNARY_STREAMING:
                addUnaryStreamingMethod(grpcServiceBuilder, binding);
                break;
            case STREAMING_UNARY:
                addStreamingUnaryMethod(grpcServiceBuilder, binding);
                break;
            case STREAMING_STREAMING:
                addStreamingStreamingMethod(grpcServiceBuilder, binding);
                break;
        }

        return grpcServiceBuilder.build();
    }

    private ClassName determineGrpcBaseClass(GrpcBinding binding) {
        // Use the new GrpcJavaTypeResolver to determine the gRPC implementation base class
        GrpcJavaTypeResolver grpcTypeResolver = new GrpcJavaTypeResolver();
        GrpcJavaTypeResolver.GrpcJavaTypes types = grpcTypeResolver.resolve(binding);
        return types.implBase();
    }

    private void addUnaryUnaryMethod(TypeSpec.Builder builder, GrpcBinding binding) {
        PipelineStepModel model = binding.model();
        ClassName grpcAdapterClassName =
                ClassName.get("org.pipelineframework.grpc", "GrpcReactiveServiceAdapter");

        // Use the GrpcJavaTypeResolver to get the proper gRPC types from the binding
        GrpcJavaTypeResolver grpcTypeResolver = new GrpcJavaTypeResolver();
        GrpcJavaTypeResolver.GrpcJavaTypes grpcTypes = grpcTypeResolver.resolve(binding);

        // Validate that required gRPC types are available
        if (grpcTypes.grpcParameterType() == null || grpcTypes.grpcReturnType() == null) {
            throw new IllegalStateException("Missing required gRPC parameter or return type for service: " + binding.serviceName());
        }

        // Create the inline adapter
        TypeSpec inlineAdapter = inlineAdapterBuilder(binding, grpcAdapterClassName);

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
                .addStatement("return adapter.remoteProcess($N)", "request");

        // Add @RunOnVirtualThread annotation if the property is enabled
        if (model.executionMode() == org.pipelineframework.processor.ir.ExecutionMode.VIRTUAL_THREADS) {
            remoteProcessMethodBuilder.addAnnotation(
                    ClassName.get("io.smallrye.common.annotation", "RunOnVirtualThread"));
        }

        builder.addMethod(remoteProcessMethodBuilder.build());
    }

    private void addUnaryStreamingMethod(TypeSpec.Builder builder, GrpcBinding binding) {
        PipelineStepModel model = binding.model();
        ClassName grpcAdapterClassName =
                ClassName.get("org.pipelineframework.grpc", "GrpcServiceStreamingAdapter");

        // Use the GrpcJavaTypeResolver to get the proper gRPC types from the binding
        GrpcJavaTypeResolver grpcTypeResolver = new GrpcJavaTypeResolver();
        GrpcJavaTypeResolver.GrpcJavaTypes grpcTypes = grpcTypeResolver.resolve(binding);

        // Validate that required gRPC types are available
        if (grpcTypes.grpcParameterType() == null || grpcTypes.grpcReturnType() == null) {
            throw new IllegalStateException("Missing required gRPC parameter or return type for service: " + binding.serviceName());
        }

        // Create the inline adapter
        TypeSpec inlineAdapter = inlineAdapterBuilder(binding, grpcAdapterClassName);

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
                .addStatement("return adapter.remoteProcess($N)", "request");

        // Add @RunOnVirtualThread annotation if the property is enabled
        if (model.executionMode() == org.pipelineframework.processor.ir.ExecutionMode.VIRTUAL_THREADS) {
            remoteProcessMethodBuilder.addAnnotation(
                    ClassName.get("io.smallrye.common.annotation", "RunOnVirtualThread"));
        }

        builder.addMethod(remoteProcessMethodBuilder.build());
    }

    private void addStreamingUnaryMethod(TypeSpec.Builder builder, GrpcBinding binding) {
        PipelineStepModel model = binding.model();
        ClassName grpcAdapterClassName =
                ClassName.get("org.pipelineframework.grpc", "GrpcServiceClientStreamingAdapter");

        // Use the GrpcJavaTypeResolver to get the proper gRPC types from the binding
        GrpcJavaTypeResolver grpcTypeResolver = new GrpcJavaTypeResolver();
        GrpcJavaTypeResolver.GrpcJavaTypes grpcTypes = grpcTypeResolver.resolve(binding);

        // Validate that required gRPC types are available
        if (grpcTypes.grpcParameterType() == null || grpcTypes.grpcReturnType() == null) {
            throw new IllegalStateException("Missing required gRPC parameter or return type for service: " + binding.serviceName());
        }

        // Create the inline adapter
        TypeSpec inlineAdapter = inlineAdapterBuilder(binding, grpcAdapterClassName);

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
                .addStatement("return adapter.remoteProcess($N)", "request");

        // Add @RunOnVirtualThread annotation if the property is enabled
        if (model.executionMode() == org.pipelineframework.processor.ir.ExecutionMode.VIRTUAL_THREADS) {
            remoteProcessMethodBuilder.addAnnotation(
                    ClassName.get("io.smallrye.common.annotation", "RunOnVirtualThread"));
        }

        builder.addMethod(remoteProcessMethodBuilder.build());
    }

    private void addStreamingStreamingMethod(TypeSpec.Builder builder, GrpcBinding binding) {
        PipelineStepModel model = binding.model();
        ClassName grpcAdapterClassName =
                ClassName.get("org.pipelineframework.grpc", "GrpcServiceBidirectionalStreamingAdapter");

        // Use the GrpcJavaTypeResolver to get the proper gRPC types from the binding
        GrpcJavaTypeResolver grpcTypeResolver = new GrpcJavaTypeResolver();
        GrpcJavaTypeResolver.GrpcJavaTypes grpcTypes = grpcTypeResolver.resolve(binding);

        // Validate that required gRPC types are available
        if (grpcTypes.grpcParameterType() == null || grpcTypes.grpcReturnType() == null) {
            throw new IllegalStateException("Missing required gRPC parameter or return type for service: " + binding.serviceName());
        }

        // Create the inline adapter
        TypeSpec inlineAdapterStreaming = inlineAdapterBuilder(binding, grpcAdapterClassName);

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
                .addStatement("return adapter.remoteProcess($N)", "request");

        // Add @RunOnVirtualThread annotation if the property is enabled
        if (model.executionMode() == org.pipelineframework.processor.ir.ExecutionMode.VIRTUAL_THREADS) {
            remoteProcessMethodBuilder.addAnnotation(
                    ClassName.get("io.smallrye.common.annotation", "RunOnVirtualThread"));
        }

        builder.addMethod(remoteProcessMethodBuilder.build());
    }

    private TypeSpec inlineAdapterBuilder(
            GrpcBinding binding,
            ClassName grpcAdapterClassName
    ) {
        PipelineStepModel model = binding.model();
        TypeName serviceType = model.serviceClassName();

        // Use the GrpcJavaTypeResolver to get the proper gRPC types from the binding
        GrpcJavaTypeResolver grpcTypeResolver = new GrpcJavaTypeResolver();
        GrpcJavaTypeResolver.GrpcJavaTypes grpcTypes = grpcTypeResolver.resolve(binding);

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

        return TypeSpec.anonymousClassBuilder("")
                .superclass(ParameterizedTypeName.get(
                        grpcAdapterClassName,
                        inputGrpcType,
                        outputGrpcType,
                        inputDomainType,
                        outputDomainType
                ))
                .addMethod(MethodSpec.methodBuilder("getService")
                        .addAnnotation(Override.class)
                        .addModifiers(Modifier.PROTECTED)
                        .returns(serviceType)
                        .addStatement("return service")
                        .build())
                .addMethod(fromGrpcMethodBuilder.build())
                .addMethod(toGrpcMethodBuilder.build())
                .build();
    }

    private String determineRoleForGrpcService(GrpcBinding binding) {
        // For now, assume it's a regular pipeline server
        // In a future implementation, we could distinguish between regular and plugin servers
        // based on additional metadata in the binding
        return "PIPELINE_SERVER";
    }
}
