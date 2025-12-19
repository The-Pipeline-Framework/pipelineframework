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
import org.pipelineframework.processor.ir.PipelineStepIR;

/**
 * Renderer for gRPC service adapters based on PipelineStepIR
 */
public class GrpcServiceAdapterRenderer implements PipelineRenderer {

    /**
     * Creates a new GrpcServiceAdapterRenderer.
     */
    public GrpcServiceAdapterRenderer() {
    }
    
    @Override
    public GenerationTarget target() {
        return GenerationTarget.GRPC_SERVICE;
    }
    
    @Override
    public void render(PipelineStepIR ir, GenerationContext ctx) throws IOException {
        if (ir.getStepKind() == org.pipelineframework.processor.ir.StepKind.LOCAL) {
            return; // Skip for local steps
        }
        
        TypeSpec grpcServiceClass = buildGrpcServiceClass(ir, ctx.getProcessingEnv());
        
        // Write the generated class
        JavaFile javaFile = JavaFile.builder(
            ir.getServicePackage() + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX, 
            grpcServiceClass)
            .build();

        try (var writer = ctx.getBuilderFile().openWriter()) {
            javaFile.writeTo(writer);
        }
    }
    
    private TypeSpec buildGrpcServiceClass(PipelineStepIR ir, javax.annotation.processing.ProcessingEnvironment processingEnv) {
        String simpleClassName = ir.getServiceName() + PipelineStepProcessor.GRPC_SERVICE_SUFFIX;
        
        // Determine the appropriate gRPC service base class based on configuration
        ClassName grpcBaseClassName = determineGrpcBaseClass(ir);
        
        TypeSpec.Builder grpcServiceBuilder = TypeSpec.classBuilder(simpleClassName)
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(AnnotationSpec.builder(ClassName.get(GrpcService.class)).build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.inject", "Singleton")).build())
            .addAnnotation(AnnotationSpec.builder(Unremovable.class).build())
            .superclass(grpcBaseClassName); // Extend the actual gRPC service base class

        // Add mapper fields with @Inject if they exist
        if (ir.getInputMapping().hasMapper()) {
            FieldSpec inboundMapperField = FieldSpec.builder(
                ir.getInputMapping().getMapperType(),
                "inboundMapper")
                .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.inject", "Inject")).build())
                .build();
            grpcServiceBuilder.addField(inboundMapperField);
        }

        if (ir.getOutputMapping().hasMapper()) {
            FieldSpec outboundMapperField = FieldSpec.builder(
                ir.getOutputMapping().getMapperType(),
                "outboundMapper")
                .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.inject", "Inject")).build())
                .build();
            grpcServiceBuilder.addField(outboundMapperField);
        }

        FieldSpec serviceField = FieldSpec.builder(
            ir.getServiceClassName(),
            "service")
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.inject", "Inject")).build())
            .build();
        grpcServiceBuilder.addField(serviceField);

        // Add the required gRPC service method implementation based on streaming shape
        switch (ir.getStreamingShape()) {
            case UNARY_UNARY:
                addUnaryUnaryMethod(grpcServiceBuilder, ir);
                break;
            case UNARY_STREAMING:
                addUnaryStreamingMethod(grpcServiceBuilder, ir);
                break;
            case STREAMING_UNARY:
                addStreamingUnaryMethod(grpcServiceBuilder, ir);
                break;
            case STREAMING_STREAMING:
                addStreamingStreamingMethod(grpcServiceBuilder, ir);
                break;
        }

        return grpcServiceBuilder.build();
    }
    
    private ClassName determineGrpcBaseClass(PipelineStepIR ir) {
        // Check if there's an explicit gRPC implementation class specified in the annotation
        if (ir.getGrpcImplType() != null) {
            // Use the explicit gRPC implementation class
            String grpcImplTypeStr = ir.getGrpcImplType().toString();
            // Split the name to package and simple name
            int lastDot = grpcImplTypeStr.lastIndexOf('.');
            if (lastDot > 0) {
                String packageName = grpcImplTypeStr.substring(0, lastDot);
                String simpleName = grpcImplTypeStr.substring(lastDot + 1);
                return ClassName.get(packageName, simpleName);
            } else {
                // If no package, just use the simple name
                return ClassName.get("", grpcImplTypeStr);
            }
        }

        // Default to determine the package from available gRPC types
        String grpcPackage = ir.getServicePackage() + ".grpc";

        // Try to determine the actual gRPC package from grpcStub if available
        // This is more reliable than using inputGrpcType/outputGrpcType which might be generic types like Any/Empty
        if (ir.getGrpcStubType() != null &&
            !ir.getGrpcStubType().toString().equals("void") &&
            !ir.getGrpcStubType().toString().equals("java.lang.Void") &&
            !ir.getGrpcStubType().toString().equals(Void.class.getName())) {
            // Use the grpcStub type to determine the correct package
            String grpcStubString = ir.getGrpcStubType().toString();
            int lastDotIndex = grpcStubString.lastIndexOf('.');
            if (lastDotIndex > 0) {
                grpcPackage = grpcStubString.substring(0, lastDotIndex);
            }
        } else if (ir.getInputMapping().getDomainType() != null &&
                   !ir.getInputMapping().getDomainType().toString().equals("void") &&
                   !ir.getInputMapping().getDomainType().toString().equals("java.lang.Void") &&
                   !ir.getInputMapping().getDomainType().toString().contains("google.protobuf")) {
            // Use input domain type if grpcStub is not available
            String domainTypeString = ir.getInputMapping().getDomainType().toString();
            int lastDotIndex = domainTypeString.lastIndexOf('.');
            if (lastDotIndex > 0) {
                grpcPackage = domainTypeString.substring(0, lastDotIndex);
            }
        } else if (ir.getOutputMapping().getDomainType() != null &&
                   !ir.getOutputMapping().getDomainType().toString().equals("void") &&
                   !ir.getOutputMapping().getDomainType().toString().equals("java.lang.Void") &&
                   !ir.getOutputMapping().getDomainType().toString().contains("google.protobuf")) {
            // Use output domain type if other options are not available
            String domainTypeString = ir.getOutputMapping().getDomainType().toString();
            int lastDotIndex = domainTypeString.lastIndexOf('.');
            if (lastDotIndex > 0) {
                grpcPackage = domainTypeString.substring(0, lastDotIndex);
            }
        }

        // Construct the gRPC service base class using the determined package
        // The gRPC implementation class follows the pattern: {ServiceName}ImplBase
        // where {ServiceName} is the full service class name (e.g., "PersistenceService" -> "PersistenceServiceImplBase")
        String baseServiceName = ir.getServiceName();
        String grpcServiceBaseClassSimple = baseServiceName + "ImplBase";

        return ClassName.get(grpcPackage, grpcServiceBaseClassSimple);
    }
    
    private void addUnaryUnaryMethod(TypeSpec.Builder builder, PipelineStepIR ir) {
        ClassName grpcAdapterClassName = 
            ClassName.get("org.pipelineframework.grpc", "GrpcReactiveServiceAdapter");

        // Create the inline adapter
        TypeSpec inlineAdapter = inlineAdapterBuilder(ir, grpcAdapterClassName);

        MethodSpec.Builder remoteProcessMethodBuilder = MethodSpec.methodBuilder("remoteProcess")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(ParameterizedTypeName.get(ClassName.get(Uni.class),
                ir.getOutputMapping().getGrpcType()))
            .addParameter(ir.getInputMapping().getGrpcType(), "request")
            .addStatement("$T adapter = $L",
                ParameterizedTypeName.get(grpcAdapterClassName,
                    ir.getInputMapping().getGrpcType(),
                    ir.getOutputMapping().getGrpcType(),
                    ir.getInputMapping().getDomainType(),
                    ir.getOutputMapping().getDomainType()),
                inlineAdapter)
            .addStatement("return adapter.remoteProcess($N)", "request");

        // Add @RunOnVirtualThread annotation if the property is enabled
        if (ir.getExecutionMode() == org.pipelineframework.processor.ir.ExecutionMode.VIRTUAL_THREADS) {
            remoteProcessMethodBuilder.addAnnotation(
                ClassName.get("io.smallrye.common.annotation", "RunOnVirtualThread"));
        }

        builder.addMethod(remoteProcessMethodBuilder.build());
    }

    private void addUnaryStreamingMethod(TypeSpec.Builder builder, PipelineStepIR ir) {
        ClassName grpcAdapterClassName = 
            ClassName.get("org.pipelineframework.grpc", "GrpcServiceStreamingAdapter");

        // Create the inline adapter
        TypeSpec inlineAdapter = inlineAdapterBuilder(ir, grpcAdapterClassName);

        MethodSpec.Builder remoteProcessMethodBuilder = MethodSpec.methodBuilder("remoteProcess")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(ParameterizedTypeName.get(ClassName.get(Multi.class),
                ir.getOutputMapping().getGrpcType()))
            .addParameter(ir.getInputMapping().getGrpcType(), "request")
            .addStatement("$T adapter = $L",
                ParameterizedTypeName.get(grpcAdapterClassName,
                    ir.getInputMapping().getGrpcType(),
                    ir.getOutputMapping().getGrpcType(),
                    ir.getInputMapping().getDomainType(),
                    ir.getOutputMapping().getDomainType()),
                inlineAdapter)
            .addStatement("return adapter.remoteProcess($N)", "request");

        // Add @RunOnVirtualThread annotation if the property is enabled
        if (ir.getExecutionMode() == org.pipelineframework.processor.ir.ExecutionMode.VIRTUAL_THREADS) {
            remoteProcessMethodBuilder.addAnnotation(
                ClassName.get("io.smallrye.common.annotation", "RunOnVirtualThread"));
        }

        builder.addMethod(remoteProcessMethodBuilder.build());
    }
    
    private void addStreamingUnaryMethod(TypeSpec.Builder builder, PipelineStepIR ir) {
        ClassName grpcAdapterClassName = 
            ClassName.get("org.pipelineframework.grpc", "GrpcServiceClientStreamingAdapter");

        // Create the inline adapter
        TypeSpec inlineAdapter = inlineAdapterBuilder(ir, grpcAdapterClassName);

        MethodSpec.Builder remoteProcessMethodBuilder = MethodSpec.methodBuilder("remoteProcess")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(ParameterizedTypeName.get(ClassName.get(Uni.class),
                ir.getOutputMapping().getGrpcType()))
            .addParameter(ParameterizedTypeName.get(ClassName.get(Multi.class),
                ir.getInputMapping().getGrpcType()), "request")
            .addStatement("$T adapter = $L",
                ParameterizedTypeName.get(grpcAdapterClassName,
                    ir.getInputMapping().getGrpcType(),
                    ir.getOutputMapping().getGrpcType(),
                    ir.getInputMapping().getDomainType(),
                    ir.getOutputMapping().getDomainType()),
                inlineAdapter)
            .addStatement("return adapter.remoteProcess($N)", "request");

        // Add @RunOnVirtualThread annotation if the property is enabled
        if (ir.getExecutionMode() == org.pipelineframework.processor.ir.ExecutionMode.VIRTUAL_THREADS) {
            remoteProcessMethodBuilder.addAnnotation(
                ClassName.get("io.smallrye.common.annotation", "RunOnVirtualThread"));
        }

        builder.addMethod(remoteProcessMethodBuilder.build());
    }

    private void addStreamingStreamingMethod(TypeSpec.Builder builder, PipelineStepIR ir) {
        ClassName grpcAdapterClassName = 
            ClassName.get("org.pipelineframework.grpc", "GrpcServiceBidirectionalStreamingAdapter");

        // Create the inline adapter
        TypeSpec inlineAdapterStreaming = inlineAdapterBuilder(ir, grpcAdapterClassName);

        MethodSpec.Builder remoteProcessMethodBuilder = MethodSpec.methodBuilder("remoteProcess")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(ParameterizedTypeName.get(ClassName.get(Multi.class),
                ir.getOutputMapping().getGrpcType()))
            .addParameter(ParameterizedTypeName.get(ClassName.get(Multi.class),
                ir.getInputMapping().getGrpcType()), "request")
            .addStatement("$T adapter = $L",
                ParameterizedTypeName.get(grpcAdapterClassName,
                    ir.getInputMapping().getGrpcType(),
                    ir.getOutputMapping().getGrpcType(),
                    ir.getInputMapping().getDomainType(),
                    ir.getOutputMapping().getDomainType()),
                inlineAdapterStreaming)
            .addStatement("return adapter.remoteProcess($N)", "request");

        // Add @RunOnVirtualThread annotation if the property is enabled
        if (ir.getExecutionMode() == org.pipelineframework.processor.ir.ExecutionMode.VIRTUAL_THREADS) {
            remoteProcessMethodBuilder.addAnnotation(
                ClassName.get("io.smallrye.common.annotation", "RunOnVirtualThread"));
        }

        builder.addMethod(remoteProcessMethodBuilder.build());
    }

    private TypeSpec inlineAdapterBuilder(
            PipelineStepIR ir,
            ClassName grpcAdapterClassName
    ) {
        return TypeSpec.anonymousClassBuilder("")
                .superclass(ParameterizedTypeName.get(
                        grpcAdapterClassName,
                        ir.getInputMapping().getGrpcType(),
                        ir.getOutputMapping().getGrpcType(),
                        ir.getInputMapping().getDomainType(),
                        ir.getOutputMapping().getDomainType()
                ))
                .addMethod(MethodSpec.methodBuilder("getService")
                        .addAnnotation(Override.class)
                        .addModifiers(Modifier.PROTECTED)
                        .returns(ir.getServiceClassName())
                        .addStatement("return service")
                        .build())
                .addMethod(MethodSpec.methodBuilder("fromGrpc")
                        .addAnnotation(Override.class)
                        .addModifiers(Modifier.PROTECTED)
                        .returns(ir.getInputMapping().getDomainType())
                        .addParameter(ir.inboundGrpcParamType(), "grpcIn")
                        .addStatement("return inboundMapper.fromGrpcFromDto(grpcIn)")
                        .build())
                .addMethod(MethodSpec.methodBuilder("toGrpc")
                        .addAnnotation(Override.class)
                        .addModifiers(Modifier.PROTECTED)
                        .returns(ir.getOutputMapping().getGrpcType())
                        .addParameter(ir.getOutputMapping().getDomainType(), "output")
                        .addStatement("return outboundMapper.toDtoToGrpc(output)")
                        .build())
                .build();
    }
}