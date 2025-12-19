package org.pipelineframework.processor.renderer;

import java.io.IOException;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.*;
import io.quarkus.arc.Unremovable;
import io.quarkus.grpc.GrpcClient;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.processor.PipelineStepProcessor;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepIR;
import org.pipelineframework.step.StepManyToOne;
import org.pipelineframework.step.StepOneToOne;

/**
 * Renderer for gRPC client step implementations based on PipelineStepIR
 */
public class ClientStepRenderer implements PipelineRenderer {

    /**
     * Creates a new ClientStepRenderer.
     */
    public ClientStepRenderer() {
    }
    
    @Override
    public GenerationTarget target() {
        return GenerationTarget.CLIENT_STEP;
    }
    
    @Override
    public void render(PipelineStepIR ir, GenerationContext ctx) throws IOException {
        if (ir.getStepKind() == org.pipelineframework.processor.ir.StepKind.LOCAL) {
            return; // Skip for local steps
        }
        
        TypeSpec clientStepClass = buildClientStepClass(ir, ctx.getProcessingEnv());
        
        // Write the generated class
        JavaFile javaFile = JavaFile.builder(
            ir.getServicePackage() + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX, 
            clientStepClass)
            .build();

        try (var writer = ctx.getBuilderFile().openWriter()) {
            javaFile.writeTo(writer);
        }
    }
    
    private TypeSpec buildClientStepClass(PipelineStepIR ir, javax.annotation.processing.ProcessingEnvironment processingEnv) {
        String serviceClassName = ir.getServiceName();
        String clientStepClassName = serviceClassName.replace("Service", "") + PipelineStepProcessor.CLIENT_STEP_SUFFIX;

        // Create the class with Dependent annotation for CDI and Unremovable to prevent Quarkus from removing it during build
        TypeSpec.Builder clientStepBuilder = TypeSpec.classBuilder(clientStepClassName)
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.enterprise.context", "Dependent"))
                .build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get(Unremovable.class))
                .build());

        // Add gRPC client field with @GrpcClient annotation
        TypeName grpcClientType = null;
        if (ir.getGrpcStubType() != null && !ir.getGrpcStubType().toString().equals("void") && !ir.getGrpcStubType().toString().equals("java.lang.Void")) {
            // Use the gRPC stub type directly from the annotation as the field type
            // This follows the original approach of using the exact type specified in the annotation
            grpcClientType = ir.getGrpcStubType();
        } else {
            // Derive the gRPC stub class from the service name pattern as fallback
            // Typical gRPC stub pattern: {ServiceName}Grpc.{ServiceName}Stub
            String grpcPackage = ir.getServicePackage() + ".grpc";

            // Check if we can determine the package from input/output gRPC types
            if (ir.getInputMapping().getGrpcType() != null) {
                String grpcTypeString = ir.getInputMapping().getGrpcType().toString();
                int lastDotIndex = grpcTypeString.lastIndexOf('.');
                if (lastDotIndex > 0) {
                    grpcPackage = grpcTypeString.substring(0, lastDotIndex);
                }
            }

            // Construct the gRPC stub class name using service name
            String grpcServiceName = ir.getServiceName();
            // If the service name already ends with "Grpc", don't add another "Grpc"
            String grpcServiceGrpcClass = grpcServiceName.endsWith("Grpc") ? grpcServiceName : grpcServiceName + "Grpc";
            String grpcServiceStubClass = grpcServiceName.endsWith("Grpc") ? grpcServiceName.replace("Grpc", "Stub") : grpcServiceName + "Stub";
            grpcClientType = ClassName.get(grpcPackage, grpcServiceGrpcClass, grpcServiceStubClass);
        }

        if (grpcClientType != null) {
            FieldSpec grpcClientField = FieldSpec.builder(
                grpcClientType,
                "grpcClient",
                Modifier.PRIVATE)
                .addAnnotation(AnnotationSpec.builder(GrpcClient.class)
                    .addMember("value", "$S", ir.getGrpcClientName() != null ? ir.getGrpcClientName() : ir.getServiceName()) // Using grpcClient annotation value or service name as default
                    .build())
                .build();

            clientStepBuilder.addField(grpcClientField);
        }


        // Add default constructor
        MethodSpec constructor = MethodSpec.constructorBuilder()
            .addModifiers(Modifier.PUBLIC)
            .build();
        clientStepBuilder.addMethod(constructor);

        // Extend ConfigurableStep and implement the pipeline step interface based on streaming shape
        ClassName configurableStep = ClassName.get("org.pipelineframework.step", "ConfigurableStep");
        clientStepBuilder.superclass(configurableStep);

        // Add the appropriate pipeline step interface based on streaming shape
        ClassName stepInterface;
        switch (ir.getStreamingShape()) {
            case UNARY_UNARY:
                stepInterface = ClassName.get(StepOneToOne.class);
                clientStepBuilder.addSuperinterface(ParameterizedTypeName.get(stepInterface,
                    ir.getInputMapping().getGrpcType(),
                    ir.getOutputMapping().getGrpcType()));
                break;
            case UNARY_STREAMING:
                stepInterface = ClassName.get("org.pipelineframework.step", "StepOneToMany");
                clientStepBuilder.addSuperinterface(ParameterizedTypeName.get(stepInterface,
                    ir.getInputMapping().getGrpcType(),
                    ir.getOutputMapping().getGrpcType()));
                break;
            case STREAMING_UNARY:
                stepInterface = ClassName.get(StepManyToOne.class);
                clientStepBuilder.addSuperinterface(ParameterizedTypeName.get(stepInterface,
                    ir.getInputMapping().getGrpcType(),
                    ir.getOutputMapping().getGrpcType()));
                break;
            case STREAMING_STREAMING:
                stepInterface = ClassName.get("org.pipelineframework.step", "StepManyToMany");
                clientStepBuilder.addSuperinterface(ParameterizedTypeName.get(stepInterface,
                    ir.getInputMapping().getGrpcType(),
                    ir.getOutputMapping().getGrpcType()));
                break;
        }

        // Add the apply method implementation based on the streaming shape
        switch (ir.getStreamingShape()) {
            case UNARY_STREAMING:
                // For OneToMany: Input -> Multi<Output> (StepOneToMany interface has applyOneToMany(Input in) method)
                MethodSpec applyOneToManyMethod = MethodSpec.methodBuilder("applyOneToMany")
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .returns(ParameterizedTypeName.get(ClassName.get(Multi.class),
                        ir.getOutputMapping().getGrpcType()))
                    .addParameter(ir.getInputMapping().getGrpcType(), "input")
                    .addStatement("return this.grpcClient.remoteProcess(input)")
                    .build();
                clientStepBuilder.addMethod(applyOneToManyMethod);
                break;
            case STREAMING_UNARY:
                // For ManyToOne: Multi<Input> -> Uni<Output> (ManyToOne interface has applyBatchMulti(Multi<Input> in) method)
                MethodSpec applyBatchMultiMethod = MethodSpec.methodBuilder("applyBatchMulti")
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .returns(ParameterizedTypeName.get(ClassName.get(Uni.class),
                        ir.getOutputMapping().getGrpcType()))
                    .addParameter(ParameterizedTypeName.get(ClassName.get(Multi.class),
                        ir.getInputMapping().getGrpcType()), "inputs")
                    .addStatement("return this.grpcClient.remoteProcess(inputs)")
                    .build();
                clientStepBuilder.addMethod(applyBatchMultiMethod);
                break;
            case STREAMING_STREAMING:
                // For ManyToMany: Multi<Input> -> Multi<Output> (ManyToMany interface has applyTransform(Multi<Input> in) method)
                MethodSpec applyTransformMethod = MethodSpec.methodBuilder("applyTransform")
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .returns(ParameterizedTypeName.get(ClassName.get(Multi.class),
                        ir.getOutputMapping().getGrpcType()))
                    .addParameter(ParameterizedTypeName.get(ClassName.get(Multi.class),
                        ir.getInputMapping().getGrpcType()), "inputs")
                    .addStatement("return this.grpcClient.remoteProcess(inputs)")
                    .build();
                clientStepBuilder.addMethod(applyTransformMethod);
                break;
            case UNARY_UNARY:
            default:
                // Default to OneToOne: Input -> Uni<Output> (StepOneToOne interface has applyOneToOne(Input in) method)
                MethodSpec applyOneToOneMethod = MethodSpec.methodBuilder("applyOneToOne")
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .returns(ParameterizedTypeName.get(ClassName.get(Uni.class),
                        ir.getOutputMapping().getGrpcType()))
                    .addParameter(ir.getInputMapping().getGrpcType(), "input")
                    .addStatement("return this.grpcClient.remoteProcess(input)")
                    .build();
                clientStepBuilder.addMethod(applyOneToOneMethod);
                break;
        }

        return clientStepBuilder.build();
    }
}