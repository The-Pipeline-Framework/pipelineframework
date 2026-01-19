/*
 * Copyright (c) 2023-2025 Mariano Barcia
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
import javax.annotation.processing.Messager;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.*;
import io.quarkus.arc.Unremovable;
import io.quarkus.grpc.GrpcClient;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.parallelism.OrderingRequirement;
import org.pipelineframework.parallelism.ThreadSafety;
import org.pipelineframework.processor.PipelineStepProcessor;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.GrpcBinding;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.util.GrpcJavaTypeResolver;
import org.pipelineframework.step.StepManyToOne;
import org.pipelineframework.step.StepOneToOne;

/**
 * Renderer for gRPC client step implementations based on PipelineStepModel and GrpcBinding.
 * Supports both regular client steps and plugin client steps
 *
 * @param target The generation target for this renderer
 */
public record ClientStepRenderer(GenerationTarget target) implements PipelineRenderer<GrpcBinding> {

    /**
     * Generate and write the gRPC client step class for the given binding.
     *
     * @param binding the gRPC binding and its associated pipeline model used to build the client step
     * @param ctx     the generation context providing output directories and processing environment
     * @throws IOException if an I/O error occurs while writing the generated Java file
     */
    @Override
    public void render(GrpcBinding binding, GenerationContext ctx) throws IOException {
        TypeSpec clientStepClass = buildClientStepClass(binding, ctx);

        // Write the generated class
        JavaFile javaFile = JavaFile.builder(
                        binding.servicePackage() + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX,
                        clientStepClass)
                .build();

        javaFile.writeTo(ctx.outputDir());
    }

    /**
     * Build the JavaPoet TypeSpec for a gRPC client pipeline step corresponding to the given binding.
     * <p>
     * The produced TypeSpec represents a public ConfigurableStep subclass annotated for CDI and Quarkus,
     * optionally containing a gRPC client field, implementing the pipeline step interface appropriate
     * to the model's streaming shape, and providing the corresponding apply* method that delegates to
     * the gRPC client.
     *
     * @param binding  the gRPC binding describing the service, model and generated-class naming
     * @param messager the annotation processing Messager used during gRPC type resolution
     * @return the generated TypeSpec describing the client step class
     */
    private TypeSpec buildClientStepClass(GrpcBinding binding, GenerationContext ctx) {
        Messager messager = ctx.processingEnv().getMessager();
        org.pipelineframework.processor.ir.DeploymentRole role = ctx.role();
        PipelineStepModel model = binding.model();
        String clientStepClassName = getClientStepClassName(model);

        // Use the gRPC types resolved via GrpcJavaTypeResolver
        GrpcJavaTypeResolver grpcTypeResolver = new GrpcJavaTypeResolver();
        GrpcJavaTypeResolver.GrpcJavaTypes grpcTypes = grpcTypeResolver.resolve(binding, messager);
        TypeName inputGrpcType = grpcTypes.grpcParameterType();
        TypeName outputGrpcType = grpcTypes.grpcReturnType();

        // Create the class with Dependent annotation for CDI and Unremovable to prevent Quarkus from removing it during build
        TypeSpec.Builder clientStepBuilder = TypeSpec.classBuilder(clientStepClassName)
                .addModifiers(Modifier.PUBLIC)
                .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.enterprise.context", "Dependent"))
                        .build())
                .addAnnotation(AnnotationSpec.builder(ClassName.get(Unremovable.class))
                        .build())
                .addAnnotation(AnnotationSpec.builder(ClassName.get("org.pipelineframework.annotation", "ParallelismHint"))
                        .addMember("ordering", "$T.$L",
                            ClassName.get(OrderingRequirement.class),
                            model.orderingRequirement().name())
                        .addMember("threadSafety", "$T.$L",
                            ClassName.get(ThreadSafety.class),
                            model.threadSafety().name())
                        .build())
                // Add the GeneratedRole annotation to indicate the target role
                .addAnnotation(AnnotationSpec.builder(ClassName.get("org.pipelineframework.annotation", "GeneratedRole"))
                        .addMember("value", "$T.$L",
                            ClassName.get("org.pipelineframework.annotation.GeneratedRole", "Role"),
                            role.name())
                        .build());

        // Add gRPC client field with @GrpcClient annotation
        TypeName grpcClientType = resolveGrpcStubType(binding, messager);

        if (grpcClientType != null) {
            FieldSpec grpcClientField = FieldSpec.builder(
                            grpcClientType,
                            "grpcClient")
                    .addAnnotation(AnnotationSpec.builder(GrpcClient.class)
                            .addMember("value", "$S", toGrpcClientName(binding.serviceName()))
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
        switch (model.streamingShape()) {
            case UNARY_UNARY:
                stepInterface = ClassName.get(StepOneToOne.class);
                clientStepBuilder.addSuperinterface(ParameterizedTypeName.get(stepInterface,
                        inputGrpcType,
                        outputGrpcType));
                break;
            case UNARY_STREAMING:
                stepInterface = ClassName.get("org.pipelineframework.step", "StepOneToMany");
                clientStepBuilder.addSuperinterface(ParameterizedTypeName.get(stepInterface,
                        inputGrpcType,
                        outputGrpcType));
                break;
            case STREAMING_UNARY:
                stepInterface = ClassName.get(StepManyToOne.class);
                clientStepBuilder.addSuperinterface(ParameterizedTypeName.get(stepInterface,
                        inputGrpcType,
                        outputGrpcType));
                break;
            case STREAMING_STREAMING:
                stepInterface = ClassName.get("org.pipelineframework.step", "StepManyToMany");
                clientStepBuilder.addSuperinterface(ParameterizedTypeName.get(stepInterface,
                        inputGrpcType,
                        outputGrpcType));
                break;
        }

        if (grpcClientType != null) {
            // Add the apply method implementation based on the streaming shape
            switch (model.streamingShape()) {
                case UNARY_STREAMING:
                    // For OneToMany: Input -> Multi<Output> (StepOneToMany interface has applyOneToMany(Input in) method)
                    MethodSpec applyOneToManyMethod = MethodSpec.methodBuilder("applyOneToMany")
                            .addAnnotation(Override.class)
                            .addModifiers(Modifier.PUBLIC)
                            .returns(ParameterizedTypeName.get(ClassName.get(Multi.class),
                                    outputGrpcType))
                            .addParameter(inputGrpcType, "input")
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
                                    outputGrpcType))
                            .addParameter(ParameterizedTypeName.get(ClassName.get(Multi.class),
                                    inputGrpcType), "inputs")
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
                                    outputGrpcType))
                            .addParameter(ParameterizedTypeName.get(ClassName.get(Multi.class),
                                    inputGrpcType), "inputs")
                            .addStatement("return this.grpcClient.remoteProcess(inputs)")
                            .build();
                    clientStepBuilder.addMethod(applyTransformMethod);
                    break;
                case UNARY_UNARY:
                default:
                    // Default to OneToOne: Input -> Uni<Output> (StepOneToOne interface has applyOneToOne(Input in) method)
                    MethodSpec.Builder applyOneToOneMethod = MethodSpec.methodBuilder("applyOneToOne")
                            .addAnnotation(Override.class)
                            .addModifiers(Modifier.PUBLIC)
                            .returns(ParameterizedTypeName.get(ClassName.get(Uni.class),
                                    outputGrpcType))
                            .addParameter(inputGrpcType, "input");
                    applyOneToOneMethod.addStatement("return this.grpcClient.remoteProcess(input)");
                    clientStepBuilder.addMethod(applyOneToOneMethod.build());
                    break;
            }
        }

        return clientStepBuilder.build();
    }

    /**
     * Compute the Java class name for the generated client step for the given gRPC binding.
     *
     * @param binding the gRPC binding whose service name is used to derive the client step class name
     * @return the client step class name derived from the binding's service name
     */
    private String getClientStepClassName(PipelineStepModel model) {
        String serviceClassName = model.generatedName();

        // Determine client step class name based on the target
        String clientStepClassName;
        // For client steps: ${PluginName}GrpcClientStep
        clientStepClassName = serviceClassName.replace("Service", "") + PipelineStepProcessor.GRPC_CLIENT_STEP_SUFFIX;
        return clientStepClassName;
    }

    /**
     * Resolve the gRPC stub TypeName to be used for a client field for the given binding.
     *
     * @param binding the gRPC binding describing the service and step model
     * @return the TypeName of the gRPC stub to inject, or `null` if resolution is not available
     */
    private TypeName resolveGrpcStubType(GrpcBinding binding, Messager messager) {
        GrpcJavaTypeResolver grpcTypeResolver = new GrpcJavaTypeResolver();
        return grpcTypeResolver.resolve(binding, messager).stub();
    }

    /**
     * Convert a gRPC service class name into a hyphen-separated lowercase client name.
     *
     * <p>Removes a trailing "Service" suffix (if present) then inserts hyphens at camel-case boundaries
     * and returns the result in lowercase.</p>
     *
     * @param serviceName the service class name to convert; may be null or blank
     * @return the hyphen-separated lowercase client name, or an empty string if {@code serviceName} is null or blank
     */
    private static String toGrpcClientName(String serviceName) {
        if (serviceName == null || serviceName.isBlank()) {
            return "";
        }

        String baseName = serviceName.replaceFirst("Service$", "");
        String withBoundaryHyphens = baseName
                .replaceAll("([A-Z]+)([A-Z][a-z])", "$1-$2")
                .replaceAll("([a-z0-9])([A-Z])", "$1-$2");
        return withBoundaryHyphens.toLowerCase();
    }

}
