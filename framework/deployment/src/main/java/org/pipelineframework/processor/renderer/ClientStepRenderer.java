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
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.*;
import io.quarkus.arc.Unremovable;
import io.quarkus.grpc.GrpcClient;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
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

    @Override
    public void render(GrpcBinding binding, GenerationContext ctx) throws IOException {
        TypeSpec clientStepClass = buildClientStepClass(binding);

        // Write the generated class
        JavaFile javaFile = JavaFile.builder(
                        binding.servicePackage() + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX,
                        clientStepClass)
                .build();

        try (var writer = ctx.builderFile().openWriter()) {
            javaFile.writeTo(writer);
        }
    }

    private TypeSpec buildClientStepClass(GrpcBinding binding) {
        PipelineStepModel model = binding.model();
        String clientStepClassName = getClientStepClassName(binding);

        // Use the gRPC types resolved via GrpcJavaTypeResolver
        GrpcJavaTypeResolver grpcTypeResolver = new GrpcJavaTypeResolver();
        GrpcJavaTypeResolver.GrpcJavaTypes grpcTypes = grpcTypeResolver.resolve(binding);
        TypeName inputGrpcType = grpcTypes.grpcParameterType();
        TypeName outputGrpcType = grpcTypes.grpcReturnType();

        // Create the class with Dependent annotation for CDI and Unremovable to prevent Quarkus from removing it during build
        TypeSpec.Builder clientStepBuilder = TypeSpec.classBuilder(clientStepClassName)
                .addModifiers(Modifier.PUBLIC)
                .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.enterprise.context", "Dependent"))
                        .build())
                .addAnnotation(AnnotationSpec.builder(ClassName.get(Unremovable.class))
                        .build())
                // Add the GeneratedRole annotation to indicate this is an orchestrator client
                .addAnnotation(AnnotationSpec.builder(ClassName.get("org.pipelineframework.annotation", "GeneratedRole"))
                        .addMember("value", "$T.$L",
                            ClassName.get("org.pipelineframework.annotation.GeneratedRole", "Role"),
                            determineRoleForClientStep(binding))
                        .build());

        // Add gRPC client field with @GrpcClient annotation
        TypeName grpcClientType = resolveGrpcStubType(binding);

        if (grpcClientType != null) {
            FieldSpec grpcClientField = FieldSpec.builder(
                            grpcClientType,
                            "grpcClient",
                            Modifier.PRIVATE)
                    .addAnnotation(AnnotationSpec.builder(GrpcClient.class)
                            .addMember("value", "$S", binding.serviceName().replace("Service", "")) // Using service name as default
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
                MethodSpec applyOneToOneMethod = MethodSpec.methodBuilder("applyOneToOne")
                        .addAnnotation(Override.class)
                        .addModifiers(Modifier.PUBLIC)
                        .returns(ParameterizedTypeName.get(ClassName.get(Uni.class),
                                outputGrpcType))
                        .addParameter(inputGrpcType, "input")
                        .addStatement("return this.grpcClient.remoteProcess(input)")
                        .build();
                clientStepBuilder.addMethod(applyOneToOneMethod);
                break;
        }

        return clientStepBuilder.build();
    }

    private String getClientStepClassName(GrpcBinding binding) {
        String serviceClassName = binding.serviceName();

        // Determine client step class name based on the target
        String clientStepClassName;
        // For client steps: ${PluginName}ClientStep
        clientStepClassName = serviceClassName.replace("Service", "") + PipelineStepProcessor.CLIENT_STEP_SUFFIX;
        return clientStepClassName;
    }

    private TypeName resolveGrpcStubType(GrpcBinding binding) {
        GrpcJavaTypeResolver grpcTypeResolver = new GrpcJavaTypeResolver();
        return grpcTypeResolver.resolve(binding).stub();
    }

    private String determineRoleForClientStep(GrpcBinding binding) {
        // For now, determine the role based on naming convention
        // If the class name starts with "SideEffect", it's a plugin client
        if (getClientStepClassName(binding).startsWith("SideEffect")) {
            return "PLUGIN_CLIENT";
        } else {
            return "ORCHESTRATOR_CLIENT";
        }
    }
}
