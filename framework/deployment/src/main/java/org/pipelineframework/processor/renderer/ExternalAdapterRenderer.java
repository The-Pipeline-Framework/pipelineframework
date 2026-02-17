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
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.pipelineframework.parallelism.OrderingRequirement;
import org.pipelineframework.parallelism.ThreadSafety;
import org.pipelineframework.processor.PipelineStepProcessor;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.ExternalAdapterBinding;
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.service.ReactiveService;
import org.pipelineframework.service.ReactiveStreamingClientService;
import org.pipelineframework.service.ReactiveStreamingService;
import org.pipelineframework.service.ReactiveBidirectionalStreamingService;

/**
 * Renderer for external adapter implementations based on PipelineStepModel.
 * The external adapter handles delegation to operator services and manages operator mapping.
 *
 * @param target The generation target for this renderer
 */
public record ExternalAdapterRenderer(GenerationTarget target) implements PipelineRenderer<ExternalAdapterBinding> {

    // Static constants for reactive service interface comparison
    private static final ClassName REACTIVE_SERVICE = ClassName.get(ReactiveService.class);
    private static final ClassName REACTIVE_STREAMING_SERVICE = ClassName.get(ReactiveStreamingService.class);
    private static final ClassName REACTIVE_STREAMING_CLIENT_SERVICE = ClassName.get(ReactiveStreamingClientService.class);
    private static final ClassName REACTIVE_BIDIRECTIONAL_STREAMING_SERVICE = ClassName.get(ReactiveBidirectionalStreamingService.class);

    /**
     * Generate and write the external adapter class for the given binding.
     *
     * @param binding the external adapter binding and its associated pipeline model used to build the adapter
     * @param ctx     the generation context providing output directories and processing environment
     * @throws IOException if an I/O error occurs while writing the generated Java file
     */
    @Override
    public void render(ExternalAdapterBinding binding, GenerationContext ctx) throws IOException {
        TypeSpec externalAdapterClass = buildExternalAdapterClass(binding, ctx);

        // Write the generated class
        JavaFile javaFile = JavaFile.builder(
                        binding.servicePackage() + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX,
                        externalAdapterClass)
                .build();

        javaFile.writeTo(ctx.outputDir());
    }

    /**
     * Constructs a JavaPoet TypeSpec for an external adapter tailored to the given binding and generation context.
     *
     * The produced TypeSpec is a public class annotated for CDI and Quarkus, implements the appropriate
     * reactive service interface based on the delegate service type, and provides the appropriate process method
     * that handles operator mapping and delegation.
     *
     * @param binding the external adapter binding describing the service, model, and generated-class naming
     * @param ctx the generation context supplying the processing environment and deployment role
     * @return the generated TypeSpec describing the external adapter class
     */
    private TypeSpec buildExternalAdapterClass(ExternalAdapterBinding binding, GenerationContext ctx) {
        PipelineStepModel model = binding.model();
        String externalAdapterClassName = getExternalAdapterClassName(model);

        // Get the delegate service type to determine the appropriate reactive interface
        ClassName delegateServiceType = model.delegateService();
        ClassName externalMapperType = model.externalMapper();

        // Determine the reactive service interface based on the delegate service
        ClassName reactiveServiceInterface = determineReactiveServiceInterface(model.streamingShape());

        // Get the input and output types for the external adapter
        TypeName applicationInputType = model.inboundDomainType();
        TypeName applicationOutputType = model.outboundDomainType();

        // Create the class with appropriate annotations
        TypeSpec.Builder externalAdapterBuilder = TypeSpec.classBuilder(externalAdapterClassName)
                .addModifiers(Modifier.PUBLIC)
                .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.enterprise.context", "ApplicationScoped"))
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
                        .build());

        // Add the reactive service interface
        externalAdapterBuilder.addSuperinterface(
            ParameterizedTypeName.get(reactiveServiceInterface, applicationInputType, applicationOutputType));

        // Add field for the delegate service (package-private, not public)
        FieldSpec delegateServiceField = FieldSpec.builder(
                        delegateServiceType,
                        "delegateService")
                .addAnnotation(Inject.class)
                .build();
        externalAdapterBuilder.addField(delegateServiceField);

        // Add field for the operator mapper if specified (package-private, not public)
        if (externalMapperType != null) {
            FieldSpec externalMapperField = FieldSpec.builder(
                            externalMapperType,
                            "externalMapper")
                    .addAnnotation(Inject.class)
                    .build();
            externalAdapterBuilder.addField(externalMapperField);
        }

        // Add default constructor
        MethodSpec constructor = MethodSpec.constructorBuilder()
                .addModifiers(Modifier.PUBLIC)
                .build();
        externalAdapterBuilder.addMethod(constructor);

        // Add the process method based on the reactive service interface
        MethodSpec processMethod = buildProcessMethod(
            model.streamingShape(),
            applicationInputType,
            applicationOutputType,
            externalMapperType != null);
        externalAdapterBuilder.addMethod(processMethod);

        return externalAdapterBuilder.build();
    }

    /**
     * Builds the process method based on the reactive service interface.
     */
    private MethodSpec buildProcessMethod(
            StreamingShape streamingShape,
            TypeName applicationInputType,
            TypeName applicationOutputType,
            boolean hasExternalMapper) {

        String methodName = "process";
        TypeName returnType;
        TypeName paramType;

        switch (streamingShape) {
            case UNARY_UNARY -> {
                returnType = ParameterizedTypeName.get(ClassName.get(Uni.class), applicationOutputType);
                paramType = applicationInputType;
            }
            case UNARY_STREAMING, STREAMING_STREAMING -> {
                returnType = ParameterizedTypeName.get(ClassName.get(Multi.class), applicationOutputType);
                paramType = ParameterizedTypeName.get(ClassName.get(Multi.class), applicationInputType);
            }
            case STREAMING_UNARY -> {
                returnType = ParameterizedTypeName.get(ClassName.get(Uni.class), applicationOutputType);
                paramType = ParameterizedTypeName.get(ClassName.get(Multi.class), applicationInputType);
            }
            default -> throw new IllegalStateException("Unsupported streaming shape: " + streamingShape);
        }

        MethodSpec.Builder methodBuilder = MethodSpec.methodBuilder(methodName)
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(returnType)
                .addParameter(paramType, "input");

        // Build the method body based on whether operator mapping is needed
        if (hasExternalMapper) {
            // If we have an operator mapper, we need to map between application and operator types
            if (streamingShape == StreamingShape.UNARY_UNARY) {
                // For unary operations: Uni<I> -> Uni<O>
                methodBuilder.addStatement("return delegateService.process(externalMapper.toOperatorInput(input)).map(libOutput -> externalMapper.toApplicationOutput(libOutput))");
            } else if (streamingShape == StreamingShape.STREAMING_UNARY) {
                // For client-streaming operations: Multi<I> -> Uni<O>
                addClientStreamingMapperLogic(methodBuilder);
            } else if (streamingShape == StreamingShape.UNARY_STREAMING
                    || streamingShape == StreamingShape.STREAMING_STREAMING) {
                // For streaming operations returning Multi: Multi<I> -> Multi<O>
                addStreamingMapperLogic(methodBuilder);
            } else {
                // Default case - no operator mapping
                methodBuilder.addStatement("return delegateService.process(input)");
            }
        } else {
            // If no operator mapper, just delegate directly
            methodBuilder.addStatement("return delegateService.process(input)");
        }

        return methodBuilder.build();
    }

    /**
     * Adds streaming mapper logic to the method builder for streaming reactive services.
     */
    private void addStreamingMapperLogic(MethodSpec.Builder methodBuilder) {
        methodBuilder
            .addStatement("var operatorInputs = input.map(appInput -> externalMapper.toOperatorInput(appInput))")
            .addStatement("var operatorOutputs = delegateService.process(operatorInputs)")
            .addStatement("return operatorOutputs.map(libOutput -> externalMapper.toApplicationOutput(libOutput))");
    }

    private void addClientStreamingMapperLogic(MethodSpec.Builder methodBuilder) {
        methodBuilder
            .addStatement("var operatorInputs = input.map(appInput -> externalMapper.toOperatorInput(appInput))")
            .addStatement("return delegateService.process(operatorInputs).map(libOutput -> externalMapper.toApplicationOutput(libOutput))");
    }

    /**
     * Determines the reactive service interface based on the delegate service type.
     */
    private ClassName determineReactiveServiceInterface(StreamingShape streamingShape) {
        return switch (streamingShape) {
            case UNARY_UNARY -> REACTIVE_SERVICE;
            case UNARY_STREAMING -> REACTIVE_STREAMING_SERVICE;
            case STREAMING_UNARY -> REACTIVE_STREAMING_CLIENT_SERVICE;
            case STREAMING_STREAMING -> REACTIVE_BIDIRECTIONAL_STREAMING_SERVICE;
        };
    }

    /**
     * Compute the Java class name for the generated external adapter for the given model.
     *
     * @param model the pipeline step model
     * @return the external adapter class name
     * @throws IllegalArgumentException if model.generatedName() returns null
     */
    public static String getExternalAdapterClassName(PipelineStepModel model) {
        String serviceClassName = model.generatedName();
        if (serviceClassName == null) {
            throw new IllegalArgumentException("PipelineStepModel.generatedName() must not be null");
        }
        // Only strip "Service" if it's at the end of the name
        String baseName = serviceClassName.endsWith("Service")
            ? serviceClassName.substring(0, serviceClassName.length() - "Service".length())
            : serviceClassName;
        return baseName + "ExternalAdapter";
    }
}
