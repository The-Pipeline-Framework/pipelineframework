package org.pipelineframework.processor.renderer;

import java.io.IOException;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import io.quarkus.arc.Unremovable;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.processor.PipelineStepProcessor;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.ServiceApiKind;
import org.pipelineframework.processor.util.GeneratedServiceTypeResolver;

/**
 * Generates a reactive bridge bean for blocking-authored internal services.
 */
public class BlockingReactiveBridgeRenderer {

    public GenerationTarget target() {
        return GenerationTarget.BLOCKING_REACTIVE_BRIDGE;
    }

    public void render(PipelineStepModel model, GenerationContext ctx) throws IOException {
        TypeSpec bridgeClass = buildBridgeClass(model, ctx.role());
        JavaFile.builder(
                model.servicePackage() + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX,
                bridgeClass)
            .build()
            .writeTo(ctx.outputDir());
    }

    private TypeSpec buildBridgeClass(
        PipelineStepModel model,
        org.pipelineframework.processor.ir.DeploymentRole role
    ) {
        TypeName inputType = model.inboundDomainType() != null ? model.inboundDomainType() : ClassName.OBJECT;
        TypeName outputType = model.outboundDomainType() != null ? model.outboundDomainType() : ClassName.OBJECT;

        TypeSpec.Builder builder = TypeSpec.classBuilder(GeneratedServiceTypeResolver.blockingReactiveBridgeClassName(model).simpleName())
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.enterprise.context", "ApplicationScoped")).build())
            .addAnnotation(AnnotationSpec.builder(Unremovable.class).build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get("org.pipelineframework.annotation", "GeneratedRole"))
                .addMember("value", "$T.$L",
                    ClassName.get("org.pipelineframework.annotation", "GeneratedRole", "Role"),
                    role.name())
                .build());

        builder.addSuperinterface(resolveReactiveInterface(model, inputType, outputType));
        builder.addField(FieldSpec.builder(model.serviceClassName(), "blockingService", Modifier.PRIVATE)
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.inject", "Inject")).build())
            .build());
        builder.addField(FieldSpec.builder(
                ClassName.get("org.pipelineframework.blocking", "BlockingExecutionSupport"),
                "blockingExecutionSupport",
                Modifier.PRIVATE)
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.inject", "Inject")).build())
            .build());

        boolean useVirtualThreads = model.executionMode() == org.pipelineframework.processor.ir.ExecutionMode.VIRTUAL_THREADS;
        builder.addMethod(buildProcessMethod(model, inputType, outputType, useVirtualThreads));
        return builder.build();
    }

    private TypeName resolveReactiveInterface(PipelineStepModel model, TypeName inputType, TypeName outputType) {
        return switch (model.streamingShape()) {
            case UNARY_STREAMING -> ParameterizedTypeName.get(
                ClassName.get("org.pipelineframework.service", "ReactiveStreamingService"),
                inputType,
                outputType);
            case STREAMING_UNARY -> ParameterizedTypeName.get(
                ClassName.get("org.pipelineframework.service", "ReactiveStreamingClientService"),
                inputType,
                outputType);
            case STREAMING_STREAMING -> ParameterizedTypeName.get(
                ClassName.get("org.pipelineframework.service", "ReactiveBidirectionalStreamingService"),
                inputType,
                outputType);
            default -> ParameterizedTypeName.get(
                ClassName.get("org.pipelineframework.service", "ReactiveService"),
                inputType,
                outputType);
        };
    }

    private MethodSpec buildProcessMethod(
        PipelineStepModel model,
        TypeName inputType,
        TypeName outputType,
        boolean useVirtualThreads
    ) {
        if (model.serviceApiKind() == ServiceApiKind.BLOCKING_ITERATOR) {
            return MethodSpec.methodBuilder("process")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(ParameterizedTypeName.get(ClassName.get(Multi.class), outputType))
                .addParameter(inputType, "processableObj")
                .addStatement(
                    "return blockingExecutionSupport.emitIterator($L, () -> blockingService.iterateBlocking(processableObj))",
                    useVirtualThreads)
                .build();
        }
        return switch (model.streamingShape()) {
            case UNARY_STREAMING -> MethodSpec.methodBuilder("process")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(ParameterizedTypeName.get(ClassName.get(Multi.class), outputType))
                .addParameter(inputType, "processableObj")
                .addStatement(
                    "return blockingExecutionSupport.emitList($L, () -> blockingService.processBlocking(processableObj))",
                    useVirtualThreads)
                .build();
            case STREAMING_UNARY -> MethodSpec.methodBuilder("process")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(ParameterizedTypeName.get(ClassName.get(Uni.class), outputType))
                .addParameter(ParameterizedTypeName.get(ClassName.get(Multi.class), inputType), "processableObj")
                .addStatement(
                    "return processableObj.collect().asList()"
                        + ".onItem().transformToUni(items -> blockingExecutionSupport.supply($L, () -> blockingService.processBlocking(items)))",
                    useVirtualThreads)
                .build();
            case STREAMING_STREAMING -> MethodSpec.methodBuilder("process")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(ParameterizedTypeName.get(ClassName.get(Multi.class), outputType))
                .addParameter(ParameterizedTypeName.get(ClassName.get(Multi.class), inputType), "processableObj")
                .addStatement(
                    "return processableObj.collect().asList()"
                        + ".onItem().transformToMulti(items -> blockingExecutionSupport.emitList($L, () -> blockingService.processBlocking(items)))",
                    useVirtualThreads)
                .build();
            default -> MethodSpec.methodBuilder("process")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(ParameterizedTypeName.get(ClassName.get(Uni.class), outputType))
                .addParameter(inputType, "processableObj")
                .addStatement(
                    "return blockingExecutionSupport.supply($L, () -> blockingService.processBlocking(processableObj))",
                    useVirtualThreads)
                .build();
        };
    }
}
