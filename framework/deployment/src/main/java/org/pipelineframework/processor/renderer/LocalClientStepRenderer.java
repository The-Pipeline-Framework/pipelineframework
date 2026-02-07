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
import com.squareup.javapoet.WildcardTypeName;
import io.quarkus.arc.Unremovable;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.parallelism.OrderingRequirement;
import org.pipelineframework.parallelism.ThreadSafety;
import org.pipelineframework.processor.PipelineStepProcessor;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.LocalBinding;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.step.StepManyToOne;
import org.pipelineframework.step.StepOneToOne;

/**
 * Renderer for local/in-process client step implementations.
 */
public class LocalClientStepRenderer implements PipelineRenderer<LocalBinding> {

    @Override
    public GenerationTarget target() {
        return GenerationTarget.LOCAL_CLIENT_STEP;
    }

    @Override
    public void render(LocalBinding binding, GenerationContext ctx) throws IOException {
        TypeSpec clientStepClass = buildClientStepClass(binding, ctx);
        JavaFile javaFile = JavaFile.builder(
                binding.servicePackage() + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX,
                clientStepClass)
            .build();
        javaFile.writeTo(ctx.outputDir());
    }

    private TypeSpec buildClientStepClass(LocalBinding binding, GenerationContext ctx) {
        PipelineStepModel model = binding.model();
        String clientStepClassName = getClientStepClassName(model);
        org.pipelineframework.processor.ir.DeploymentRole role = ctx.role();

        TypeName inputType = resolveDomainType(model.inboundDomainType());
        TypeName outputType = resolveDomainType(model.outboundDomainType());

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
            .addAnnotation(AnnotationSpec.builder(ClassName.get("org.pipelineframework.annotation", "GeneratedRole"))
                .addMember("value", "$T.$L",
                    ClassName.get("org.pipelineframework.annotation.GeneratedRole", "Role"),
                    role.name())
                .build());
        if (model.sideEffect()) {
            clientStepBuilder.addSuperinterface(ClassName.get("org.pipelineframework.cache", "CacheReadBypass"));
        }

        TypeName serviceType = resolveServiceType(model);
        FieldSpec serviceField = FieldSpec.builder(serviceType, "service")
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.inject", "Inject"))
                .build())
            .build();
        clientStepBuilder.addField(serviceField);
        clientStepBuilder.addMethod(MethodSpec.constructorBuilder()
            .addModifiers(Modifier.PUBLIC)
            .build());

        ClassName configurableStep = ClassName.get("org.pipelineframework.step", "ConfigurableStep");
        clientStepBuilder.superclass(configurableStep);

        ClassName stepInterface;
        switch (model.streamingShape()) {
            case UNARY_UNARY:
                stepInterface = ClassName.get(StepOneToOne.class);
                clientStepBuilder.addSuperinterface(ClassName.get("org.pipelineframework.cache", "CacheKeyTarget"));
                clientStepBuilder.addSuperinterface(ParameterizedTypeName.get(stepInterface, inputType, outputType));
                MethodSpec cacheKeyTargetMethod = MethodSpec.methodBuilder("cacheKeyTargetType")
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .returns(ParameterizedTypeName.get(ClassName.get(Class.class),
                        WildcardTypeName.subtypeOf(Object.class)))
                    .addStatement("return $T.class", outputType)
                    .build();
                clientStepBuilder.addMethod(cacheKeyTargetMethod);
                break;
            case UNARY_STREAMING:
                stepInterface = ClassName.get("org.pipelineframework.step", "StepOneToMany");
                clientStepBuilder.addSuperinterface(ParameterizedTypeName.get(stepInterface, inputType, outputType));
                break;
            case STREAMING_UNARY:
                stepInterface = ClassName.get(StepManyToOne.class);
                clientStepBuilder.addSuperinterface(ParameterizedTypeName.get(stepInterface, inputType, outputType));
                break;
            case STREAMING_STREAMING:
                stepInterface = ClassName.get("org.pipelineframework.step", "StepManyToMany");
                clientStepBuilder.addSuperinterface(ParameterizedTypeName.get(stepInterface, inputType, outputType));
                break;
        }

        ClassName tracing = ClassName.get("org.pipelineframework.telemetry", "LocalClientTracing");
        String rpcServiceName = model.serviceName();
        String rpcMethodName = "remoteProcess";

        switch (model.streamingShape()) {
            case UNARY_STREAMING -> {
                MethodSpec applyOneToManyMethod = MethodSpec.methodBuilder("applyOneToMany")
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .returns(ParameterizedTypeName.get(ClassName.get(Multi.class), outputType))
                    .addParameter(inputType, "input")
                    .addStatement("return $T.traceMulti($S, $S, this.service.process(input))",
                        tracing, rpcServiceName, rpcMethodName)
                    .build();
                clientStepBuilder.addMethod(applyOneToManyMethod);
            }
            case STREAMING_UNARY -> {
                MethodSpec applyBatchMultiMethod = MethodSpec.methodBuilder("applyBatchMulti")
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .returns(ParameterizedTypeName.get(ClassName.get(Uni.class), outputType))
                    .addParameter(ParameterizedTypeName.get(ClassName.get(Multi.class), inputType), "inputs")
                    .addStatement("return $T.traceUnary($S, $S, this.service.process(inputs))",
                        tracing, rpcServiceName, rpcMethodName)
                    .build();
                clientStepBuilder.addMethod(applyBatchMultiMethod);
            }
            case STREAMING_STREAMING -> {
                MethodSpec applyTransformMethod = MethodSpec.methodBuilder("applyTransform")
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .returns(ParameterizedTypeName.get(ClassName.get(Multi.class), outputType))
                    .addParameter(ParameterizedTypeName.get(ClassName.get(Multi.class), inputType), "inputs")
                    .addStatement("return $T.traceMulti($S, $S, this.service.process(inputs))",
                        tracing, rpcServiceName, rpcMethodName)
                    .build();
                clientStepBuilder.addMethod(applyTransformMethod);
            }
            case UNARY_UNARY -> {
                MethodSpec applyOneToOneMethod = MethodSpec.methodBuilder("applyOneToOne")
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .returns(ParameterizedTypeName.get(ClassName.get(Uni.class), outputType))
                    .addParameter(inputType, "input")
                    .addStatement("return $T.traceUnary($S, $S, this.service.process(input))",
                        tracing, rpcServiceName, rpcMethodName)
                    .build();
                clientStepBuilder.addMethod(applyOneToOneMethod);
            }
        }

        return clientStepBuilder.build();
    }

    private String getClientStepClassName(PipelineStepModel model) {
        String serviceClassName = model.generatedName();
        if (serviceClassName.endsWith("Service")) {
            serviceClassName = serviceClassName.substring(0, serviceClassName.length() - "Service".length());
        }
        return serviceClassName + "LocalClientStep";
    }

    private TypeName resolveServiceType(PipelineStepModel model) {
        if (model.sideEffect()) {
            return ClassName.get(
                model.servicePackage() + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX,
                model.serviceName());
        }
        return model.serviceClassName();
    }

    private TypeName resolveDomainType(TypeName type) {
        return type != null ? type : ClassName.OBJECT;
    }
}
