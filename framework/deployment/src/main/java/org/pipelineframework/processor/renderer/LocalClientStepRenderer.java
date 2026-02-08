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

    /**
     * The generation target produced by this renderer.
     *
     * @return the GenerationTarget that identifies local client step generation
     */
    @Override
    public GenerationTarget target() {
        return GenerationTarget.LOCAL_CLIENT_STEP;
    }

    /**
     * Generates and writes the local client step Java source file for the given binding.
     *
     * Builds a TypeSpec for the client step using the binding and generation context, wraps it
     * in a JavaFile under the binding's service package + pipeline package suffix, and writes
     * the resulting file to the generation output directory.
     *
     * @param binding the LocalBinding describing the step model and target package
     * @param ctx the generation context containing output directory and role information
     * @throws IOException if writing the generated Java file to the output directory fails
     */
    @Override
    public void render(LocalBinding binding, GenerationContext ctx) throws IOException {
        TypeSpec clientStepClass = buildClientStepClass(binding, ctx);
        JavaFile javaFile = JavaFile.builder(
                binding.servicePackage() + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX,
                clientStepClass)
            .build();
        javaFile.writeTo(ctx.outputDir());
    }

    /**
     * Builds a TypeSpec for a local client step class based on the provided binding and generation context.
     *
     * The generated class is annotated with lifecycle and parallelism hints, implements the step interface
     * corresponding to the model's streaming shape, conditionally implements cache bypass/target interfaces
     * when the model indicates side effects or unary-unary shape, injects the resolved service type, and
     * adds tracing-enabled RPC wrapper methods that delegate to the service's `process` method.
     *
     * @param binding the local binding containing the pipeline step model and related metadata used to generate the class
     * @param ctx the generation context providing role and output directory information used during generation
     * @return the TypeSpec representing the complete local client step class
     */
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
            case UNARY_UNARY -> {
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
            }
            case UNARY_STREAMING -> {
                stepInterface = ClassName.get("org.pipelineframework.step", "StepOneToMany");
                clientStepBuilder.addSuperinterface(ParameterizedTypeName.get(stepInterface, inputType, outputType));
            }
            case STREAMING_UNARY -> {
                stepInterface = ClassName.get(StepManyToOne.class);
                clientStepBuilder.addSuperinterface(ParameterizedTypeName.get(stepInterface, inputType, outputType));
            }
            case STREAMING_STREAMING -> {
                stepInterface = ClassName.get("org.pipelineframework.step", "StepManyToMany");
                clientStepBuilder.addSuperinterface(ParameterizedTypeName.get(stepInterface, inputType, outputType));
            }
            default -> throw new IllegalStateException(
                "Unsupported streaming shape for local client generation: " + model.streamingShape());
        }

        ClassName tracing = ClassName.get("org.pipelineframework.telemetry", "LocalClientTracing");
        String rpcServiceName = model.serviceName();
        String rpcMethodName = "localProcess";

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
            default -> throw new IllegalStateException(
                "Unsupported streaming shape for local call " + rpcServiceName + "." + rpcMethodName
                    + ": " + model.streamingShape());
        }

        return clientStepBuilder.build();
    }

    /**
     * Builds the generated local client step class name for the given pipeline step model.
     *
     * @param model the pipeline step model whose generatedName() is used as the base
     * @return the class name formed by removing a trailing "Service" from the model's generated name if present, then appending "LocalClientStep"
     */
    private String getClientStepClassName(PipelineStepModel model) {
        String serviceClassName = model.generatedName();
        if (serviceClassName.endsWith("Service")) {
            serviceClassName = serviceClassName.substring(0, serviceClassName.length() - "Service".length());
        }
        return serviceClassName + "LocalClientStep";
    }

    /**
     * Resolve the service type to use when generating the client step.
     *
     * @param model the pipeline step model describing the step and its service
     * @return `ClassName` for the service in the pipeline package when the step declares side effects; otherwise the service class name declared on the model
     */
    private TypeName resolveServiceType(PipelineStepModel model) {
        if (model.sideEffect()) {
            return ClassName.get(
                model.servicePackage() + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX,
                model.serviceName());
        }
        return model.serviceClassName();
    }

    /**
     * Resolve a domain type, falling back to `ClassName.OBJECT` when no type is provided.
     *
     * @return the provided `TypeName`, or `ClassName.OBJECT` if the argument is `null`
     */
    private TypeName resolveDomainType(TypeName type) {
        return type != null ? type : ClassName.OBJECT;
    }
}