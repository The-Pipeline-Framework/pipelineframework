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
import org.pipelineframework.parallelism.OrderingRequirement;
import org.pipelineframework.parallelism.ThreadSafety;
import org.pipelineframework.processor.PipelineStepProcessor;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.step.StepManyToMany;
import org.pipelineframework.step.StepOneToOne;
import org.pipelineframework.step.StepOneToMany;
import org.pipelineframework.step.functional.ManyToOne;

/**
 * Renders generated await client steps.
 */
public class AwaitClientStepRenderer {

    public GenerationTarget target() {
        return GenerationTarget.AWAIT_CLIENT_STEP;
    }

    public void render(PipelineStepModel model, GenerationContext ctx) throws IOException {
        String baseName = model.generatedName().endsWith("Service")
            ? model.generatedName().substring(0, model.generatedName().length() - "Service".length())
            : model.generatedName();
        String className = baseName + "AwaitClientStep";
        TypeName inputType = model.inboundDomainType();
        TypeName outputType = model.outboundDomainType();

        FieldSpec support = FieldSpec.builder(ClassName.get("org.pipelineframework.awaitable", "AwaitStepSupport"), "support")
            .addAnnotation(ClassName.get("jakarta.inject", "Inject"))
            .build();
        FieldSpec descriptorFactory = FieldSpec.builder(
                ClassName.get("org.pipelineframework.awaitable", "AwaitStepDescriptorFactory"),
                "descriptorFactory")
            .addAnnotation(ClassName.get("jakarta.inject", "Inject"))
            .build();

        MethodSpec apply = renderApplyMethod(model, inputType, outputType);
        TypeName stepInterface = stepInterface(model.streamingShape(), inputType, outputType);

        MethodSpec cacheKeyTargetType = MethodSpec.methodBuilder("cacheKeyTargetType")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(ParameterizedTypeName.get(ClassName.get(Class.class),
                com.squareup.javapoet.WildcardTypeName.subtypeOf(Object.class)))
            .addStatement("return $T.class", outputType)
            .build();

        TypeSpec type = TypeSpec.classBuilder(className)
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.enterprise.context", "Dependent")).build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get(Unremovable.class)).build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get("org.pipelineframework.annotation", "GeneratedRole"))
                .addMember("value", "$T.$L",
                    ClassName.get("org.pipelineframework.annotation", "GeneratedRole", "Role"),
                    ctx.role().name())
                .build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get("org.pipelineframework.annotation", "ParallelismHint"))
                .addMember("ordering", "$T.$L", ClassName.get(OrderingRequirement.class), OrderingRequirement.RELAXED.name())
                .addMember("threadSafety", "$T.$L", ClassName.get(ThreadSafety.class), ThreadSafety.SAFE.name())
                .build())
            .superclass(ClassName.get("org.pipelineframework.step", "ConfigurableStep"))
            .addSuperinterface(stepInterface)
            .addSuperinterface(ClassName.get("org.pipelineframework.cache", "CacheKeyTarget"))
            .addField(support)
            .addField(descriptorFactory)
            .addMethod(MethodSpec.constructorBuilder().addModifiers(Modifier.PUBLIC).build())
            .addMethod(cacheKeyTargetType)
            .addMethod(apply)
            .build();

        JavaFile.builder(model.servicePackage() + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX, type)
            .build()
            .writeTo(ctx.outputDir());
    }

    private MethodSpec renderApplyMethod(PipelineStepModel model, TypeName inputType, TypeName outputType) {
        return switch (model.streamingShape()) {
            case UNARY_UNARY -> MethodSpec.methodBuilder("applyOneToOne")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(ParameterizedTypeName.get(ClassName.get(Uni.class), outputType))
            .addParameter(inputType, "input")
            .addStatement("return support.awaitOneToOne(descriptorFactory.descriptor($S, $S, $S), input)",
                model.serviceName(),
                inputType.toString(),
                outputType.toString())
            .build();
            case UNARY_STREAMING -> MethodSpec.methodBuilder("applyOneToMany")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(ParameterizedTypeName.get(ClassName.get(Multi.class), outputType))
            .addParameter(inputType, "input")
            .addStatement("return support.awaitOneToMany(descriptorFactory.descriptor($S, $S, $S), input)",
                model.serviceName(),
                inputType.toString(),
                outputType.toString())
            .build();
            case STREAMING_UNARY -> MethodSpec.methodBuilder("apply")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(ParameterizedTypeName.get(ClassName.get(Uni.class), outputType))
            .addParameter(ParameterizedTypeName.get(ClassName.get(Multi.class), inputType), "input")
            .addStatement("return support.awaitManyToOne(descriptorFactory.descriptor($S, $S, $S), input)",
                model.serviceName(),
                inputType.toString(),
                outputType.toString())
            .build();
            case STREAMING_STREAMING -> MethodSpec.methodBuilder("applyTransform")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(ParameterizedTypeName.get(ClassName.get(Multi.class), outputType))
            .addParameter(ParameterizedTypeName.get(ClassName.get(Multi.class), inputType), "input")
            .addStatement("return support.awaitManyToMany(descriptorFactory.descriptor($S, $S, $S), input)",
                model.serviceName(),
                inputType.toString(),
                outputType.toString())
            .build();
        };
    }

    private TypeName stepInterface(StreamingShape shape, TypeName inputType, TypeName outputType) {
        return switch (shape) {
            case UNARY_UNARY -> ParameterizedTypeName.get(ClassName.get(StepOneToOne.class), inputType, outputType);
            case UNARY_STREAMING -> ParameterizedTypeName.get(ClassName.get(StepOneToMany.class), inputType, outputType);
            case STREAMING_UNARY -> ParameterizedTypeName.get(ClassName.get(ManyToOne.class), inputType, outputType);
            case STREAMING_STREAMING -> ParameterizedTypeName.get(ClassName.get(StepManyToMany.class), inputType, outputType);
        };
    }
}
