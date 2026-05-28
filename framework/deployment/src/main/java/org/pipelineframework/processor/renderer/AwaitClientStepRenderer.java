package org.pipelineframework.processor.renderer;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
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
import org.pipelineframework.config.pipeline.PipelineYamlConfig;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLoader;
import org.pipelineframework.parallelism.OrderingRequirement;
import org.pipelineframework.parallelism.ThreadSafety;
import org.pipelineframework.processor.PipelineStepProcessor;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.processor.ir.TransportMode;
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
        PipelineConfigHints configHints = resolveConfigHints(ctx);
        TransportMode transportMode = configHints.transportMode();
        TypeName inputType = clientStepType(model.inboundDomainType(), transportMode, configHints.basePackage());
        TypeName outputType = clientStepType(model.outboundDomainType(), transportMode, configHints.basePackage());

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
        if (model.streamingShape() == StreamingShape.UNARY_UNARY) {
            type = type.toBuilder()
                .addSuperinterface(ParameterizedTypeName.get(
                    ClassName.get("org.pipelineframework.awaitable", "AwaitStreamOneToOneStep"),
                    inputType,
                    outputType))
                .addMethod(MethodSpec.methodBuilder("applyAwaitPerItem")
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .returns(ParameterizedTypeName.get(ClassName.get(Multi.class), outputType))
                    .addParameter(ParameterizedTypeName.get(ClassName.get(Multi.class), inputType), "input")
                    .addStatement("return support.awaitOneToOneStream(descriptorFactory.descriptor($S, $S, $S), input)",
                        model.serviceName(),
                        inputType.toString(),
                        outputType.toString())
                    .build())
                .build();
        }

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

    private PipelineConfigHints resolveConfigHints(GenerationContext ctx) {
        if (ctx.transportMode() != null && ctx.pipelineBasePackage() != null && !ctx.pipelineBasePackage().isBlank()) {
            return new PipelineConfigHints(ctx.transportMode(), ctx.pipelineBasePackage());
        }
        Map<String, String> options = ctx.processingEnv() == null ? Map.of() : ctx.processingEnv().getOptions();
        TransportMode configuredTransport = TransportMode.fromStringOptional(
            options == null ? null : options.get("pipeline.transport")).orElse(null);
        String basePackage = null;
        if (options != null) {
            String configPath = options.get("pipeline.config");
            if (configPath != null && !configPath.isBlank()) {
                PipelineYamlConfig config = loadPipelineConfig(ctx, configPath);
                if (config != null) {
                    if (configuredTransport == null) {
                        configuredTransport = TransportMode.fromString(config.transport());
                    }
                    basePackage = config.basePackage();
                }
            }
        }
        if (configuredTransport == null) {
            configuredTransport = TransportMode.GRPC;
        }
        return new PipelineConfigHints(configuredTransport, basePackage);
    }

    private PipelineYamlConfig loadPipelineConfig(GenerationContext ctx, String configPath) {
        try {
            return new PipelineYamlConfigLoader(ctx.processingEnv().getOptions()::get, System::getenv)
                .load(Path.of(configPath));
        } catch (RuntimeException ex) {
            if (ctx.processingEnv() != null && ctx.processingEnv().getMessager() != null) {
                ctx.processingEnv().getMessager().printMessage(
                    javax.tools.Diagnostic.Kind.WARNING,
                    "Failed to load pipeline config '" + configPath + "' while rendering await client step: " + ex.getMessage());
            }
            throw new IllegalStateException("Failed to load pipeline config at '" + configPath + "'", ex);
        }
    }

    private TypeName clientStepType(TypeName domainType, TransportMode transportMode, String pipelineBasePackage) {
        if (!(domainType instanceof ClassName className)) {
            return domainType;
        }
        String basePackage = basePackage(className, pipelineBasePackage);
        return switch (transportMode) {
            case LOCAL -> className;
            case REST -> ClassName.get(basePackage + ".common.dto", className.simpleName() + "Dto");
            case GRPC -> ClassName.get(basePackage + ".grpc", "PipelineTypes", className.simpleName());
        };
    }

    private String basePackage(ClassName className, String pipelineBasePackage) {
        String packageName = className.packageName();
        if (packageName == null || packageName.isBlank()) {
            return pipelineBasePackage == null || pipelineBasePackage.isBlank()
                ? packageName
                : pipelineBasePackage;
        }
        if (packageName.endsWith(".common.domain")) {
            return packageName.substring(0, packageName.length() - ".common.domain".length());
        }
        if (packageName.endsWith(".common.dto")) {
            return packageName.substring(0, packageName.length() - ".common.dto".length());
        }
        if (packageName.endsWith(".service")) {
            return packageName.substring(0, packageName.length() - ".service".length());
        }
        return packageName;
    }

    private record PipelineConfigHints(TransportMode transportMode, String basePackage) {
    }
}
