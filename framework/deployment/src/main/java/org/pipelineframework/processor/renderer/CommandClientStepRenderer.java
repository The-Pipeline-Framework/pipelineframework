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
import io.smallrye.mutiny.Uni;
import org.pipelineframework.config.pipeline.PipelineYamlConfig;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLoader;
import org.pipelineframework.parallelism.OrderingRequirement;
import org.pipelineframework.parallelism.ThreadSafety;
import org.pipelineframework.processor.PipelineStepProcessor;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.PipelineTransport;
import org.pipelineframework.step.StepOneToOne;

/**
 * Renders generated command client steps.
 */
public class CommandClientStepRenderer {

    public GenerationTarget target() {
        return GenerationTarget.COMMAND_CLIENT_STEP;
    }

    public void render(PipelineStepModel model, GenerationContext ctx) throws IOException {
        if (model.cacheKeyGenerator() == null) {
            throw new IllegalArgumentException("Command step " + model.serviceName() + " is missing command id generator");
        }
        String baseName = model.generatedName().endsWith("Service")
            ? model.generatedName().substring(0, model.generatedName().length() - "Service".length())
            : model.generatedName();
        String className = baseName + "CommandClientStep";
        PipelineConfigHints configHints = resolveConfigHints(ctx);
        PipelineTransport transportMode = configHints.transportMode();
        TypeName inputType = clientStepType(model.inboundDomainType(), transportMode, configHints.basePackage());
        TypeName outputType = clientStepType(model.outboundDomainType(), transportMode, configHints.basePackage());

        FieldSpec support = FieldSpec.builder(ClassName.get("org.pipelineframework.command", "CommandStepSupport"), "support")
            .addAnnotation(ClassName.get("jakarta.inject", "Inject"))
            .build();
        FieldSpec descriptorFactory = FieldSpec.builder(
                ClassName.get("org.pipelineframework.command", "CommandStepDescriptorFactory"),
                "descriptorFactory")
            .addAnnotation(ClassName.get("jakarta.inject", "Inject"))
            .build();
        FieldSpec commandIdGenerator = FieldSpec.builder(model.cacheKeyGenerator(), "commandIdGenerator")
            .addAnnotation(ClassName.get("jakarta.inject", "Inject"))
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
            .addSuperinterface(ParameterizedTypeName.get(ClassName.get(StepOneToOne.class), inputType, outputType))
            .addSuperinterface(ClassName.get("org.pipelineframework.cache", "CacheKeyTarget"))
            .addField(support)
            .addField(descriptorFactory)
            .addField(commandIdGenerator)
            .addMethod(MethodSpec.constructorBuilder().addModifiers(Modifier.PUBLIC).build())
            .addMethod(MethodSpec.methodBuilder("cacheKeyTargetType")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(ParameterizedTypeName.get(ClassName.get(Class.class),
                    com.squareup.javapoet.WildcardTypeName.subtypeOf(Object.class)))
                .addStatement("return $T.class", outputType)
                .build())
            .addMethod(MethodSpec.methodBuilder("applyOneToOne")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(ParameterizedTypeName.get(ClassName.get(Uni.class), outputType))
                .addParameter(inputType, "input")
                .addStatement("return support.execute(descriptorFactory.descriptor($S, null, $S, $S, $S), commandIdGenerator, input)",
                    model.serviceName(),
                    inputType.toString(),
                    outputType.toString(),
                    model.cacheKeyGenerator().canonicalName())
                .build())
            .build();

        JavaFile.builder(model.servicePackage() + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX, type)
            .build()
            .writeTo(ctx.outputDir());
    }

    private PipelineConfigHints resolveConfigHints(GenerationContext ctx) {
        if (ctx.transportMode() != null) {
            String basePackage = ctx.pipelineBasePackage() == null || ctx.pipelineBasePackage().isBlank()
                ? null
                : ctx.pipelineBasePackage();
            return new PipelineConfigHints(ctx.transportMode(), basePackage);
        }
        Map<String, String> options = ctx.processingEnv() == null ? Map.of() : ctx.processingEnv().getOptions();
        PipelineTransport configuredTransport = PipelineTransport.fromStringOptional(
            options == null ? null : options.get("pipeline.transport")).orElse(null);
        String basePackage = null;
        if (options != null) {
            String configPath = options.get("pipeline.config");
            if (configPath != null && !configPath.isBlank()) {
                PipelineYamlConfig config = loadPipelineConfig(ctx, configPath);
                if (config != null) {
                    if (configuredTransport == null) {
                        configuredTransport = PipelineTransport.fromString(config.transport());
                    }
                    basePackage = config.basePackage();
                }
            }
        }
        if (configuredTransport == null) {
            configuredTransport = PipelineTransport.GRPC;
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
                    javax.tools.Diagnostic.Kind.ERROR,
                    "Failed to load pipeline config '" + configPath + "' while rendering command client step: " + ex.getMessage());
            }
            throw new IllegalStateException("Failed to load pipeline config at '" + configPath + "'", ex);
        }
    }

    private TypeName clientStepType(TypeName domainType, PipelineTransport transportMode, String pipelineBasePackage) {
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
            if (pipelineBasePackage == null || pipelineBasePackage.isBlank()) {
                throw new IllegalArgumentException(
                    "Cannot infer command client transport type package for " + className
                        + "; pipeline base package is blank");
            }
            return pipelineBasePackage;
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

    private record PipelineConfigHints(PipelineTransport transportMode, String basePackage) {
    }
}
