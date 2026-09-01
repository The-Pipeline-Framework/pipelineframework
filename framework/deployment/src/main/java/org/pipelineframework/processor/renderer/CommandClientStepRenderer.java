package org.pipelineframework.processor.renderer;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
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
        TypeName domainInputType = model.inboundDomainType();
        TypeName domainOutputType = model.outboundDomainType();
        TransportBindingPair normalizedTransport = normalizedTransport(model, ctx, transportMode);
        TypeName inputType = normalizedTransport.input().<TypeName>map(binding -> binding.transportType(transportMode))
            .orElseGet(() -> clientStepType(domainInputType, transportMode, configHints.basePackage()));
        TypeName outputType = normalizedTransport.output().<TypeName>map(binding -> binding.transportType(transportMode))
            .orElseGet(() -> clientStepType(domainOutputType, transportMode, configHints.basePackage()));
        boolean transportMapped = transportMode != PipelineTransport.LOCAL;

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
        FieldSpec inputMapper = null;
        FieldSpec outputMapper = null;
        if (transportMapped) {
            TypeName inputMapperType = normalizedTransport.input().<TypeName>map(binding -> binding.mapperType(transportMode))
                .orElseGet(() -> mapperType(domainInputType, configHints.basePackage()));
            TypeName outputMapperType = normalizedTransport.output().<TypeName>map(binding -> binding.mapperType(transportMode))
                .orElseGet(() -> mapperType(domainOutputType, configHints.basePackage()));
            FieldSpec.Builder inputMapperBuilder = FieldSpec.builder(inputMapperType, "inputMapper");
            FieldSpec.Builder outputMapperBuilder = FieldSpec.builder(outputMapperType, "outputMapper");
            if (normalizedTransport.input().isPresent()) {
                inputMapperBuilder.addModifiers(Modifier.PRIVATE, Modifier.FINAL).initializer("new $T()", inputMapperType);
            } else {
                inputMapperBuilder.addAnnotation(ClassName.get("jakarta.inject", "Inject"));
            }
            if (normalizedTransport.output().isPresent()) {
                outputMapperBuilder.addModifiers(Modifier.PRIVATE, Modifier.FINAL).initializer("new $T()", outputMapperType);
            } else {
                outputMapperBuilder.addAnnotation(ClassName.get("jakarta.inject", "Inject"));
            }
            inputMapper = inputMapperBuilder.build();
            outputMapper = outputMapperBuilder.build();
        }

        TypeSpec.Builder typeBuilder = TypeSpec.classBuilder(className)
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
            .addSuperinterface(ClassName.get("org.pipelineframework.command", "CommandStep"))
            .addSuperinterface(ClassName.get("org.pipelineframework.cache", "CacheKeyTarget"))
            .addField(support)
            .addField(descriptorFactory)
            .addField(commandIdGenerator);
        if (inputMapper != null) {
            typeBuilder.addField(inputMapper);
        }
        if (outputMapper != null) {
            typeBuilder.addField(outputMapper);
        }
        TypeSpec type = typeBuilder
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
                .addCode(applyBody(
                    model,
                    transportMode,
                    domainInputType,
                    domainOutputType,
                    model.cacheKeyGenerator().canonicalName(),
                    normalizedTransport.input().isPresent(),
                    normalizedTransport.output().isPresent()))
                .build())
            .build();

        JavaFile.builder(model.servicePackage() + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX, type)
            .build()
            .writeTo(ctx.outputDir());
    }

    private com.squareup.javapoet.CodeBlock applyBody(
        PipelineStepModel model,
        PipelineTransport transportMode,
        TypeName domainInputType,
        TypeName domainOutputType,
        String commandIdGeneratorName,
        boolean normalizedInput,
        boolean normalizedOutput
    ) {
        com.squareup.javapoet.CodeBlock.Builder body = com.squareup.javapoet.CodeBlock.builder();
        if (transportMode == PipelineTransport.LOCAL) {
            body.addStatement("return support.execute(descriptorFactory.descriptor($S, null, $S, $S, $S), commandIdGenerator, input)",
                model.serviceName(),
                domainInputType.toString(),
                domainOutputType.toString(),
                commandIdGeneratorName);
            return body.build();
        }
        if (transportMode == PipelineTransport.REST) {
            body.addStatement("$T commandInput = inputMapper.fromExternal(input)", domainInputType)
                .addStatement("return support.<$T, $T>execute(descriptorFactory.descriptor($S, null, $S, $S, $S), commandIdGenerator, commandInput)\n"
                        + "    .map(commandOutput -> outputMapper.toExternal(commandOutput))",
                    domainInputType,
                    domainOutputType,
                    model.serviceName(),
                    domainInputType.toString(),
                    domainOutputType.toString(),
                    commandIdGeneratorName);
            return body.build();
        }
        String fromGrpc = normalizedInput ? "fromGrpc" : "fromGrpcFromDto";
        String toGrpc = normalizedOutput ? "toGrpc" : "toDtoToGrpc";
        body.addStatement("$T commandInput = inputMapper.$L(input)", domainInputType, fromGrpc)
            .addStatement("return support.<$T, $T>execute(descriptorFactory.descriptor($S, null, $S, $S, $S), commandIdGenerator, commandInput)\n"
                    + "    .map(commandOutput -> outputMapper.$L(commandOutput))",
                domainInputType,
                domainOutputType,
                model.serviceName(),
                domainInputType.toString(),
                domainOutputType.toString(),
                commandIdGeneratorName,
                toGrpc);
        return body.build();
    }

    private TransportBindingPair normalizedTransport(
        PipelineStepModel model,
        GenerationContext context,
        PipelineTransport transport
    ) throws IOException {
        if (transport == PipelineTransport.LOCAL) {
            return new TransportBindingPair(Optional.empty(), Optional.empty());
        }
        V3TransportTypeBindingResolver resolver = new V3TransportTypeBindingResolver(context);
        Optional<V3TransportTypeBinding> input = resolver.resolve(model.inputMapping());
        Optional<V3TransportTypeBinding> output = resolver.resolve(model.outputMapping());
        if (input.isPresent() || output.isPresent()) {
            V3TransportRecordRenderer renderer = new V3TransportRecordRenderer(context, resolver);
            for (V3TransportTypeBinding binding : java.util.stream.Stream.concat(input.stream(), output.stream()).toList()) {
                if (transport == PipelineTransport.REST) {
                    renderer.ensureRest(binding);
                } else {
                    renderer.ensureGrpc(binding);
                }
            }
        }
        return new TransportBindingPair(input, output);
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

    private TypeName mapperType(TypeName domainType, String pipelineBasePackage) {
        if (!(domainType instanceof ClassName className)) {
            throw new IllegalArgumentException(
                "Cannot infer command mapper for non-class command type " + domainType);
        }
        String basePackage = basePackage(className, pipelineBasePackage);
        return ClassName.get(basePackage + ".common.mapper", className.simpleName() + "Mapper");
    }

    private record PipelineConfigHints(PipelineTransport transportMode, String basePackage) {
    }

    private record TransportBindingPair(
        Optional<V3TransportTypeBinding> input,
        Optional<V3TransportTypeBinding> output
    ) {
    }
}
