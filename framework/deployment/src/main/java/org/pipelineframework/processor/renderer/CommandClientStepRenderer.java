package org.pipelineframework.processor.renderer;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
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
import org.pipelineframework.processor.ir.ConnectorOperationSelection;
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
        TypeName domainOutputType = model.deferredCompletionSelection()
            .map(completion -> completion.finalOutputType()).orElseGet(model::outboundDomainType);
        PipelineStepModel transportModel = model.deferredCompletionSelection().isPresent()
            ? model.toBuilder().outputMapping(org.pipelineframework.processor.ir.TypeMapping.withoutMapper(domainOutputType)).build()
            : model;
        CanonicalTransportBindingPair normalizedTransport = CanonicalTransportBindingResolver.resolveAndEnsure(
            ctx, transportModel, transportMode);
        TypeName inputType = normalizedTransport.input().<TypeName>map(binding -> binding.transportType(transportMode))
            .orElseGet(() -> clientStepType(domainInputType, transportMode, configHints.basePackage()));
        TypeName outputType = normalizedTransport.output().<TypeName>map(binding -> binding.transportType(transportMode))
            .orElseGet(() -> clientStepType(domainOutputType, transportMode, configHints.basePackage()));
        boolean transportMapped = transportMode != PipelineTransport.LOCAL;
        Optional<ConnectorOperationSelection> connectorSelection = model.connectorOperationSelection();

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
            .addField(commandIdGenerator);
        if (connectorSelection.isEmpty()) {
            typeBuilder.addField(descriptorFactory);
        }
        if (model.deferredCompletionSelection().isPresent()) {
            addCallbackCompletion(typeBuilder, model);
        }
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
                    normalizedTransport.output().isPresent(),
                    connectorSelection))
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
        boolean normalizedOutput,
        Optional<ConnectorOperationSelection> connectorSelection
    ) {
        com.squareup.javapoet.CodeBlock.Builder body = com.squareup.javapoet.CodeBlock.builder();
        CodeBlock descriptor = connectorSelection
            .map(selection -> nativeDescriptor(
                selection, domainInputType.toString(), model.outboundDomainType().toString(), commandIdGeneratorName))
            .orElseGet(() -> CodeBlock.of("descriptorFactory.descriptor($S, null, $S, $S, $S)",
                model.serviceName(), domainInputType.toString(), domainOutputType.toString(), commandIdGeneratorName));
        if (model.deferredCompletionSelection().isPresent()) {
            if (transportMode == PipelineTransport.LOCAL) {
                body.addStatement("$T commandInput = input", domainInputType);
            } else {
                String from = transportMode == PipelineTransport.REST ? "fromExternal"
                    : normalizedInput ? "fromGrpc" : "fromGrpcFromDto";
                body.addStatement("$T commandInput = inputMapper.$L(input)", domainInputType, from);
            }
            String outputConversion = transportMode == PipelineTransport.LOCAL ? ""
                : transportMode == PipelineTransport.REST ? ".map(outputMapper::toExternal)"
                : normalizedOutput ? ".map(outputMapper::toGrpc)" : ".map(outputMapper::toDtoToGrpc)";
            body.addStatement("return deferredCompletion.<$T, $T>execute(completion, commandInput, endpointResolver, "
                    + "callback -> support.<$T, $T>execute($L, commandIdGenerator, commandInput, callback))$L",
                domainInputType, domainOutputType, domainInputType, model.outboundDomainType(), descriptor, outputConversion);
            return body.build();
        }
        if (transportMode == PipelineTransport.LOCAL) {
            body.addStatement("return support.execute($L, commandIdGenerator, input)", descriptor);
            return body.build();
        }
        if (transportMode == PipelineTransport.REST) {
            body.addStatement("$T commandInput = inputMapper.fromExternal(input)", domainInputType)
                .addStatement("return support.<$T, $T>execute($L, commandIdGenerator, commandInput)\n"
                        + "    .map(commandOutput -> outputMapper.toExternal(commandOutput))",
                    domainInputType,
                    domainOutputType,
                    descriptor);
            return body.build();
        }
        String fromGrpc = normalizedInput ? "fromGrpc" : "fromGrpcFromDto";
        String toGrpc = normalizedOutput ? "toGrpc" : "toDtoToGrpc";
        body.addStatement("$T commandInput = inputMapper.$L(input)", domainInputType, fromGrpc)
            .addStatement("return support.<$T, $T>execute($L, commandIdGenerator, commandInput)\n"
                    + "    .map(commandOutput -> outputMapper.$L(commandOutput))",
                domainInputType,
                domainOutputType,
                descriptor,
                toGrpc);
        return body.build();
    }

    private void addCallbackCompletion(TypeSpec.Builder type, PipelineStepModel model) {
        var completion = model.deferredCompletionSelection().orElseThrow();
        var callback = completion.callback().orElseThrow();
        var selected = callback.operation();
        ClassName descriptorType = ClassName.get("org.pipelineframework.awaitable", "AwaitCompletionDescriptor");
        type.addAnnotation(ClassName.get("io.quarkus.runtime", "Startup"))
            .addSuperinterface(ClassName.get("org.pipelineframework.cache", "CacheReadBypass"))
            .addField(FieldSpec.builder(ClassName.get("org.pipelineframework.awaitable", "CommandDeferredCompletionSupport"),
                "deferredCompletion").addAnnotation(ClassName.get("jakarta.inject", "Inject")).build())
            .addField(FieldSpec.builder(ClassName.get("org.pipelineframework.awaitable", "AwaitCompletionDescriptorRegistry"),
                "completionRegistry").addAnnotation(ClassName.get("jakarta.inject", "Inject")).build())
            .addField(FieldSpec.builder(callback.endpointResolver(), "endpointResolver")
                .addAnnotation(ClassName.get("jakarta.inject", "Inject")).build())
            .addField(FieldSpec.builder(callback.authenticator(), "authenticator")
                .addAnnotation(ClassName.get("jakarta.inject", "Inject")).build())
            .addField(FieldSpec.builder(descriptorType, "completion", Modifier.PRIVATE).build());
        CodeBlock callbackSelection = CodeBlock.of(
            "new $T($T.of($S), new $T($T.of($S), $S, $T.COMMAND, $L), $L, "
                + "new $T($S, new $T($S, $T.empty()), $L), $S, $S)",
            ClassName.get("org.pipelineframework.awaitable", "ConnectorCallbackSelection"),
            ClassName.get("org.pipelineframework.connector", "ConnectorBindingName"), selected.binding().value(),
            ClassName.get("org.pipelineframework.connector", "ConnectorOperationIdentity"),
            ClassName.get("org.pipelineframework.connector", "ConnectorProviderId"), selected.operation().providerId().value(),
            selected.operation().operationId(), ClassName.get("org.pipelineframework.connector", "ConnectorOperationKind"),
            selected.operation().majorVersion(), selected.providerMajorVersion(),
            ClassName.get("org.pipelineframework.connector", "ConnectorOperationCallbackDescriptor"), callback.descriptor().id(),
            ClassName.get("org.pipelineframework.connector", "ConnectorOperationTypeContract"), callback.descriptor().typeContract().inputType(),
            Optional.class, callback.descriptor().required(), callback.endpointResolver().canonicalName(), callback.authenticator().canonicalName());
        CodeBlock descriptor = CodeBlock.of(
            "new $T($S, $S, $S, $S, $T.parse($S), $S, $S, $T.of(), $T.of(), $S, $S, "
                + "$T.identity(), $T.identity(), $S, ($T<Object, Object, Object>) ($T<?, ?, ?>) new $T(), true, $T.of($L))",
            descriptorType, model.serviceName(), model.inboundDomainType().toString(), completion.finalOutputType().toString(),
            "ONE_TO_ONE", java.time.Duration.class, completion.timeout().toString(), completion.correlationStrategy(), "",
            java.util.Map.class, java.util.List.class, model.inboundDomainType().toString(), completion.completionPayloadType().orElseThrow().toString(),
            java.util.function.Function.class, java.util.function.Function.class, completion.completionProjector().orElseThrow().canonicalName(),
            ClassName.get("org.pipelineframework.awaitable", "AwaitCompletionProjector"),
            ClassName.get("org.pipelineframework.awaitable", "AwaitCompletionProjector"),
            completion.completionProjector().orElseThrow(), Optional.class, callbackSelection);
        type.addMethod(MethodSpec.methodBuilder("registerCompletion").addAnnotation(ClassName.get("jakarta.annotation", "PostConstruct"))
            .addStatement("completion = completionRegistry.register($L)", descriptor).build());
    }

    private CodeBlock nativeDescriptor(
        ConnectorOperationSelection selection,
        String inputType,
        String outputType,
        String commandIdGenerator
    ) {
        ConnectorOperationSelection.CommandSelection command = selection.command().orElseThrow(() ->
            new IllegalArgumentException("Command client step requires Command connector selection semantics"));
        org.pipelineframework.connector.CommandPolicy policy = command.policy();
        CodeBlock selector = CodeBlock.of(
            "new $T($T.of($T.of($S)), new $T($T.of($S), $S, $T.COMMAND, $L), $L, $L)",
            ClassName.get("org.pipelineframework.command", "NativeCommandSelector"),
            Optional.class,
            org.pipelineframework.connector.ConnectorBindingName.class,
            selection.binding().value(),
            org.pipelineframework.connector.ConnectorOperationIdentity.class,
            org.pipelineframework.connector.ConnectorProviderId.class,
            selection.operation().providerId().value(),
            selection.operation().operationId(),
            org.pipelineframework.connector.ConnectorOperationKind.class,
            selection.operation().majorVersion(),
            selection.providerMajorVersion(),
            commandPolicy(policy));
        return CodeBlock.of(
            "$T.nativeCommand($S, $L, $S, $S, $S, $T.$L, $L)",
            ClassName.get("org.pipelineframework.command", "CommandDescriptor"),
            selection.runtimeStepId(), selector, inputType, outputType, commandIdGenerator,
            org.pipelineframework.command.CommandDuplicatePolicy.class,
            command.duplicatePolicy().name(),
            JavaPoetLiteral.value(selection.operationConfiguration()));
    }

    private static CodeBlock commandPolicy(org.pipelineframework.connector.CommandPolicy policy) {
        return CodeBlock.of(
            "new $T($L, $L, $L, $L, $L, $L)",
            org.pipelineframework.connector.CommandPolicy.class,
            policy.requireRetryRedrive(),
            policy.requireIdempotency(),
            policy.requireReconciliation(),
            optionalEnum(policy.requiredExecutionPosture(), org.pipelineframework.connector.CommandExecutionPosture.class),
            optionalEnum(policy.minimumMachineConfirmation(), org.pipelineframework.connector.CommandMachineConfirmation.class),
            policy.requireUserConfirmation());
    }

    private static <T extends Enum<T>> CodeBlock optionalEnum(Optional<T> value, Class<T> type) {
        return value
            .map(entry -> CodeBlock.of("$T.of($T.$L)", Optional.class, type, entry.name()))
            .orElseGet(() -> CodeBlock.of("$T.empty()", Optional.class));
    }

    private PipelineConfigHints resolveConfigHints(GenerationContext ctx) {
        if (ctx.transportMode() != null) {
            String basePackage = ctx.pipelineBasePackage() == null || ctx.pipelineBasePackage().isBlank()
                ? null
                : ctx.pipelineBasePackage();
            return new PipelineConfigHints(ctx.transportMode(), basePackage);
        }
        Map<String, String> options = ctx.compilerOptions().asMap();
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
            return new PipelineYamlConfigLoader(ctx.compilerOptions().asMap()::get, System::getenv)
                .load(Path.of(configPath));
        } catch (RuntimeException ex) {
            if (ctx.compilerServices().available() && ctx.compilerDiagnostics() != null) {
                ctx.compilerDiagnostics().error(
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

}
