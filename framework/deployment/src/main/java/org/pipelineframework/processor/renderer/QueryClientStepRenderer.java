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
import org.pipelineframework.config.pipeline.PipelineYamlConnectorBinding;
import org.pipelineframework.config.pipeline.PipelineYamlOperationSelection;
import org.pipelineframework.config.pipeline.PipelineYamlStep;
import org.pipelineframework.connector.ConnectorOperationIdentity;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorProviderManifestLoader;
import org.pipelineframework.connector.QueryCapabilities;
import org.pipelineframework.parallelism.OrderingRequirement;
import org.pipelineframework.parallelism.ThreadSafety;
import org.pipelineframework.processor.PipelineStepProcessor;
import org.pipelineframework.processor.phase.NamingPolicy;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.PipelineTransport;
import org.pipelineframework.step.StepOneToOne;

/**
 * Renders generated captured query client steps.
 */
public class QueryClientStepRenderer {

    public GenerationTarget target() {
        return GenerationTarget.QUERY_CLIENT_STEP;
    }

    public void render(PipelineStepModel model, GenerationContext ctx) throws IOException {
        String baseName = model.generatedName().endsWith("Service")
            ? model.generatedName().substring(0, model.generatedName().length() - "Service".length())
            : model.generatedName();
        String className = baseName + "QueryClientStep";
        PipelineConfigHints configHints = resolveConfigHints(ctx);
        Optional<NativeCacheRequirements> nativeCacheRequirements = resolveNativeCacheRequirements(model, ctx);
        TypeName inputType = clientStepType(model.inboundDomainType(), configHints.transportMode(), configHints.basePackage());
        TypeName outputType = clientStepType(model.outboundDomainType(), configHints.transportMode(), configHints.basePackage());

        FieldSpec support = FieldSpec.builder(ClassName.get("org.pipelineframework.query", "QueryStepSupport"), "support")
            .addAnnotation(ClassName.get("jakarta.inject", "Inject"))
            .build();
        FieldSpec descriptorFactory = FieldSpec.builder(
                ClassName.get("org.pipelineframework.query", "QueryStepDescriptorFactory"),
                "descriptorFactory")
            .addAnnotation(ClassName.get("jakarta.inject", "Inject"))
            .build();

        MethodSpec cacheKeyTargetType = MethodSpec.methodBuilder("cacheKeyTargetType")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(ParameterizedTypeName.get(ClassName.get(Class.class),
                com.squareup.javapoet.WildcardTypeName.subtypeOf(Object.class)))
            .addStatement("return $T.class", outputType)
            .build();

        MethodSpec apply = MethodSpec.methodBuilder("applyOneToOne")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(ParameterizedTypeName.get(ClassName.get(Uni.class), outputType))
            .addParameter(inputType, "input")
            .addStatement("return support.queryOneToOne(descriptorFactory.descriptor($S, $S, $S), input, $T.class)",
                model.serviceName(),
                inputType.toString(),
                outputType.toString(),
                outputType)
            .build();

        TypeSpec.Builder type = TypeSpec.classBuilder(className)
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
            .addMethod(MethodSpec.constructorBuilder().addModifiers(Modifier.PUBLIC).build())
            .addMethod(cacheKeyTargetType)
            .addMethod(apply);

        nativeCacheRequirements.ifPresent(requirements -> type
            .addSuperinterface(ClassName.get("org.pipelineframework.query", "ProviderQueryStep"))
            .addMethod(queryCacheRequirementsMethod(requirements)));

        JavaFile.builder(model.servicePackage() + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX, type.build())
            .build()
            .writeTo(ctx.outputDir());
    }

    /** Renders an adapter that invokes one operation from a compiler-pinned callable catalogue. */
    public void renderDynamicOperation(PipelineStepModel model, GenerationContext ctx) throws IOException {
        String baseName = model.generatedName().endsWith("Service")
            ? model.generatedName().substring(0, model.generatedName().length() - "Service".length())
            : model.generatedName();
        String className = baseName + "DynamicOperationClientStep";
        PipelineConfigHints configHints = resolveConfigHints(ctx);
        TypeName inputType = clientStepType(model.inboundDomainType(), configHints.transportMode(), configHints.basePackage());
        TypeName outputType = clientStepType(model.outboundDomainType(), configHints.transportMode(), configHints.basePackage());
        FieldSpec support = FieldSpec.builder(
                ClassName.get("org.pipelineframework.dispatch", "OperationDispatchSupport"), "support")
            .addAnnotation(ClassName.get("jakarta.inject", "Inject")).build();
        FieldSpec descriptorFactory = FieldSpec.builder(
                ClassName.get("org.pipelineframework.dispatch", "OperationDispatchDescriptorFactory"), "descriptorFactory")
            .addAnnotation(ClassName.get("jakarta.inject", "Inject")).build();
        MethodSpec apply = MethodSpec.methodBuilder("applyOneToOne")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(ParameterizedTypeName.get(ClassName.get(Uni.class), outputType))
            .addParameter(inputType, "input")
            .addStatement("return support.dispatch(descriptorFactory.descriptor($S), input.binding(), input.operation(), input.argumentsJson(), $T.class)",
                model.serviceName(), outputType)
            .build();
        TypeSpec type = TypeSpec.classBuilder(className)
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.enterprise.context", "Dependent")).build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get(Unremovable.class)).build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get("org.pipelineframework.annotation", "GeneratedRole"))
                .addMember("value", "$T.$L", ClassName.get("org.pipelineframework.annotation", "GeneratedRole", "Role"),
                    ctx.role().name()).build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get("org.pipelineframework.annotation", "ParallelismHint"))
                .addMember("ordering", "$T.$L", ClassName.get(OrderingRequirement.class), OrderingRequirement.RELAXED.name())
                .addMember("threadSafety", "$T.$L", ClassName.get(ThreadSafety.class), ThreadSafety.SAFE.name()).build())
            .superclass(ClassName.get("org.pipelineframework.step", "ConfigurableStep"))
            .addSuperinterface(ParameterizedTypeName.get(ClassName.get(StepOneToOne.class), inputType, outputType))
            .addField(support)
            .addField(descriptorFactory)
            .addMethod(MethodSpec.constructorBuilder().addModifiers(Modifier.PUBLIC).build())
            .addMethod(apply)
            .build();
        JavaFile.builder(model.servicePackage() + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX, type)
            .build().writeTo(ctx.outputDir());
    }

    private MethodSpec queryCacheRequirementsMethod(NativeCacheRequirements requirements) {
        QueryCapabilities capabilities = requirements.capabilities();
        CodeBlock maximumCacheAge = capabilities.maximumCacheAge()
            .map(value -> CodeBlock.of("$T.of($T.parse($S))", Optional.class, java.time.Duration.class, value.toString()))
            .orElseGet(() -> CodeBlock.of("$T.empty()", Optional.class));
        CodeBlock maximumNegativeCacheTtl = capabilities.maximumNegativeCacheTtl()
            .map(value -> CodeBlock.of("$T.of($T.parse($S))", Optional.class, java.time.Duration.class, value.toString()))
            .orElseGet(() -> CodeBlock.of("$T.empty()", Optional.class));
        CodeBlock negativeCacheTtl = requirements.negativeCacheTtl()
            .map(value -> CodeBlock.of("$T.of($T.parse($S))", Optional.class, java.time.Duration.class, value.toString()))
            .orElseGet(() -> CodeBlock.of("$T.empty()", Optional.class));
        return MethodSpec.methodBuilder("queryCacheRequirements")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(ClassName.get("org.pipelineframework.query", "QueryCacheRequirements"))
            .addStatement(
                "return new $T(new $T($T.of($S), $S, $T.QUERY, $L), $L, "
                    + "new $T($T.$L, $L, $L), $L)",
                ClassName.get("org.pipelineframework.query", "QueryCacheRequirements"),
                ConnectorOperationIdentity.class,
                ConnectorProviderId.class,
                requirements.providerId(),
                requirements.operationId(),
                ConnectorOperationKind.class,
                requirements.operationMajorVersion(),
                requirements.providerMajorVersion(),
                QueryCapabilities.class,
                org.pipelineframework.connector.QueryCacheability.class,
                capabilities.cacheability().name(),
                maximumCacheAge,
                maximumNegativeCacheTtl,
                negativeCacheTtl)
            .build();
    }

    private Optional<NativeCacheRequirements> resolveNativeCacheRequirements(
        PipelineStepModel model,
        GenerationContext ctx
    ) {
        if (ctx.processingEnv() == null || ctx.processingEnv().getOptions() == null) {
            return Optional.empty();
        }
        String configuredPath = ctx.processingEnv().getOptions().get("pipeline.config");
        if (configuredPath == null || configuredPath.isBlank()) {
            return Optional.empty();
        }
        PipelineYamlConfig config = new PipelineYamlConfigLoader(
            ctx.processingEnv().getOptions()::get, System::getenv).load(Path.of(configuredPath));
        Optional<PipelineYamlStep> selected = config.steps().stream()
            .filter(step -> matchesServiceName(model.serviceName(), step.name()))
            .filter(step -> "query".equalsIgnoreCase(step.kind()))
            .filter(step -> step.operationSelection().isPresent())
            .findFirst();
        if (selected.isEmpty()) {
            return Optional.empty();
        }
        PipelineYamlStep step = selected.orElseThrow();
        PipelineYamlOperationSelection operation = step.operationSelection().orElseThrow();
        PipelineYamlConnectorBinding binding = Optional.ofNullable(config.connectors().get(operation.using()))
            .orElseThrow(() -> new IllegalStateException(
                "Query step " + model.serviceName() + " references unknown connector binding '"
                    + operation.using() + "'"));
        ConnectorOperationIdentity identity = new ConnectorOperationIdentity(
            ConnectorProviderId.of(binding.provider()),
            operation.operation(),
            ConnectorOperationKind.QUERY,
            operation.operationVersion());
        QueryCapabilities capabilities = ConnectorProviderManifestLoader.load(metadataClassLoader())
            .requireQueryCapabilities(identity, binding.version());
        return Optional.of(new NativeCacheRequirements(
            binding.provider(),
            binding.version(),
            operation.operation(),
            operation.operationVersion(),
            capabilities,
            step.negativeCacheTtl()));
    }

    private static ClassLoader metadataClassLoader() {
        return ConnectorProviderManifestLoader.metadataClassLoader(QueryClientStepRenderer.class);
    }

    private static boolean matchesServiceName(String generatedServiceName, String stepName) {
        String serviceName = toServiceName(stepName);
        String compact = serviceName.startsWith("Process") && serviceName.endsWith("Service")
            ? serviceName.substring("Process".length(), serviceName.length() - "Service".length())
            : serviceName;
        return generatedServiceName.equals(serviceName) || generatedServiceName.equals(compact);
    }

    private static String toServiceName(String stepName) {
        if (stepName == null || stepName.isBlank()) {
            return "ProcessStepService";
        }
        String formatted = NamingPolicy.formatForClassName(NamingPolicy.stripProcessPrefix(stepName));
        return formatted.isBlank() ? "ProcessStepService" : "Process" + formatted + "Service";
    }

    private PipelineConfigHints resolveConfigHints(GenerationContext ctx) {
        if (ctx.transportMode() != null && ctx.pipelineBasePackage() != null && !ctx.pipelineBasePackage().isBlank()) {
            return new PipelineConfigHints(ctx.transportMode(), ctx.pipelineBasePackage());
        }
        Map<String, String> options = ctx.processingEnv() == null ? Map.of() : ctx.processingEnv().getOptions();
        PipelineTransport configuredTransport = PipelineTransport.fromStringOptional(
            options == null ? null : options.get("pipeline.transport")).orElse(null);
        String basePackage = null;
        if (options != null) {
            String configPath = options.get("pipeline.config");
            if (configPath != null && !configPath.isBlank()) {
                PipelineYamlConfig config = new PipelineYamlConfigLoader(ctx.processingEnv().getOptions()::get, System::getenv)
                    .load(Path.of(configPath));
                if (configuredTransport == null) {
                    configuredTransport = PipelineTransport.fromStringOptional(config.transport()).orElse(null);
                }
                basePackage = config.basePackage();
            }
        }
        if (configuredTransport == null) {
            configuredTransport = PipelineTransport.GRPC;
        }
        return new PipelineConfigHints(configuredTransport, basePackage);
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
                throw new IllegalStateException(
                    "Cannot determine base package for type " + className
                        + "; configure pipeline basePackage or use named-package domain types");
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
        if (pipelineBasePackage != null && !pipelineBasePackage.isBlank()) {
            return pipelineBasePackage;
        }
        throw new IllegalStateException(
            "Cannot determine base package for type " + className
                + "; package '" + packageName
                + "' does not match .common.domain, .common.dto, or .service and pipeline basePackage is not configured");
    }

    private record PipelineConfigHints(PipelineTransport transportMode, String basePackage) {
    }

    private record NativeCacheRequirements(
        String providerId,
        int providerMajorVersion,
        String operationId,
        int operationMajorVersion,
        QueryCapabilities capabilities,
        Optional<java.time.Duration> negativeCacheTtl
    ) {
    }
}
