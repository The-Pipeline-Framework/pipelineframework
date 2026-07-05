package org.pipelineframework.processor.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Logger;
import javax.tools.Diagnostic;
import javax.annotation.processing.ProcessingEnvironment;
import javax.tools.StandardLocation;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;
import org.pipelineframework.config.pipeline.PipelineYamlConfig;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLoader;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLocator;
import org.pipelineframework.config.pipeline.PipelineYamlStep;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.PipelineTransport;
import org.pipelineframework.processor.routing.PipelineBranchingPlan;

/**
 * Generates META-INF/pipeline/branching.json for branch-aware linear routing.
 */
public final class PipelineBranchingMetadataGenerator {

    private static final String RESOURCE = "META-INF/pipeline/branching.json";
    private static final Logger LOGGER = Logger.getLogger(PipelineBranchingMetadataGenerator.class.getName());

    private final ProcessingEnvironment processingEnv;
    private final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    public PipelineBranchingMetadataGenerator(ProcessingEnvironment processingEnv) {
        this.processingEnv = processingEnv;
    }

    public void writeBranchingMetadata(PipelineCompilationContext ctx) throws IOException {
        if (ctx == null || ctx.getBranchingPlan() == null || !ctx.getBranchingPlan().branchAware()) {
            return;
        }
        PipelineBranchingPlan plan = ctx.getBranchingPlan();
        List<PipelineStepModel> orderedModels = orderedModels(ctx);
        if (orderedModels.isEmpty()) {
            return;
        }
        Map<String, PipelineStepModel> modelsByStepName = indexModelsByStepName(orderedModels);
        List<StepMetadata> steps = new ArrayList<>();
        for (PipelineBranchingPlan.BranchStep step : plan.steps()) {
            PipelineStepModel model = modelsByStepName.get(normalizeStepToken(step.stepName()));
            if (model == null) {
                warn(ctx, "Branch-aware step '" + step.stepName()
                    + "' could not be matched to a runtime step model while generating branching metadata.");
                continue;
            }
            String runtimeStepClass = runtimeStepClass(model, ctx);
            boolean transportMappedRuntime = usesTransportMappedRuntime(model, ctx);
            String inputRuntimeClass = runtimeInputType(model, ctx, transportMappedRuntime).orElse(null);
            List<String> acceptedRuntimeClasses = step.acceptedDomainTypes().stream()
                .map(type -> runtimeAcceptedType(type, ctx, transportMappedRuntime))
                .toList();
            steps.add(new StepMetadata(
                step.index(),
                step.stepName(),
                runtimeStepClass,
                inputRuntimeClass,
                step.acceptedContractTypes(),
                acceptedRuntimeClasses,
                step.terminal()));
        }
        if (steps.isEmpty()) {
            return;
        }
        BranchingMetadata metadata = new BranchingMetadata(plan.terminalStepIndex(), steps);
        if (processingEnv != null) {
            javax.tools.FileObject resourceFile = processingEnv.getFiler()
                .createResource(StandardLocation.CLASS_OUTPUT, "", RESOURCE, (javax.lang.model.element.Element[]) null);
            try (var writer = resourceFile.openWriter()) {
                writer.write(gson.toJson(metadata));
            }
        }
    }

    private List<PipelineStepModel> orderedModels(PipelineCompilationContext ctx) {
        List<PipelineStepModel> stepModels = ctx.getStepModels() == null ? List.of() : ctx.getStepModels();
        Optional<PipelineYamlConfig> config = loadPipelineConfig(ctx);
        if (config.isEmpty() || config.orElseThrow().steps() == null || config.orElseThrow().steps().isEmpty()) {
            return stepModels.stream().filter(model -> !model.sideEffect()).toList();
        }
        if (stepModels.isEmpty()) {
            return List.of();
        }
        Map<String, PipelineStepModel> byToken = new LinkedHashMap<>();
        for (PipelineStepModel model : stepModels) {
            if (model.sideEffect()) {
                continue;
            }
            byToken.put(normalizeStepToken(stepTokenFromModel(model)), model);
            byToken.put(normalizeStepToken(stripTrailingService(model.generatedName())), model);
            byToken.put(normalizeStepToken(model.serviceName()), model);
        }
        List<PipelineStepModel> ordered = new ArrayList<>();
        Set<PipelineStepModel> added = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        for (PipelineYamlStep step : config.orElseThrow().steps()) {
            if (step == null || step.name() == null) {
                continue;
            }
            PipelineStepModel model = byToken.get(normalizeStepToken(step.name()));
            if (model != null && added.add(model)) {
                ordered.add(model);
            }
        }
        for (PipelineStepModel model : stepModels) {
            if (!model.sideEffect() && added.add(model)) {
                ordered.add(model);
            }
        }
        return ordered;
    }

    private Map<String, PipelineStepModel> indexModelsByStepName(List<PipelineStepModel> orderedModels) {
        Map<String, PipelineStepModel> models = new LinkedHashMap<>();
        for (PipelineStepModel model : orderedModels) {
            models.putIfAbsent(normalizeStepToken(stepTokenFromModel(model)), model);
            models.putIfAbsent(normalizeStepToken(stripTrailingService(model.generatedName())), model);
            models.putIfAbsent(normalizeStepToken(model.serviceName()), model);
        }
        return models;
    }

    private Optional<PipelineYamlConfig> loadPipelineConfig(PipelineCompilationContext ctx) {
        Optional<java.nio.file.Path> configPath = resolvePipelineConfigPath(ctx);
        if (configPath.isEmpty()) {
            return Optional.empty();
        }
        PipelineYamlConfigLoader loader = processingEnv != null
            ? new PipelineYamlConfigLoader(processingEnv.getOptions()::get, System::getenv)
            : new PipelineYamlConfigLoader(key -> null, System::getenv);
        return Optional.of(loader.load(configPath.get()));
    }

    private Optional<java.nio.file.Path> resolvePipelineConfigPath(PipelineCompilationContext ctx) {
        Map<String, String> options = processingEnv != null ? processingEnv.getOptions() : Map.of();
        String explicit = options.get("pipeline.config");
        if (explicit != null && !explicit.isBlank()) {
            java.nio.file.Path explicitPath = java.nio.file.Path.of(explicit.trim());
            if (!explicitPath.isAbsolute()) {
                if (ctx.getModuleDir() == null) {
                    LOGGER.warning("pipeline.config provided as relative path but moduleDir is null: " + explicit);
                    return Optional.empty();
                }
                explicitPath = ctx.getModuleDir().resolve(explicitPath).normalize();
            }
            if (java.nio.file.Files.exists(explicitPath)) {
                return Optional.of(explicitPath);
            }
            LOGGER.warning("pipeline.config path not found: " + explicitPath);
        }
        if (ctx.getModuleDir() == null) {
            return Optional.empty();
        }
        return new PipelineYamlConfigLocator().locate(ctx.getModuleDir());
    }

    private String runtimeStepClass(PipelineStepModel model, PipelineCompilationContext ctx) {
        if (!usesTransportMappedRuntime(model, ctx) && model.serviceClassName() != null) {
            return model.serviceClassName().canonicalName();
        }
        return clientClass(model, ctx);
    }

    private boolean usesTransportMappedRuntime(PipelineStepModel model, PipelineCompilationContext ctx) {
        if (ctx.isOrchestratorGenerated()) {
            return true;
        }
        return model.enabledTargets().contains(GenerationTarget.AWAIT_CLIENT_STEP)
            || model.enabledTargets().contains(GenerationTarget.COMMAND_CLIENT_STEP)
            || model.enabledTargets().contains(GenerationTarget.QUERY_CLIENT_STEP);
    }

    private String clientClass(PipelineStepModel model, PipelineCompilationContext ctx) {
        return ClientStepClassNames.className(
            model,
            java.util.Objects.requireNonNullElse(ctx.getTransportMode(), PipelineTransport.GRPC));
    }

    private String runtimeAcceptedType(ClassName domainType, PipelineCompilationContext ctx, boolean transportMappedRuntime) {
        if (!transportMappedRuntime) {
            return domainType.reflectionName();
        }
        PipelineTransport transportMode = java.util.Objects.requireNonNullElse(ctx.getTransportMode(), PipelineTransport.GRPC);
        TypeName transportType = clientStepType(domainType, transportMode, pipelineBasePackage(ctx, domainType));
        if (transportType instanceof ClassName className) {
            if (transportMode == PipelineTransport.GRPC
                && ctx.getProcessingEnv() != null
                && ctx.getProcessingEnv().getElementUtils() != null) {
                javax.lang.model.element.TypeElement element = ctx.getProcessingEnv().getElementUtils()
                    .getTypeElement(className.canonicalName());
                if (element == null) {
                    throw new IllegalStateException("Branch-aware step accepted gRPC runtime type '"
                        + className.canonicalName() + "' (derived from domain type '" + domainType.reflectionName()
                        + "') could not be resolved. Ensure gRPC descriptor-set bindings are generated before compiling the pipeline.");
                }
            }
            return className.reflectionName();
        }
        return transportType.toString();
    }

    private Optional<String> runtimeInputType(PipelineStepModel model, PipelineCompilationContext ctx, boolean transportMappedRuntime) {
        TypeName inputType = model.inputMapping() == null ? null : model.inputMapping().domainType();
        if (!(inputType instanceof ClassName className)) {
            return inputType == null ? Optional.empty() : Optional.of(inputType.toString());
        }
        if (!transportMappedRuntime) {
            return Optional.of(className.reflectionName());
        }
        TypeName transportType = clientStepType(className, java.util.Objects.requireNonNullElse(
            ctx.getTransportMode(),
            PipelineTransport.GRPC), pipelineBasePackage(ctx, className));
        if (transportType instanceof ClassName transportClassName) {
            return Optional.of(transportClassName.reflectionName());
        }
        return Optional.of(transportType.toString());
    }

    private String pipelineBasePackage(PipelineCompilationContext ctx, ClassName domainType) {
        if (ctx.getPipelineTemplateConfig() instanceof org.pipelineframework.config.template.PipelineTemplateConfig templateConfig
            && templateConfig.basePackage() != null
            && !templateConfig.basePackage().isBlank()) {
            return templateConfig.basePackage();
        }
        String packageName = domainType.packageName();
        return packageName.endsWith(".common.domain")
            ? packageName.substring(0, packageName.length() - ".common.domain".length())
            : packageName;
    }

    private TypeName clientStepType(TypeName domainType, PipelineTransport transportMode, String pipelineBasePackage) {
        if (!(domainType instanceof ClassName className)) {
            return domainType;
        }
        String basePackage = pipelineBasePackage == null || pipelineBasePackage.isBlank()
            ? className.packageName().replaceFirst("\\.common\\.domain$", "")
            : pipelineBasePackage;
        return switch (transportMode) {
            case LOCAL -> className;
            case REST -> ClassName.get(basePackage + ".common.dto", className.simpleName() + "Dto");
            case GRPC -> ClassName.get(basePackage + ".grpc", "PipelineTypes", className.simpleName());
        };
    }

    private static String stepTokenFromModel(PipelineStepModel model) {
        String token = stripTrailingService(model.generatedName());
        return token.startsWith("Process") && token.length() > "Process".length()
            ? token.substring("Process".length())
            : token;
    }

    private static String stripTrailingService(String value) {
        if (value == null) {
            return "";
        }
        return value.endsWith("Service") ? value.substring(0, value.length() - "Service".length()) : value;
    }

    private static String normalizeStepToken(String value) {
        if (value == null) {
            return "";
        }
        return value.replaceAll("[^A-Za-z0-9]", "").toLowerCase(java.util.Locale.ROOT);
    }

    private void warn(PipelineCompilationContext ctx, String message) {
        if (processingEnv != null && processingEnv.getMessager() != null) {
            processingEnv.getMessager().printMessage(Diagnostic.Kind.WARNING, message);
        } else {
            LOGGER.warning(message);
        }
    }

    private record BranchingMetadata(
        int terminalStepIndex,
        List<StepMetadata> steps
    ) {
    }

    private record StepMetadata(
        int index,
        String step,
        String runtimeStepClass,
        String inputRuntimeClass,
        List<String> acceptedContracts,
        List<String> acceptedRuntimeClasses,
        boolean terminal
    ) {
    }
}
