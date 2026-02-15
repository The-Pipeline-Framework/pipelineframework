package org.pipelineframework.processor.phase;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import javax.annotation.processing.Messager;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;

import org.pipelineframework.annotation.PipelineOrchestrator;
import org.pipelineframework.annotation.PipelinePlugin;
import org.pipelineframework.config.PlatformMode;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.PipelineCompilationPhase;
import org.pipelineframework.processor.config.PipelineStepConfigLoader;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineOrchestratorModel;
import org.pipelineframework.processor.ir.TransportMode;
import org.pipelineframework.processor.mapping.PipelineRuntimeMapping;

/**
 * Discovers and loads pipeline configuration, aspects, and semantic models.
 * This phase focuses solely on discovery without performing validation or code generation.
 */
public class PipelineDiscoveryPhase implements PipelineCompilationPhase {
    private static final PipelineStepConfigLoader.StepConfig DEFAULT_STEP_CONFIG =
        new PipelineStepConfigLoader.StepConfig("", "GRPC", "COMPUTE", List.of(), List.of());

    private final DiscoveryPathResolver pathResolver;
    private final DiscoveryConfigLoader configLoader;
    private final TransportPlatformResolver transportPlatformResolver;

    /**
     * Creates a new PipelineDiscoveryPhase with default collaborators.
     */
    public PipelineDiscoveryPhase() {
        this(new DiscoveryPathResolver(), new DiscoveryConfigLoader(), new TransportPlatformResolver());
    }

    /**
     * Creates a new PipelineDiscoveryPhase with explicit collaborators.
     */
    PipelineDiscoveryPhase(
            DiscoveryPathResolver pathResolver,
            DiscoveryConfigLoader configLoader,
            TransportPlatformResolver transportPlatformResolver) {
        this.pathResolver = Objects.requireNonNull(pathResolver, "pathResolver must not be null");
        this.configLoader = Objects.requireNonNull(configLoader, "configLoader must not be null");
        this.transportPlatformResolver = Objects.requireNonNull(transportPlatformResolver, "transportPlatformResolver must not be null");
    }

    @Override
    public String name() {
        return "Pipeline Discovery Phase";
    }

    /**
     * Discovers pipeline annotations, configuration files, aspects, transport/platform modes, and orchestrator models,
     * then stores the discovered artifacts on the provided compilation context.
     *
     * @param ctx the compilation context used to read the processing environment and options and to receive discovered
     *            artifacts (generated sources root, module directory/name, plugin host flag, aspect models,
     *            template config, runtime mapping, transport/platform modes, and orchestrator models)
     * @throws Exception if a fatal error occurs while locating or loading required configuration or models
     */
    @Override
    public void execute(PipelineCompilationContext ctx) throws Exception {
        Map<String, String> options = ctx.getProcessingEnv() != null ? ctx.getProcessingEnv().getOptions() : Map.of();
        Messager messager = ctx.getProcessingEnv() != null ? ctx.getProcessingEnv().getMessager() : new NoOpMessager();

        // Discover annotated elements - handle null round environment gracefully
        Set<? extends Element> orchestratorElements =
            ctx.getRoundEnv() != null ? ctx.getRoundEnv().getElementsAnnotatedWith(PipelineOrchestrator.class) : Set.of();
        Set<? extends Element> pluginElements =
            ctx.getRoundEnv() != null ? ctx.getRoundEnv().getElementsAnnotatedWith(PipelinePlugin.class) : Set.of();

        // Resolve generated sources root and module directory
        Path generatedSourcesRoot = pathResolver.resolveGeneratedSourcesRoot(options);
        ctx.setGeneratedSourcesRoot(generatedSourcesRoot);

        Path moduleDir = pathResolver.resolveModuleDir(generatedSourcesRoot);
        ctx.setModuleDir(moduleDir);
        ctx.setModuleName(pathResolver.resolveModuleName(options));

        // Check if this is a plugin host
        ctx.setPluginHost(!pluginElements.isEmpty());

        // Resolve pipeline config path (used by multiple loaders)
        Optional<Path> configPath = configLoader.resolvePipelineConfigPath(options, moduleDir, messager);

        // Load pipeline aspects
        ctx.setAspectModels(configPath.isPresent() ? configLoader.loadAspects(configPath.get(), messager) : List.of());

        // Load pipeline template config
        ctx.setPipelineTemplateConfig(configPath.isPresent() ? configLoader.loadTemplateConfig(configPath.get(), messager) : null);

        // Load runtime mapping config (optional)
        ctx.setRuntimeMapping(configLoader.loadRuntimeMapping(moduleDir, messager));

        // Determine transport and platform modes
        PipelineStepConfigLoader.StepConfig stepConfig = configPath.isPresent()
            ? configLoader.loadStepConfig(configPath.get(), System::getProperty, System::getenv, messager)
            : null;
        if (stepConfig == null) {
            stepConfig = DEFAULT_STEP_CONFIG;
        }
        ctx.setTransportMode(transportPlatformResolver.resolveTransport(stepConfig.transport(), messager));
        ctx.setPlatformMode(transportPlatformResolver.resolvePlatform(stepConfig.platform(), messager));

        // Discover orchestrator models if present
        ctx.setOrchestratorModels(discoverOrchestratorModels(ctx, orchestratorElements));
    }

    /**
     * Discover and build simple PipelineOrchestratorModel instances from elements annotated with @PipelineOrchestrator.
     */
    private List<PipelineOrchestratorModel> discoverOrchestratorModels(
            PipelineCompilationContext ctx,
            Set<? extends Element> orchestratorElements) {
        if (orchestratorElements == null || orchestratorElements.isEmpty()) {
            return List.of();
        }

        List<PipelineOrchestratorModel> models = new ArrayList<>();

        for (Element element : orchestratorElements) {
            PipelineOrchestrator annotation = element.getAnnotation(PipelineOrchestrator.class);
            if (annotation == null) {
                continue;
            }

            String serviceName = element.getSimpleName().toString() + "Orchestrator";
            String servicePackage = element instanceof TypeElement typeElement
                ? computeServicePackage(typeElement.getQualifiedName().toString(), element.getSimpleName().toString())
                : "org.pipelineframework.orchestrator.service";

            Set<GenerationTarget> enabledTargets;
            if (ctx.isTransportModeLocal()) {
                enabledTargets = Set.of();
            } else if (ctx.isTransportModeRest()) {
                enabledTargets = Set.of(GenerationTarget.REST_RESOURCE);
            } else {
                enabledTargets = Set.of(GenerationTarget.GRPC_SERVICE);
            }

            models.add(new PipelineOrchestratorModel(serviceName, servicePackage, enabledTargets, annotation.generateCli()));
        }

        return models;
    }

    /**
     * Computes the service package by finding the last occurrence of the simple class name
     * in the qualified name and replacing it with ".orchestrator.service".
     *
     * @param qualifiedName the fully qualified name of the type
     * @param simpleName the simple name of the type
     * @return the computed service package
     */
    private String computeServicePackage(String qualifiedName, String simpleName) {
        String classNameWithDot = "." + simpleName;
        int lastDotIndex = qualifiedName.lastIndexOf(classNameWithDot);
        if (lastDotIndex != -1) {
            return qualifiedName.substring(0, lastDotIndex) + ".orchestrator.service";
        }
        // Fallback to the original qualified name if the simple name isn't found at the end
        return "org.pipelineframework.orchestrator.service";
    }
}
