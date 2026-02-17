package org.pipelineframework.processor.phase;

import java.io.IOException;
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
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.util.Elements;
import javax.tools.Diagnostic;

import org.pipelineframework.annotation.PipelineOrchestrator;
import org.pipelineframework.annotation.PipelinePlugin;
import org.pipelineframework.config.PlatformMode;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLocator;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.config.template.PipelineTemplateConfigLoader;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.PipelineCompilationPhase;
import org.pipelineframework.processor.config.PipelineAspectConfigLoader;
import org.pipelineframework.processor.config.PipelineRuntimeMappingLoader;
import org.pipelineframework.processor.config.PipelineRuntimeMappingLocator;
import org.pipelineframework.processor.config.PipelineStepConfigLoader;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineAspectModel;
import org.pipelineframework.processor.ir.PipelineOrchestratorModel;
import org.pipelineframework.processor.ir.StepDefinition;
import org.pipelineframework.processor.ir.TransportMode;
import org.pipelineframework.processor.mapping.PipelineRuntimeMapping;
import org.pipelineframework.processor.parser.StepDefinitionParser;

/**
 * Discovers and loads pipeline configuration, aspects, and semantic models.
 * This phase focuses solely on discovery without performing validation or code generation.
 */
public class PipelineDiscoveryPhase implements PipelineCompilationPhase {
    private static final PipelineStepConfigLoader.StepConfig DEFAULT_STEP_CONFIG =
        new PipelineStepConfigLoader.StepConfig("", "GRPC", "COMPUTE", List.of(), List.of());
    private final DiscoveryPathResolver discoveryPathResolver;
    private final DiscoveryConfigLoader discoveryConfigLoader;
    private final TransportPlatformResolver transportPlatformResolver;

    /**
     * Creates a new PipelineDiscoveryPhase.
     */
    public PipelineDiscoveryPhase() {
        this(new DiscoveryPathResolver(), new DiscoveryConfigLoader(), new TransportPlatformResolver());
    }

    /**
     * Creates a new PipelineDiscoveryPhase with injected collaborators.
     *
     * @param discoveryPathResolver path resolver collaborator
     * @param discoveryConfigLoader config loader collaborator
     * @param transportPlatformResolver transport/platform resolver collaborator
     */
    public PipelineDiscoveryPhase(
            DiscoveryPathResolver discoveryPathResolver,
            DiscoveryConfigLoader discoveryConfigLoader,
            TransportPlatformResolver transportPlatformResolver) {
        this.discoveryPathResolver = Objects.requireNonNull(discoveryPathResolver, "discoveryPathResolver");
        this.discoveryConfigLoader = Objects.requireNonNull(discoveryConfigLoader, "discoveryConfigLoader");
        this.transportPlatformResolver = Objects.requireNonNull(transportPlatformResolver, "transportPlatformResolver");
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
        // Discover annotated elements - handle null round environment gracefully
        Set<? extends Element> orchestratorElements =
            ctx.getRoundEnv() != null ? ctx.getRoundEnv().getElementsAnnotatedWith(PipelineOrchestrator.class) : Set.of();
        Set<? extends Element> pluginElements =
            ctx.getRoundEnv() != null ? ctx.getRoundEnv().getElementsAnnotatedWith(PipelinePlugin.class) : Set.of();

        Map<String, String> options = ctx.getProcessingEnv() != null ? ctx.getProcessingEnv().getOptions() : Map.of();
        Messager messager = ctx.getProcessingEnv() != null ? ctx.getProcessingEnv().getMessager() : null;

        // Resolve generated sources root and module directory early (needed for config discovery)
        Path generatedSourcesRoot = discoveryPathResolver.resolveGeneratedSourcesRoot(options);
        ctx.setGeneratedSourcesRoot(generatedSourcesRoot);

        Path moduleDir = discoveryPathResolver.resolveModuleDir(generatedSourcesRoot);
        ctx.setModuleDir(moduleDir);
        ctx.setModuleName(discoveryPathResolver.resolveModuleName(options));

        Optional<Path> configPath = discoveryConfigLoader.resolvePipelineConfigPath(options, moduleDir, messager);

        // Check if this is a plugin host
        boolean isPluginHost = !pluginElements.isEmpty();
        ctx.setPluginHost(isPluginHost);

        // Load pipeline aspects
        List<PipelineAspectModel> aspects = loadPipelineAspects(configPath, messager);
        ctx.setAspectModels(aspects);

        // Load pipeline template config
        PipelineTemplateConfig templateConfig = loadPipelineTemplateConfig(configPath, messager);
        ctx.setPipelineTemplateConfig(templateConfig);

        // Parse step definitions from YAML
        List<StepDefinition> stepDefinitions = parseStepDefinitions(configPath, messager);
        ctx.setStepDefinitions(stepDefinitions);

        // Load runtime mapping config (optional)
        PipelineRuntimeMapping runtimeMapping = loadRuntimeMapping(moduleDir, messager);
        ctx.setRuntimeMapping(runtimeMapping);

        // Determine transport and platform modes
        PipelineStepConfigLoader.StepConfig stepConfig = loadPipelineStepConfig(configPath, messager);
        TransportMode transportMode = transportPlatformResolver.resolveTransport(stepConfig.transport(), messager);
        ctx.setTransportMode(transportMode);
        PlatformMode platformMode = transportPlatformResolver.resolvePlatform(stepConfig.platform(), messager);
        ctx.setPlatformMode(platformMode);

        // Discover orchestrator models if present
        List<PipelineOrchestratorModel> orchestratorModels = discoverOrchestratorModels(ctx, orchestratorElements);
        ctx.setOrchestratorModels(orchestratorModels);
    }

    /**
     * Loads pipeline aspect models from the resolved pipeline configuration file.
     *
     * @param ctx the pipeline compilation context used to resolve the config path and report diagnostics
     * @return a list of loaded {@code PipelineAspectModel} instances; an empty list if no pipeline config is found
     * @throws Exception if loading fails; an error diagnostic is reported to the processing environment's messager when available
     */
    private List<PipelineAspectModel> loadPipelineAspects(Optional<Path> configPath, Messager messager) {
        if (configPath.isEmpty()) {
            return List.of();
        }
        return discoveryConfigLoader.loadAspects(configPath.get(), messager);
    }

    /**
     * Locates and loads the pipeline template configuration from the module directory.
     *
     * @param ctx compilation context used to determine the module directory and to report warnings
     * @return the loaded PipelineTemplateConfig, or `null` if no configuration is present or if loading fails (a warning is emitted via the processing environment when available)
     */
    private PipelineTemplateConfig loadPipelineTemplateConfig(Optional<Path> configPath, Messager messager) {
        if (configPath.isEmpty()) {
            return null;
        }
        return discoveryConfigLoader.loadTemplateConfig(configPath.get(), messager);
    }

    /**
     * Resolve pipeline step configuration from the module's pipeline YAML.
     *
     * Locates a pipeline YAML under the context's module directory and loads its step-level
     * configuration (base package, transport, platform, input/output types). When the file is
     * missing or cannot be loaded, returns a default non-null configuration.
     *
     * @param ctx the pipeline compilation context used to locate the module directory and to report warnings
     * @return a non-null {@link org.pipelineframework.processor.config.PipelineStepConfigLoader.StepConfig}
     */
    private PipelineStepConfigLoader.StepConfig loadPipelineStepConfig(Optional<Path> configPath, Messager messager) {
        if (configPath.isEmpty()) {
            return DEFAULT_STEP_CONFIG;
        }
        PipelineStepConfigLoader.StepConfig loaded = discoveryConfigLoader.loadStepConfig(
            configPath.get(),
            key -> null,
            System::getenv,
            messager);
        return loaded != null ? loaded : DEFAULT_STEP_CONFIG;
    }

    /**
     * Discover and build simple PipelineOrchestratorModel instances from elements annotated with @PipelineOrchestrator.
     *
     * @param ctx the compilation context used to determine transport mode and other contextual settings
     * @param orchestratorElements the set of elements to inspect for a PipelineOrchestrator annotation; may be null or empty
     * @return a list of PipelineOrchestratorModel objects created from the annotated elements; an empty list if no orchestrator elements are provided or none contain the annotation
     */
    private List<PipelineOrchestratorModel> discoverOrchestratorModels(
            PipelineCompilationContext ctx, 
            Set<? extends Element> orchestratorElements) {
        if (orchestratorElements == null || orchestratorElements.isEmpty()) {
            return List.of();
        }

        List<PipelineOrchestratorModel> models = new ArrayList<>();
        
        for (Element element : orchestratorElements) {
            PipelineOrchestrator annotation = resolveOrchestratorAnnotation(element);
            if (annotation != null) {
                // For now, we'll create a simple model based on the annotation
                // In a real implementation, this would extract more detailed information
                String serviceName = "OrchestratorService"; // Default name
                String servicePackage = "org.pipelineframework.orchestrator.service";
                if (element instanceof TypeElement typeElement && ctx.getProcessingEnv() != null) {
                    Elements elementUtils = ctx.getProcessingEnv().getElementUtils();
                    if (elementUtils != null) {
                        String packageName = elementUtils.getPackageOf(typeElement).getQualifiedName().toString();
                        servicePackage = packageName + ".orchestrator.service";
                    }
                }
                
                // Determine enabled targets based on transport mode
                // This is simplified - in reality, it would depend on configuration
                var enabledTargets = ctx.isTransportModeLocal()
                    ? java.util.Set.<GenerationTarget>of()
                    : (ctx.isTransportModeRest()
                        ? java.util.Set.of(GenerationTarget.REST_RESOURCE)
                        : java.util.Set.of(GenerationTarget.GRPC_SERVICE));
                
                PipelineOrchestratorModel model = new PipelineOrchestratorModel(
                    serviceName,
                    servicePackage,
                    enabledTargets,
                    annotation.generateCli()
                );
                
                models.add(model);
            }
        }
        
        return models;
    }

    /**
     * Returns the {@code PipelineOrchestrator} annotation from the given element, if present.
     *
     * @param orchestratorElement element to inspect; may be null
     * @return the {@code PipelineOrchestrator} annotation, or {@code null} if the element is null or not annotated
     */
    private PipelineOrchestrator resolveOrchestratorAnnotation(Element orchestratorElement) {
        if (orchestratorElement == null) {
            return null;
        }
        return orchestratorElement.getAnnotation(PipelineOrchestrator.class);
    }

    private PipelineRuntimeMapping loadRuntimeMapping(Path moduleDir, Messager messager) {
        return discoveryConfigLoader.loadRuntimeMapping(moduleDir, messager);
    }

    /**
     * Parse step definitions from the pipeline template configuration.
     *
     * @param ctx the pipeline compilation context
     * @return a list of StepDefinition objects parsed from the template
     * @throws IOException if config resolution fails or parsing step definitions fails
     */
    private List<StepDefinition> parseStepDefinitions(Optional<Path> configPath, Messager messager) {
        if (configPath.isEmpty()) {
            return List.of();
        }

        StepDefinitionParser parser = new StepDefinitionParser((kind, message) -> {
            if (messager != null) {
                messager.printMessage(kind, message);
            }
        });
        try {
            return parser.parseStepDefinitions(configPath.get());
        } catch (IOException e) {
            if (messager != null) {
                messager.printMessage(
                    Diagnostic.Kind.ERROR,
                    "Failed to parse YAML step definitions from " + configPath.get() + ": " + e.getMessage());
            }
            return List.of();
        } catch (Exception e) {
            if (messager != null) {
                messager.printMessage(
                    Diagnostic.Kind.ERROR,
                    "Unexpected error while parsing YAML step definitions from " + configPath.get() + ": " + e.getMessage());
            }
            return List.of();
        }
    }
}
