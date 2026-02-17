package org.pipelineframework.processor.phase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import javax.annotation.processing.ProcessingEnvironment;
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

        // Resolve generated sources root and module directory early (needed for config discovery)
        Path generatedSourcesRoot = resolveGeneratedSourcesRoot(ctx);
        ctx.setGeneratedSourcesRoot(generatedSourcesRoot);

        Path moduleDir = resolveModuleDir(ctx, generatedSourcesRoot);
        ctx.setModuleDir(moduleDir);
        ctx.setModuleName(resolveModuleName(ctx));

        // Check if this is a plugin host
        boolean isPluginHost = !pluginElements.isEmpty();
        ctx.setPluginHost(isPluginHost);

        // Load pipeline aspects
        List<PipelineAspectModel> aspects = loadPipelineAspects(ctx);
        ctx.setAspectModels(aspects);

        // Load pipeline template config
        PipelineTemplateConfig templateConfig = loadPipelineTemplateConfig(ctx);
        ctx.setPipelineTemplateConfig(templateConfig);

        // Parse step definitions from YAML
        List<StepDefinition> stepDefinitions = parseStepDefinitions(ctx);
        ctx.setStepDefinitions(stepDefinitions);

        // Load runtime mapping config (optional)
        PipelineRuntimeMapping runtimeMapping = loadRuntimeMapping(ctx);
        ctx.setRuntimeMapping(runtimeMapping);

        // Determine transport and platform modes
        PipelineStepConfigLoader.StepConfig stepConfig = loadPipelineStepConfig(ctx);
        TransportMode transportMode = loadPipelineTransport(ctx, stepConfig);
        ctx.setTransportMode(transportMode);
        PlatformMode platformMode = loadPipelinePlatform(ctx, stepConfig);
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
    private List<PipelineAspectModel> loadPipelineAspects(PipelineCompilationContext ctx) {
        Optional<Path> configPath = resolvePipelineConfigPath(ctx);
        if (configPath.isEmpty()) {
            return List.of();
        }

        PipelineAspectConfigLoader loader = new PipelineAspectConfigLoader();
        try {
            return loader.load(configPath.get());
        } catch (Exception e) {
            if (ctx.getProcessingEnv() != null) {
                ctx.getProcessingEnv().getMessager().printMessage(javax.tools.Diagnostic.Kind.ERROR,
                    "Failed to load pipeline aspects from " + configPath.get() + ": " + e.getMessage());
            }
            throw e;
        }
    }

    /**
     * Locates and loads the pipeline template configuration from the module directory.
     *
     * @param ctx compilation context used to determine the module directory and to report warnings
     * @return the loaded PipelineTemplateConfig, or `null` if no configuration is present or if loading fails (a warning is emitted via the processing environment when available)
     */
    private PipelineTemplateConfig loadPipelineTemplateConfig(PipelineCompilationContext ctx) {
        Optional<Path> configPath = resolvePipelineConfigPath(ctx);
        if (configPath.isEmpty()) {
            return null;
        }

        ProcessingEnvironment procEnv = ctx.getProcessingEnv();
        PipelineTemplateConfigLoader loader = procEnv != null
            ? new PipelineTemplateConfigLoader(procEnv.getOptions()::get, System::getenv)
            : new PipelineTemplateConfigLoader(key -> null, System::getenv);
        try {
            return loader.load(configPath.get());
        } catch (Exception e) {
            if (ctx.getProcessingEnv() != null) {
                ctx.getProcessingEnv().getMessager().printMessage(javax.tools.Diagnostic.Kind.WARNING,
                    "Failed to load pipeline template config from " + configPath.get() + ": " + e.getMessage());
            }
            return null;
        }
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
    private PipelineStepConfigLoader.StepConfig loadPipelineStepConfig(PipelineCompilationContext ctx) {
        Optional<Path> configPath = resolvePipelineConfigPath(ctx);
        if (configPath.isEmpty()) {
            return DEFAULT_STEP_CONFIG;
        }

        ProcessingEnvironment stepProcEnv = ctx.getProcessingEnv();
        PipelineStepConfigLoader stepLoader = new PipelineStepConfigLoader(
            stepProcEnv != null ? stepProcEnv.getOptions()::get : key -> null,
            System::getenv,
            stepProcEnv == null ? null : stepProcEnv.getMessager());
        try {
            return stepLoader.load(configPath.get());
        } catch (Exception e) {
            if (ctx.getProcessingEnv() != null) {
                ctx.getProcessingEnv().getMessager().printMessage(javax.tools.Diagnostic.Kind.WARNING,
                    "Failed to load pipeline transport/platform from " + configPath.get() + ": " + e.getMessage());
            }
            return DEFAULT_STEP_CONFIG;
        }
    }

    private TransportMode loadPipelineTransport(
        PipelineCompilationContext ctx,
        PipelineStepConfigLoader.StepConfig stepConfig) {
        String transport = stepConfig.transport();
        if (transport == null || transport.isBlank()) {
            return TransportMode.GRPC;
        }
        Optional<TransportMode> mode = TransportMode.fromStringOptional(transport);
        if (mode.isEmpty()) {
            if (ctx.getProcessingEnv() != null) {
                ctx.getProcessingEnv().getMessager().printMessage(javax.tools.Diagnostic.Kind.WARNING,
                    "Unknown pipeline transport '" + transport + "'; defaulting to GRPC.");
            }
            return TransportMode.GRPC;
        }
        return mode.get();
    }

    private PlatformMode loadPipelinePlatform(
        PipelineCompilationContext ctx,
        PipelineStepConfigLoader.StepConfig stepConfig) {
        String platform = stepConfig.platform();
        if (platform == null || platform.isBlank()) {
            return PlatformMode.COMPUTE;
        }
        Optional<PlatformMode> mode = PlatformMode.fromStringOptional(platform);
        if (mode.isEmpty()) {
            if (ctx.getProcessingEnv() != null) {
                ctx.getProcessingEnv().getMessager().printMessage(
                    javax.tools.Diagnostic.Kind.WARNING,
                    "Unknown pipeline platform '" + platform + "'; defaulting to COMPUTE.");
            }
            return PlatformMode.COMPUTE;
        }
        return mode.get();
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
                String servicePackage = element instanceof TypeElement typeElement 
                    ? typeElement.getQualifiedName().toString().replace("." + element.getSimpleName().toString(), ".orchestrator.service")
                    : "org.pipelineframework.orchestrator.service";
                
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

    /**
     * Loads the pipeline runtime mapping for the current module, if present.
     *
     * Locates a runtime mapping file in the context's module directory and parses it into a
     * PipelineRuntimeMapping. If the context has no module directory or no mapping file is found,
     * this method returns `null`. If loading fails, an error is reported to the processing
     * environment (when available) and the underlying exception is rethrown.
     *
     * @param ctx the compilation context providing the module directory and processing environment
     * @return the loaded PipelineRuntimeMapping, or `null` if none was found or the module directory is unset
     */
    private PipelineRuntimeMapping loadRuntimeMapping(PipelineCompilationContext ctx) {
        PipelineRuntimeMappingLocator locator = new PipelineRuntimeMappingLocator();
        Path moduleDir = ctx.getModuleDir();
        if (moduleDir == null) {
            return null;
        }
        Optional<Path> configPath = locator.locate(moduleDir);
        if (configPath.isEmpty()) {
            return null;
        }

        PipelineRuntimeMappingLoader loader = new PipelineRuntimeMappingLoader();
        try {
            return loader.load(configPath.get());
        } catch (Exception e) {
            if (ctx.getProcessingEnv() != null) {
                ctx.getProcessingEnv().getMessager().printMessage(javax.tools.Diagnostic.Kind.ERROR,
                    "Failed to load runtime mapping from " + configPath.get() + ": " + e.getMessage());
            }
            throw e;
        }
    }

    /**
     * Resolve the pipeline configuration file path from the processing options or by locating
     * a pipeline YAML within the current module directory.
     *
     * <p>The method first checks the processing option `pipeline.config`. If present and non-blank,
     * the value is interpreted as a filesystem path (relative paths are resolved against the
     * context's module directory). If the explicit path exists it is returned; if it does not exist
     * an error message is emitted via the processing environment messager (when available) and an
     * empty Optional is returned. If no explicit option is provided, the method attempts to locate
     * a pipeline YAML using {@code PipelineYamlConfigLocator} within the module directory. If the
     * module directory is not available or no config is found, an empty Optional is returned.
     *
     * @param ctx the compilation context used to read processing options and module directory
     * @return an Optional containing the resolved configuration Path if found, or an empty Optional
     *         if no configuration could be located or an explicit path was missing
     */
    private Optional<Path> resolvePipelineConfigPath(PipelineCompilationContext ctx) {
        Map<String, String> options = ctx.getProcessingEnv() != null ? ctx.getProcessingEnv().getOptions() : Map.of();
        String explicitConfig = options.get("pipeline.config");
        if (explicitConfig != null && !explicitConfig.isBlank()) {
            Path explicitPath = Path.of(explicitConfig.trim());
            if (!explicitPath.isAbsolute() && ctx.getModuleDir() != null) {
                explicitPath = ctx.getModuleDir().resolve(explicitPath).normalize();
            }
            if (Files.exists(explicitPath)) {
                return Optional.of(explicitPath);
            }
            if (ctx.getProcessingEnv() != null) {
                ctx.getProcessingEnv().getMessager().printMessage(
                    Diagnostic.Kind.ERROR,
                    "pipeline.config points to a missing path: '" + explicitConfig + "' (resolved to '" +
                        explicitPath + "')");
            }
            return Optional.empty();
        }
        Path moduleDir = ctx.getModuleDir();
        if (moduleDir == null) {
            return Optional.empty();
        }
        PipelineYamlConfigLocator locator = new PipelineYamlConfigLocator();
        return locator.locate(moduleDir);
    }

    /**
     * Resolve the module name from the processing environment options.
     *
     * Reads the "pipeline.module" option from the compilation context's processing environment,
     * trims surrounding whitespace, and returns the result. If the processing environment is
     * unavailable or the option is missing or blank, returns `null`.
     *
     * @param ctx the compilation context providing access to the processing environment and its options
     * @return the trimmed module name, or `null` if not present or blank
     */
    private String resolveModuleName(PipelineCompilationContext ctx) {
        if (ctx.getProcessingEnv() == null) {
            return null;
        }
        String moduleName = ctx.getProcessingEnv().getOptions().get("pipeline.module");
        if (moduleName == null || moduleName.isBlank()) {
            return null;
        }
        return moduleName.trim();
    }

    /**
     * Resolve the filesystem root directory where generated pipeline sources should be written.
     *
     * Checks processing environment options (when available) for "pipeline.generatedSourcesDir" first,
     * then "pipeline.generatedSourcesRoot" and returns the first non-blank value as a Path.
     * If neither option is present, returns {user.dir}/target/generated-sources/pipeline.
     *
     * @param ctx the compilation context whose processing environment options are consulted (may be null)
     * @return the resolved Path for generated pipeline sources
     */
    private Path resolveGeneratedSourcesRoot(PipelineCompilationContext ctx) {
        java.util.Map<String, String> options = ctx.getProcessingEnv() != null ? ctx.getProcessingEnv().getOptions() : java.util.Map.of();
        String configured = options.get("pipeline.generatedSourcesDir");
        if (configured != null && !configured.isBlank()) {
            return java.nio.file.Paths.get(configured);
        }

        String fallback = options.get("pipeline.generatedSourcesRoot");
        if (fallback != null && !fallback.isBlank()) {
            return java.nio.file.Paths.get(fallback);
        }

        return java.nio.file.Paths.get(System.getProperty("user.dir"), "target", "generated-sources", "pipeline");
    }

    /**
     * Parse step definitions from the pipeline template configuration.
     *
     * @param ctx the pipeline compilation context
     * @return a list of StepDefinition objects parsed from the template
     * @throws IOException if config resolution fails or parsing step definitions fails
     */
    private List<StepDefinition> parseStepDefinitions(PipelineCompilationContext ctx) throws IOException {
        Optional<Path> configPath = resolvePipelineConfigPath(ctx);
        if (configPath.isEmpty()) {
            return List.of();
        }

        StepDefinitionParser parser = new StepDefinitionParser((kind, message) -> {
            if (ctx.getProcessingEnv() != null) {
                ctx.getProcessingEnv().getMessager().printMessage(kind, message);
            }
        });
        return parser.parseStepDefinitions(configPath.get());
    }

    /**
     * Resolve the module root directory by ascending from a generated-sources path or falling back to the current working directory.
     *
     * <p>If {@code generatedSourcesRoot} is non-null, the method climbs up to three parent directory levels and returns the resulting path if found. If no candidate is available or {@code generatedSourcesRoot} is null, the method returns the JVM current working directory.
     *
     * @param generatedSourcesRoot the path inside the module's generated-sources tree, or {@code null} if unknown
     * @return the resolved module directory path, or the current working directory if resolution fails
     */
    private Path resolveModuleDir(PipelineCompilationContext ctx, Path generatedSourcesRoot) {
        if (generatedSourcesRoot != null) {
            Path candidate = generatedSourcesRoot;
            // .../target/generated-sources/pipeline -> module root
            for (int i = 0; i < 3 && candidate != null; i++) {
                candidate = candidate.getParent();
            }
            if (candidate != null) {
                return candidate;
            }
        }
        return java.nio.file.Paths.get(System.getProperty("user.dir"));
    }
}
