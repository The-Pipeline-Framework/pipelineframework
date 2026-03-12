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
import javax.lang.model.util.Elements;
import javax.tools.Diagnostic;

import org.pipelineframework.annotation.PipelineOrchestrator;
import org.pipelineframework.annotation.PipelinePlugin;
import org.pipelineframework.config.PlatformMode;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.config.connector.ConnectorConfig;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.PipelineCompilationPhase;
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
    private final ConnectorConfigValidator connectorConfigValidator;

    /**
     * Creates a PipelineDiscoveryPhase configured with the default collaborators.
     *
     * <p>The default collaborators are DiscoveryPathResolver, DiscoveryConfigLoader,
     * TransportPlatformResolver, and ConnectorConfigValidator.
     */
    public PipelineDiscoveryPhase() {
        this(
            new DiscoveryPathResolver(),
            new DiscoveryConfigLoader(),
            new TransportPlatformResolver(),
            new ConnectorConfigValidator());
    }

    /**
     * Create a PipelineDiscoveryPhase using the provided collaborators and a default ConnectorConfigValidator.
     *
     * @param discoveryPathResolver resolver for locating generated sources, module directory, and module name
     * @param discoveryConfigLoader loader for discovery-related configuration (aspects, templates, step config, runtime mapping)
     * @param transportPlatformResolver resolver for transport and platform modes from step configuration
     */
    public PipelineDiscoveryPhase(
            DiscoveryPathResolver discoveryPathResolver,
            DiscoveryConfigLoader discoveryConfigLoader,
            TransportPlatformResolver transportPlatformResolver) {
        this(
            discoveryPathResolver,
            discoveryConfigLoader,
            transportPlatformResolver,
            new ConnectorConfigValidator());
    }

    /**
     * Creates a PipelineDiscoveryPhase configured with the given collaborators.
     *
     * @param discoveryPathResolver resolves paths for generated sources, module directory, and module name
     * @param discoveryConfigLoader loads discovery-related configuration (aspects, template/step configs, runtime mapping)
     * @param transportPlatformResolver resolves transport and platform modes from step config
     * @param connectorConfigValidator validates connector declarations against template and step definitions
     * @throws NullPointerException if any argument is null
     */
    public PipelineDiscoveryPhase(
            DiscoveryPathResolver discoveryPathResolver,
            DiscoveryConfigLoader discoveryConfigLoader,
            TransportPlatformResolver transportPlatformResolver,
            ConnectorConfigValidator connectorConfigValidator) {
        this.discoveryPathResolver = Objects.requireNonNull(discoveryPathResolver, "discoveryPathResolver");
        this.discoveryConfigLoader = Objects.requireNonNull(discoveryConfigLoader, "discoveryConfigLoader");
        this.transportPlatformResolver = Objects.requireNonNull(transportPlatformResolver, "transportPlatformResolver");
        this.connectorConfigValidator = Objects.requireNonNull(connectorConfigValidator, "connectorConfigValidator");
    }

    /**
     * Provides the phase's human-readable name.
     *
     * @return the phase name "Pipeline Discovery Phase"
     */
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

        Path moduleDir = discoveryPathResolver.resolveModuleDir(options, generatedSourcesRoot);
        ctx.setModuleDir(moduleDir);
        ctx.setModuleName(discoveryPathResolver.resolveModuleName(options));

        Optional<Path> configPath = discoveryConfigLoader.resolvePipelineConfigPath(options, moduleDir, messager);

        // Check if this is a plugin host
        boolean isPluginHost = !pluginElements.isEmpty();
        ctx.setPluginHost(isPluginHost);
        ctx.setFunctionHttpBridge(parseStrictBooleanOption(options, "pipeline.function.httpBridge", false));

        // Load pipeline aspects
        List<PipelineAspectModel> aspects = loadPipelineAspects(configPath, messager);
        ctx.setAspectModels(aspects);

        // Load pipeline template config
        PipelineTemplateConfig templateConfig = loadPipelineTemplateConfig(configPath, messager);
        ctx.setPipelineTemplateConfig(templateConfig);

        // Parse step definitions from YAML
        List<StepDefinition> stepDefinitions = parseStepDefinitions(configPath, messager);
        ctx.setStepDefinitions(stepDefinitions);
        List<ConnectorConfig> connectorConfigs = validateConnectorConfigs(
            templateConfig,
            stepDefinitions,
            ctx,
            messager);
        ctx.setConnectorConfigs(connectorConfigs);

        // Load runtime mapping config (optional)
        PipelineRuntimeMapping runtimeMapping = loadRuntimeMapping(moduleDir, messager);
        ctx.setRuntimeMapping(runtimeMapping);

        // Determine transport and platform modes
        PipelineStepConfigLoader.StepConfig stepConfig = loadPipelineStepConfig(configPath, options, messager);
        TransportMode transportMode = transportPlatformResolver.resolveTransport(stepConfig.transport(), messager);
        ctx.setTransportMode(transportMode);
        PlatformMode platformMode = transportPlatformResolver.resolvePlatform(stepConfig.platform(), messager);
        ctx.setPlatformMode(platformMode);

        // Discover orchestrator models if present
        List<PipelineOrchestratorModel> orchestratorModels = discoverOrchestratorModels(ctx, orchestratorElements);
        ctx.setOrchestratorModels(orchestratorModels);
    }

    private boolean parseStrictBooleanOption(Map<String, String> options, String key, boolean defaultValue) {
        String rawValue = options.get(key);
        if (rawValue == null) {
            return defaultValue;
        }

        String normalized = rawValue.trim();
        if ("true".equalsIgnoreCase(normalized)) {
            return true;
        }
        if ("false".equalsIgnoreCase(normalized)) {
            return false;
        }

        throw new IllegalArgumentException(
            "Invalid value for '" + key + "': '" + rawValue + "'. Expected 'true' or 'false'.");
    }

    /**
     * Loads pipeline aspect models from the resolved pipeline configuration file.
     *
     * @param configPath the optional resolved pipeline configuration path
     * @param messager the messager used to report diagnostics, may be null
     * @return a list of loaded {@code PipelineAspectModel} instances; an empty list if no pipeline config is found
     */
    private List<PipelineAspectModel> loadPipelineAspects(Optional<Path> configPath, Messager messager) {
        if (configPath.isEmpty()) {
            return List.of();
        }
        return discoveryConfigLoader.loadAspects(configPath.get(), messager);
    }

    /**
     * Loads the pipeline template configuration from the given config path.
     *
     * @param configPath an Optional containing the resolved pipeline configuration path, or empty if none was found
     * @param messager used to report diagnostics; may be null
     * @return the loaded PipelineTemplateConfig, or `null` if no configuration path was provided or if loading fails; when loading fails a diagnostic is reported via `messager` if available
     */
    private PipelineTemplateConfig loadPipelineTemplateConfig(Optional<Path> configPath, Messager messager) {
        if (configPath.isEmpty()) {
            return null;
        }
        return discoveryConfigLoader.loadTemplateConfig(configPath.get(), messager);
    }

    /**
     * Validate connector declarations from the pipeline template against the provided step
     * definitions and return the resulting connector configurations.
     *
     * @param templateConfig   the pipeline template configuration; if null no validation is performed
     * @param stepDefinitions  the parsed step definitions to validate connector usage against
     * @param ctx              the compilation context providing the processing environment
     * @param messager         optional Messager for reporting diagnostics
     * @return                 a list of validated {@code ConnectorConfig} instances; empty if {@code templateConfig} is null
     * @throws RuntimeException if validation fails; an ERROR diagnostic is emitted via {@code messager} before the exception is rethrown
     */
    private List<ConnectorConfig> validateConnectorConfigs(
        PipelineTemplateConfig templateConfig,
        List<StepDefinition> stepDefinitions,
        PipelineCompilationContext ctx,
        Messager messager
    ) {
        if (templateConfig == null) {
            return List.of();
        }
        try {
            return connectorConfigValidator.validate(
                templateConfig,
                stepDefinitions,
                ctx.getProcessingEnv(),
                messager);
        } catch (RuntimeException e) {
            if (messager != null) {
                messager.printMessage(Diagnostic.Kind.ERROR,
                    "Failed to validate connector declarations: " + e.getMessage());
            }
            throw e;
        }
    }

    /**
     * Resolve pipeline step configuration from the module's pipeline YAML.
     *
     * Locates a pipeline YAML under the context's module directory and loads its step-level
     * configuration (base package, transport, platform, input/output types). When the file is
     * missing or cannot be loaded, returns a default non-null configuration.
     *
     * @param configPath the optional resolved pipeline configuration path
     * @param options the annotation processor options map for property lookup
     * @param messager the messager used to report warnings, may be null
     * @return a non-null {@link org.pipelineframework.processor.config.PipelineStepConfigLoader.StepConfig}
     */
    private PipelineStepConfigLoader.StepConfig loadPipelineStepConfig(Optional<Path> configPath, Map<String, String> options, Messager messager) {
        if (configPath.isEmpty()) {
            return DEFAULT_STEP_CONFIG;
        }
        // Processor options take precedence over YAML config, then env vars.
        // processingEnv.getOptions() returns the -A... annotation processor options.
        PipelineStepConfigLoader.StepConfig loaded = discoveryConfigLoader.loadStepConfig(
            configPath.get(),
            options::get,
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
                String serviceName = "OrchestratorService";
                String servicePackage = "org.pipelineframework.orchestrator.service";
                if (element instanceof TypeElement typeElement && ctx.getProcessingEnv() != null) {
                    Elements elementUtils = ctx.getProcessingEnv().getElementUtils();
                    if (elementUtils != null) {
                        String packageName = elementUtils.getPackageOf(typeElement).getQualifiedName().toString();
                        servicePackage = packageName + ".orchestrator.service";
                        serviceName = typeElement.getSimpleName() + "OrchestratorService";
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
     * Retrieve the {@code PipelineOrchestrator} annotation from the given element.
     *
     * @param orchestratorElement element to inspect; may be {@code null}
     * @return the {@code PipelineOrchestrator} annotation, or {@code null} if the element is {@code null} or not annotated
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
     * Returns the parsed StepDefinition objects found in the resolved pipeline config.
     * If no config is found or parsing fails, returns an empty list and emits diagnostics
     * via the processing environment's messager when available.
     *
     * @param configPath the optional resolved pipeline configuration path
     * @param messager the messager used to emit diagnostics, may be null
     * @return a list of StepDefinition parsed from the template; empty if none or on error
     */
    private List<StepDefinition> parseStepDefinitions(Optional<Path> configPath, Messager messager) {
        if (configPath.isEmpty()) {
            return List.of();
        }

        StepDefinitionParser parser = new StepDefinitionParser((kind, message) ->
            reportDiagnostic(messager, kind, message));
        try {
            return parser.parseStepDefinitions(configPath.get());
        } catch (IOException e) {
            reportDiagnostic(
                messager,
                Diagnostic.Kind.ERROR,
                "Failed to parse YAML step definitions from " + configPath.get() + ": " + e.getMessage());
            return List.of();
        } catch (Exception e) {
            reportDiagnostic(
                messager,
                Diagnostic.Kind.ERROR,
                "Unexpected error while parsing YAML step definitions from " + configPath.get() + ": " + e.getMessage());
            return List.of();
        }
    }

    private void reportDiagnostic(Messager messager, Diagnostic.Kind kind, String message) {
        if (messager != null) {
            messager.printMessage(kind, message);
        }
    }
}
