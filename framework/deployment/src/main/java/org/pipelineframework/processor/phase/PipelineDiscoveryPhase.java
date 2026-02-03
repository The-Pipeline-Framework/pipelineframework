package org.pipelineframework.processor.phase;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;

import org.pipelineframework.annotation.PipelineOrchestrator;
import org.pipelineframework.annotation.PipelinePlugin;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLocator;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.config.template.PipelineTemplateConfigLoader;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.PipelineCompilationPhase;
import org.pipelineframework.processor.config.PipelineAspectConfigLoader;
import org.pipelineframework.processor.config.PipelineRuntimeMappingLoader;
import org.pipelineframework.processor.config.PipelineRuntimeMappingLocator;
import org.pipelineframework.processor.config.PipelineStepConfigLoader;
import org.pipelineframework.processor.ir.PipelineAspectModel;
import org.pipelineframework.processor.ir.PipelineOrchestratorModel;
import org.pipelineframework.processor.mapping.PipelineRuntimeMapping;

/**
 * Discovers and loads pipeline configuration, aspects, and semantic models.
 * This phase focuses solely on discovery without performing validation or code generation.
 */
public class PipelineDiscoveryPhase implements PipelineCompilationPhase {

    /**
     * Creates a new PipelineDiscoveryPhase.
     */
    public PipelineDiscoveryPhase() {
    }

    @Override
    public String name() {
        return "Pipeline Discovery Phase";
    }

    /**
     * Performs discovery of pipeline annotated elements, configurations, aspects, transport mode, and models,
     * then stores the resulting data on the provided compilation context.
     *
     * @param ctx the pipeline compilation context used to read the processing environment and options,
     *            and to store discovered artifacts (generated sources root, module directory/name, plugin host flag,
     *            aspect models, template config, runtime mapping, transport mode, and orchestrator models)
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

        // Load runtime mapping config (optional)
        PipelineRuntimeMapping runtimeMapping = loadRuntimeMapping(ctx);
        ctx.setRuntimeMapping(runtimeMapping);

        // Determine transport mode
        boolean isGrpc = loadPipelineTransport(ctx);
        ctx.setTransportModeGrpc(isGrpc);

        // Discover orchestrator models if present
        List<PipelineOrchestratorModel> orchestratorModels = discoverOrchestratorModels(ctx, orchestratorElements);
        ctx.setOrchestratorModels(orchestratorModels);
    }

    private List<PipelineAspectModel> loadPipelineAspects(PipelineCompilationContext ctx) {
        PipelineYamlConfigLocator locator = new PipelineYamlConfigLocator();
        Path moduleDir = ctx.getModuleDir();
        Optional<Path> configPath = locator.locate(moduleDir);
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

    private PipelineTemplateConfig loadPipelineTemplateConfig(PipelineCompilationContext ctx) {
        PipelineYamlConfigLocator locator = new PipelineYamlConfigLocator();
        Path moduleDir = ctx.getModuleDir();
        Optional<Path> configPath = locator.locate(moduleDir);
        if (configPath.isEmpty()) {
            return null;
        }

        PipelineTemplateConfigLoader loader = new PipelineTemplateConfigLoader();
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

    private boolean loadPipelineTransport(PipelineCompilationContext ctx) {
        PipelineYamlConfigLocator locator = new PipelineYamlConfigLocator();
        Path moduleDir = ctx.getModuleDir();
        Optional<Path> configPath = locator.locate(moduleDir);
        if (configPath.isEmpty()) {
            return true; // Default to GRPC
        }

        PipelineStepConfigLoader stepLoader = new PipelineStepConfigLoader();
        try {
            PipelineStepConfigLoader.StepConfig stepConfig = stepLoader.load(configPath.get());
            String transport = stepConfig.transport();
            if (transport == null || transport.isBlank()) {
                return true; // Default to GRPC
            }
            if ("REST".equalsIgnoreCase(transport)) {
                return false; // REST mode
            }
            if (!"GRPC".equalsIgnoreCase(transport)) {
                if (ctx.getProcessingEnv() != null) {
                    ctx.getProcessingEnv().getMessager().printMessage(javax.tools.Diagnostic.Kind.WARNING,
                        "Unknown pipeline transport '" + transport + "'; defaulting to GRPC.");
                }
            }
        } catch (Exception e) {
            if (ctx.getProcessingEnv() != null) {
                ctx.getProcessingEnv().getMessager().printMessage(javax.tools.Diagnostic.Kind.WARNING,
                    "Failed to load pipeline transport from " + configPath.get() + ": " + e.getMessage());
            }
        }
        return true; // Default to GRPC
    }

    private List<PipelineOrchestratorModel> discoverOrchestratorModels(
            PipelineCompilationContext ctx, 
            Set<? extends Element> orchestratorElements) {
        if (orchestratorElements == null || orchestratorElements.isEmpty()) {
            return List.of();
        }

        List<PipelineOrchestratorModel> models = new ArrayList<>();
        
        for (Element element : orchestratorElements) {
            PipelineOrchestrator annotation = resolveOrchestratorAnnotation(orchestratorElements);
            if (annotation != null) {
                // For now, we'll create a simple model based on the annotation
                // In a real implementation, this would extract more detailed information
                String serviceName = "OrchestratorService"; // Default name
                String servicePackage = element instanceof TypeElement typeElement 
                    ? typeElement.getQualifiedName().toString().replace("." + element.getSimpleName().toString(), ".orchestrator.service")
                    : "org.pipelineframework.orchestrator.service";
                
                // Determine enabled targets based on transport mode
                // This is simplified - in reality, it would depend on configuration
                var enabledTargets = ctx.isTransportModeGrpc() 
                    ? java.util.Set.of(org.pipelineframework.processor.ir.GenerationTarget.GRPC_SERVICE)
                    : java.util.Set.of(org.pipelineframework.processor.ir.GenerationTarget.REST_RESOURCE);
                
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
     * Finds and returns the first `PipelineOrchestrator` annotation present on the provided elements.
     *
     * @param orchestratorElements elements to search for a `PipelineOrchestrator` annotation; may be null or empty
     * @return the `PipelineOrchestrator` annotation from the first element that has one, or `null` if none is found
     */
    private PipelineOrchestrator resolveOrchestratorAnnotation(Set<? extends Element> orchestratorElements) {
        if (orchestratorElements == null || orchestratorElements.isEmpty()) {
            return null;
        }
        for (Element element : orchestratorElements) {
            PipelineOrchestrator annotation = element.getAnnotation(PipelineOrchestrator.class);
            if (annotation != null) {
                return annotation;
            }
        }
        return null;
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