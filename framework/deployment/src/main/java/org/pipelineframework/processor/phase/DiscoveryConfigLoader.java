package org.pipelineframework.processor.phase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.processing.Messager;
import javax.tools.Diagnostic;

import org.pipelineframework.config.pipeline.PipelineYamlConfigLocator;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.config.template.PipelineTemplateConfigLoader;
import org.pipelineframework.processor.config.PipelineAspectConfigLoader;
import org.pipelineframework.processor.config.PipelineRuntimeMappingLoader;
import org.pipelineframework.processor.config.PipelineRuntimeMappingLocator;
import org.pipelineframework.processor.config.PipelineStepConfigLoader;
import org.pipelineframework.processor.ir.PipelineAspectModel;
import org.pipelineframework.processor.mapping.PipelineRuntimeMapping;

/**
 * Loads pipeline configuration files during the discovery phase.
 * Wraps existing loaders with consistent error handling.
 */
class DiscoveryConfigLoader {

    /**
     * Resolve the pipeline configuration file path from options or by locating
     * a pipeline YAML within the module directory.
     *
     * @param options the processing environment options
     * @param moduleDir the module root directory, may be null
     * @param messager the messager for diagnostics, may be null
     * @return an Optional containing the resolved config Path if found
     */
    Optional<Path> resolvePipelineConfigPath(Map<String, String> options, Path moduleDir, Messager messager) {
        String explicitConfig = options.get("pipeline.config");
        if (explicitConfig != null && !explicitConfig.isBlank()) {
            Path explicitPath = Path.of(explicitConfig.trim());
            if (!explicitPath.isAbsolute() && moduleDir != null) {
                explicitPath = moduleDir.resolve(explicitPath).normalize();
            }
            if (Files.exists(explicitPath)) {
                return Optional.of(explicitPath);
            }
            if (messager != null) {
                messager.printMessage(
                    Diagnostic.Kind.ERROR,
                    "pipeline.config points to a missing path: '" + explicitConfig + "' (resolved to '" +
                        explicitPath + "')");
            }
            return Optional.empty();
        }
        if (moduleDir == null) {
            return Optional.empty();
        }
        PipelineYamlConfigLocator locator = new PipelineYamlConfigLocator();
        return locator.locate(moduleDir);
    }

    /**
     * Load pipeline aspect models from the resolved configuration file.
     *
     * @param configPath the pipeline config file path
     * @param messager the messager for error reporting, may be null
     * @return a list of loaded aspect models
     */
    List<PipelineAspectModel> loadAspects(Path configPath, Messager messager) {
        PipelineAspectConfigLoader loader = new PipelineAspectConfigLoader();
        try {
            return loader.load(configPath);
        } catch (Exception e) {
            if (messager != null) {
                messager.printMessage(Diagnostic.Kind.ERROR,
                    "Failed to load pipeline aspects from " + configPath + ": " + e.getMessage());
            }
            throw e;
        }
    }

    /**
     * Load the pipeline template configuration from the config file.
     *
     * @param configPath the pipeline config file path
     * @param messager the messager for warning reporting, may be null
     * @return the loaded template config, or null if loading fails
     */
    PipelineTemplateConfig loadTemplateConfig(Path configPath, Messager messager) {
        PipelineTemplateConfigLoader loader = new PipelineTemplateConfigLoader();
        try {
            return loader.load(configPath);
        } catch (Exception e) {
            if (messager != null) {
                messager.printMessage(Diagnostic.Kind.WARNING,
                    "Failed to load pipeline template config from " + configPath + ": " + e.getMessage());
            }
            return null;
        }
    }

    /**
     * Load step configuration from the pipeline config file.
     *
     * @param configPath the pipeline config file path
     * @param propertyLookup function to look up system properties
     * @param envLookup function to look up environment variables
     * @param messager the messager for warning reporting, may be null
     * @return the loaded step config
     */
    PipelineStepConfigLoader.StepConfig loadStepConfig(
            Path configPath,
            Function<String, String> propertyLookup,
            Function<String, String> envLookup,
            Messager messager) {
        PipelineStepConfigLoader stepLoader = new PipelineStepConfigLoader(propertyLookup, envLookup, messager);
        try {
            return stepLoader.load(configPath);
        } catch (Exception e) {
            if (messager != null) {
                messager.printMessage(Diagnostic.Kind.WARNING,
                    "Failed to load pipeline transport/platform from " + configPath + ": " + e.getMessage());
            }
            return null;
        }
    }

    /**
     * Load the runtime mapping for the current module, if present.
     *
     * @param moduleDir the module root directory
     * @param messager the messager for error reporting, may be null
     * @return the loaded runtime mapping, or null if none found
     */
    PipelineRuntimeMapping loadRuntimeMapping(Path moduleDir, Messager messager) {
        if (moduleDir == null) {
            return null;
        }
        PipelineRuntimeMappingLocator locator = new PipelineRuntimeMappingLocator();
        Optional<Path> configPath = locator.locate(moduleDir);
        if (configPath.isEmpty()) {
            return null;
        }

        PipelineRuntimeMappingLoader loader = new PipelineRuntimeMappingLoader();
        try {
            return loader.load(configPath.get());
        } catch (Exception e) {
            if (messager != null) {
                messager.printMessage(Diagnostic.Kind.ERROR,
                    "Failed to load runtime mapping from " + configPath.get() + ": " + e.getMessage());
            }
            throw e;
        }
    }
}
