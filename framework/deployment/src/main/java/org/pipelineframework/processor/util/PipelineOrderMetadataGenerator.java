package org.pipelineframework.processor.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Logger;
import javax.annotation.processing.ProcessingEnvironment;
import javax.tools.StandardLocation;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.pipelineframework.config.pipeline.*;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.PipelineStepModel;

/**
 * Generates a META-INF/pipeline/order.json resource containing the resolved pipeline order.
 */
public class PipelineOrderMetadataGenerator {

    private static final String ORDER_RESOURCE = "META-INF/pipeline/order.json";
    private static final Logger LOGGER = Logger.getLogger(PipelineOrderMetadataGenerator.class.getName());
    private final ProcessingEnvironment processingEnv;
    private final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    /**
     * Creates a new PipelineOrderMetadataGenerator.
     *
     * @param processingEnv the processing environment for compiler utilities and messaging
     */
    public PipelineOrderMetadataGenerator(ProcessingEnvironment processingEnv) {
        this.processingEnv = processingEnv;
    }

    /**
     * Generate the pipeline order metadata file at META-INF/pipeline/order.json when an orchestrator is generated and a pipeline configuration with steps is available.
     *
     * The method resolves the pipeline configuration and base client steps from the compilation context, determines an ordered list of functional steps according to the YAML config, expands that order, and writes the resulting order JSON to the class output resources. If any resolution step fails or yields no steps, no file is written.
     *
     * @param ctx the compilation context used to locate pipeline configuration and step models
     * @throws IOException if creating or writing the resource file fails
     */
    public void writeOrderMetadata(PipelineCompilationContext ctx) throws IOException {
        if (!ctx.isOrchestratorGenerated()) {
            return;
        }

        PipelineYamlConfig config = loadPipelineConfig(ctx);
        if (config == null || config.steps() == null || config.steps().isEmpty()) {
            return;
        }

        List<String> baseSteps = resolveBaseClientSteps(ctx);
        if (baseSteps.isEmpty()) {
            return;
        }

        List<String> functionalSteps = baseSteps.stream()
            .filter(name -> name != null && !name.contains("SideEffect"))
            .toList();
        if (functionalSteps.isEmpty()) {
            return;
        }

        List<String> ordered = orderByYamlSteps(functionalSteps, config.steps());
        if (ordered.isEmpty()) {
            return;
        }

        List<String> expanded = PipelineOrderExpander.expand(ordered, config, null);
        if (expanded == null || expanded.isEmpty()) {
            return;
        }

        PipelineOrderMetadata metadata = new PipelineOrderMetadata(expanded);
        javax.tools.FileObject resourceFile = processingEnv.getFiler()
            .createResource(StandardLocation.CLASS_OUTPUT, "", ORDER_RESOURCE, (javax.lang.model.element.Element[]) null);
        try (var writer = resourceFile.openWriter()) {
            writer.write(gson.toJson(metadata));
        }
    }

    /**
     * Loads the pipeline YAML configuration associated with the given compilation context.
     *
     * @param ctx the compilation context used to locate the pipeline configuration (may provide module directory)
     * @return the loaded PipelineYamlConfig, or `null` if no configuration path could be resolved
     */
    private PipelineYamlConfig loadPipelineConfig(PipelineCompilationContext ctx) {
        Optional<Path> configPath = resolvePipelineConfigPath(ctx);
        if (configPath.isEmpty()) {
            return null;
        }
        PipelineYamlConfigLoader loader = new PipelineYamlConfigLoader();
        return loader.load(configPath.get());
    }

    /**
     * Resolve the filesystem path to the pipeline YAML configuration, honoring an explicit
     * "pipeline.config" compiler option and falling back to discovery within the module.
     *
     * If the "pipeline.config" option is provided and is an absolute path, that path is used
     * when it exists. If it is a relative path, it is resolved against the compilation
     * context's module directory; if the module directory is unavailable or the resolved path
     * does not exist, a warning is logged and discovery is attempted. When no explicit option
     * is provided, discovery starts from the compilation context's module directory.
     *
     * @param ctx the compilation context whose module directory is used to resolve relative paths
     *            and as the starting point for configuration discovery
     * @return an Optional containing the resolved configuration Path when found, or an empty
     *         Optional if no valid configuration path could be determined
     */
    private Optional<Path> resolvePipelineConfigPath(PipelineCompilationContext ctx) {
        Map<String, String> options = processingEnv != null ? processingEnv.getOptions() : Map.of();
        String explicit = options.get("pipeline.config");
        if (explicit != null && !explicit.isBlank()) {
            Path explicitPath = Path.of(explicit.trim());
            if (!explicitPath.isAbsolute()) {
                if (ctx.getModuleDir() == null) {
                    LOGGER.warning("pipeline.config provided as relative path but moduleDir is null: " + explicit);
                    return Optional.empty();
                }
                explicitPath = ctx.getModuleDir().resolve(explicitPath).normalize();
            }
            if (Files.exists(explicitPath)) {
                return Optional.of(explicitPath);
            }
            LOGGER.warning(
                "pipeline.config path not found: provided='" + explicit + "', resolved='" + explicitPath +
                    "'. Falling back to pipeline.yaml discovery.");
        }
        Path moduleDir = ctx.getModuleDir();
        if (moduleDir == null) {
            return Optional.empty();
        }
        PipelineYamlConfigLocator locator = new PipelineYamlConfigLocator();
        return locator.locate(moduleDir);
    }

    /**
     * Builds the ordered list of base client pipeline step class names for the given compilation context.
     *
     * Filters the pipeline step models to those with deployment role ORCHESTRATOR_CLIENT and not marked as side effects,
     * constructs each step's fully qualified class name using the model's package, generated name (with "Service" removed),
     * and the transport-mode-specific client step suffix, preserves the original model order and removes duplicates.
     *
     * @param ctx the pipeline compilation context containing step models and transport mode
     * @return a list of fully qualified client step class names in preserved insertion order, or an empty list if no applicable models exist
     */
    private List<String> resolveBaseClientSteps(PipelineCompilationContext ctx) {
        List<PipelineStepModel> models = ctx.getStepModels();
        if (models == null || models.isEmpty()) {
            return List.of();
        }
        String suffix = ctx.getTransportMode().clientStepSuffix();
        Set<String> ordered = new LinkedHashSet<>();
        for (PipelineStepModel model : models) {
            if (model.deploymentRole() != DeploymentRole.ORCHESTRATOR_CLIENT || model.sideEffect()) {
                continue;
            }
            String className = model.servicePackage() + ".pipeline." +
                model.generatedName().replace("Service", "") + suffix;
            ordered.add(className);
        }
        return new ArrayList<>(ordered);
    }

    private List<String> orderByYamlSteps(List<String> availableSteps, List<PipelineYamlStep> yamlSteps) {
        List<String> remaining = new ArrayList<>(availableSteps);
        List<String> ordered = new ArrayList<>();
        for (PipelineYamlStep step : yamlSteps) {
            if (step == null || step.name() == null) {
                continue;
            }
            String token = toClassToken(step.name());
            if (token.isBlank()) {
                continue;
            }
            String match = selectBestMatch(remaining, token);
            if (match != null) {
                ordered.add(match);
                remaining.remove(match);
            }
        }
        ordered.addAll(remaining);
        return ordered;
    }

    private String selectBestMatch(List<String> candidates, String token) {
        String best = null;
        int bestLength = -1;
        for (String candidate : candidates) {
            if (candidate == null || candidate.isBlank()) {
                continue;
            }
            String normalized = normalizeStepToken(candidate);
            if (normalized.contains(token) && token.length() > bestLength) {
                best = candidate;
                bestLength = token.length();
            }
        }
        return best;
    }

    /**
     * Convert a fully-qualified step class name into a compact alphanumeric token by removing the package
     * prefix and common step suffixes.
     *
     * @param className the fully-qualified or simple class name of the step
     * @return the normalized alphanumeric token derived from the class's simple name with known
     *         suffixes (Service, GrpcClientStep, RestClientStep, LocalClientStep and optional _Subclass)
     *         removed; returns an empty string if the result contains no alphanumeric characters
     */
    private String normalizeStepToken(String className) {
        String simple = className;
        int lastDot = simple.lastIndexOf('.');
        if (lastDot != -1) {
            simple = simple.substring(lastDot + 1);
        }
        simple = simple.replaceAll("(Service|GrpcClientStep|RestClientStep|LocalClientStep)(_Subclass)?$", "");
        return toClassToken(simple);
    }

    private String toClassToken(String name) {
        if (name == null) {
            return "";
        }
        return name.replaceAll("[^A-Za-z0-9]", "");
    }

    private static class PipelineOrderMetadata {
        List<String> order;

        /**
         * Creates a PipelineOrderMetadata that holds the resolved pipeline execution order.
         *
         * @param order the ordered list of fully qualified step class names to include in the metadata; may be empty
         */
        PipelineOrderMetadata(List<String> order) {
            this.order = order;
        }
    }
}