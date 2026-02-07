package org.pipelineframework.processor.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
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
     * Writes the resolved pipeline order to META-INF/pipeline/order.json if possible.
     *
     * @param ctx the compilation context
     * @throws IOException if writing the resource fails
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

    private PipelineYamlConfig loadPipelineConfig(PipelineCompilationContext ctx) {
        PipelineYamlConfigLocator locator = new PipelineYamlConfigLocator();
        java.nio.file.Path moduleDir = ctx.getModuleDir();
        if (moduleDir == null) {
            return null;
        }
        java.util.Optional<java.nio.file.Path> configPath = locator.locate(moduleDir);
        if (configPath.isEmpty()) {
            return null;
        }
        PipelineYamlConfigLoader loader = new PipelineYamlConfigLoader();
        return loader.load(configPath.get());
    }

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

        PipelineOrderMetadata(List<String> order) {
            this.order = order;
        }
    }
}
