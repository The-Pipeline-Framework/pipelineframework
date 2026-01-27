package org.pipelineframework.processor.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import javax.annotation.processing.ProcessingEnvironment;
import javax.tools.StandardLocation;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.pipelineframework.config.pipeline.PipelineYamlConfig;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLoader;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLocator;
import org.pipelineframework.config.pipeline.PipelineYamlStep;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.TypeMapping;

/**
 * Writes pipeline telemetry metadata for item-boundary inference.
 */
public class PipelineTelemetryMetadataGenerator {

    private static final String RESOURCE_PATH = "META-INF/pipeline/telemetry.json";
    private static final String ITEM_INPUT_TYPE_KEY = "pipeline.telemetry.item-input-type";
    private static final String ITEM_OUTPUT_TYPE_KEY = "pipeline.telemetry.item-output-type";

    private final ProcessingEnvironment processingEnv;
    private final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    /**
     * Creates a new metadata generator.
     *
     * @param processingEnv the processing environment for compiler utilities and messaging
     */
    public PipelineTelemetryMetadataGenerator(ProcessingEnvironment processingEnv) {
        this.processingEnv = processingEnv;
    }

    /**
     * Writes item-boundary metadata to META-INF/pipeline/telemetry.json.
     *
     * @param ctx the compilation context
     * @throws IOException if writing the resource fails
     */
    public void writeTelemetryMetadata(PipelineCompilationContext ctx) throws IOException {
        if (!ctx.isOrchestratorGenerated()) {
            return;
        }
        List<PipelineStepModel> models = filterClientModels(ctx);
        if (models.isEmpty()) {
            return;
        }
        List<PipelineStepModel> baseModels = models.stream()
            .filter(model -> !model.sideEffect())
            .toList();
        if (baseModels.isEmpty()) {
            return;
        }
        List<PipelineStepModel> ordered = orderBaseSteps(ctx, baseModels);
        ItemTypes itemTypes = resolveItemTypes(ctx, ordered);
        if (itemTypes == null || itemTypes.inputType() == null || itemTypes.outputType() == null) {
            return;
        }
        String consumer = findConsumerStep(ordered, itemTypes.inputType(), ctx.isTransportModeGrpc());
        String producer = findProducerStep(ordered, itemTypes.outputType(), ctx.isTransportModeGrpc());
        Map<String, String> stepParents = resolveStepParents(ctx, ordered);
        if (producer == null && consumer == null) {
            return;
        }

        TelemetryMetadata metadata = new TelemetryMetadata(
            itemTypes.inputType(),
            itemTypes.outputType(),
            producer,
            consumer,
            stepParents);
        StringWriter writer = new StringWriter();
        writer.write(gson.toJson(metadata));

        javax.tools.FileObject resourceFile = processingEnv.getFiler()
            .createResource(StandardLocation.CLASS_OUTPUT, "", RESOURCE_PATH, (javax.lang.model.element.Element[]) null);
        try (var output = resourceFile.openWriter()) {
            output.write(writer.toString());
        }
    }

    private List<PipelineStepModel> filterClientModels(PipelineCompilationContext ctx) {
        List<PipelineStepModel> models = ctx.getStepModels();
        if (models == null || models.isEmpty()) {
            return List.of();
        }
        GenerationTarget target = ctx.isTransportModeGrpc() ? GenerationTarget.CLIENT_STEP : GenerationTarget.REST_CLIENT_STEP;
        return models.stream()
            .filter(model -> model.enabledTargets().contains(target))
            .toList();
    }

    private String loadItemInputType(PipelineCompilationContext ctx) {
        Properties properties = new Properties();
        try {
            properties = loadApplicationProperties(ctx);
        } catch (IOException e) {
            processingEnv.getMessager().printMessage(javax.tools.Diagnostic.Kind.WARNING,
                "Failed to read application.properties for telemetry item type: " + e.getMessage());
        }
        String raw = properties.getProperty(ITEM_INPUT_TYPE_KEY);
        return raw == null ? null : raw.trim();
    }

    private String loadItemOutputType(PipelineCompilationContext ctx) {
        Properties properties = new Properties();
        try {
            properties = loadApplicationProperties(ctx);
        } catch (IOException e) {
            processingEnv.getMessager().printMessage(javax.tools.Diagnostic.Kind.WARNING,
                "Failed to read application.properties for telemetry item type: " + e.getMessage());
        }
        String raw = properties.getProperty(ITEM_OUTPUT_TYPE_KEY);
        return raw == null ? null : raw.trim();
    }

    private ItemTypes resolveItemTypes(PipelineCompilationContext ctx, List<PipelineStepModel> ordered) {
        String configuredInput = loadItemInputType(ctx);
        String configuredOutput = loadItemOutputType(ctx);
        if ((configuredInput != null && !configuredInput.isBlank())
            && (configuredOutput != null && !configuredOutput.isBlank())) {
            return new ItemTypes(configuredInput, configuredOutput);
        }
        if (ordered == null || ordered.isEmpty()) {
            return null;
        }
        String inferredInput = configuredInput;
        String inferredOutput = configuredOutput;
        PipelineStepModel first = ordered.get(0);
        if (inferredInput == null || inferredInput.isBlank()) {
            if (first.inputMapping() != null && first.inputMapping().domainType() != null) {
                inferredInput = first.inputMapping().domainType().toString();
            }
        }
        PipelineStepModel last = ordered.get(ordered.size() - 1);
        if (inferredOutput == null || inferredOutput.isBlank()) {
            if (last.outputMapping() != null && last.outputMapping().domainType() != null) {
                inferredOutput = last.outputMapping().domainType().toString();
            }
        }
        if (inferredInput == null || inferredInput.isBlank() || inferredOutput == null || inferredOutput.isBlank()) {
            return null;
        }
        return new ItemTypes(inferredInput, inferredOutput);
    }

    private Properties loadApplicationProperties(PipelineCompilationContext ctx) throws IOException {
        Properties properties = new Properties();
        for (Path baseDir : getBaseDirectories(ctx)) {
            Path propertiesPath = baseDir.resolve("src/main/resources/application.properties");
            if (Files.exists(propertiesPath) && Files.isReadable(propertiesPath)) {
                try (InputStream input = Files.newInputStream(propertiesPath)) {
                    properties.load(input);
                    return properties;
                }
            }
        }

        try {
            var resource = processingEnv.getFiler()
                .getResource(StandardLocation.SOURCE_PATH, "", "application.properties");
            try (InputStream input = resource.openInputStream()) {
                properties.load(input);
            }
        } catch (Exception e) {
            // Ignore when the resource is not available.
        }

        return properties;
    }

    private Set<Path> getBaseDirectories(PipelineCompilationContext ctx) {
        Set<Path> baseDirs = new LinkedHashSet<>();
        if (ctx != null && ctx.getModuleDir() != null) {
            baseDirs.add(ctx.getModuleDir());
        }
        String multiModuleDir = System.getProperty("maven.multiModuleProjectDirectory");
        if (multiModuleDir != null && !multiModuleDir.isBlank()) {
            baseDirs.add(Paths.get(multiModuleDir));
        }
        baseDirs.add(Paths.get(System.getProperty("user.dir", ".")));
        return baseDirs;
    }

    private List<PipelineStepModel> orderBaseSteps(PipelineCompilationContext ctx, List<PipelineStepModel> models) {
        PipelineYamlConfig config = loadPipelineConfig(ctx);
        if (config == null || config.steps() == null || config.steps().isEmpty()) {
            return models;
        }
        List<PipelineStepModel> remaining = new ArrayList<>(models);
        List<PipelineStepModel> ordered = new ArrayList<>();
        for (PipelineYamlStep step : config.steps()) {
            if (step == null || step.name() == null) {
                continue;
            }
            String token = toClassToken(step.name());
            if (token.isBlank()) {
                continue;
            }
            PipelineStepModel match = selectBestMatch(remaining, token, ctx.isTransportModeGrpc());
            if (match != null) {
                ordered.add(match);
                remaining.remove(match);
            }
        }
        ordered.addAll(remaining);
        return ordered;
    }

    private PipelineYamlConfig loadPipelineConfig(PipelineCompilationContext ctx) {
        PipelineYamlConfigLocator locator = new PipelineYamlConfigLocator();
        Path moduleDir = ctx.getModuleDir();
        if (moduleDir == null) {
            return null;
        }
        var configPath = locator.locate(moduleDir);
        if (configPath.isEmpty()) {
            return null;
        }
        PipelineYamlConfigLoader loader = new PipelineYamlConfigLoader();
        return loader.load(configPath.get());
    }

    private PipelineStepModel selectBestMatch(List<PipelineStepModel> candidates, String token, boolean grpcTransport) {
        PipelineStepModel best = null;
        int bestLength = -1;
        for (PipelineStepModel candidate : candidates) {
            String normalized = normalizeStepToken(resolveClientStepClassName(candidate, grpcTransport));
            if (normalized.contains(token) && normalized.length() > bestLength) {
                best = candidate;
                bestLength = normalized.length();
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
        simple = simple.replaceAll("(Service|GrpcClientStep|RestClientStep)(_Subclass)?$", "");
        return toClassToken(simple);
    }

    private String toClassToken(String name) {
        if (name == null) {
            return "";
        }
        return name.replaceAll("[^A-Za-z0-9]", "").toLowerCase();
    }

    private String findProducerStep(List<PipelineStepModel> ordered, String itemType, boolean grpcTransport) {
        String lastMatch = null;
        for (PipelineStepModel model : ordered) {
            if (matchesType(model.outputMapping(), itemType)) {
                lastMatch = resolveClientStepClassName(model, grpcTransport);
            }
        }
        if (lastMatch != null) {
            return lastMatch;
        }
        if (ordered == null || ordered.isEmpty()) {
            return null;
        }
        return resolveClientStepClassName(ordered.get(ordered.size() - 1), grpcTransport);
    }

    private Map<String, String> resolveStepParents(PipelineCompilationContext ctx, List<PipelineStepModel> orderedBase) {
        List<PipelineStepModel> models = ctx.getStepModels();
        if (models == null || models.isEmpty()) {
            return Map.of();
        }
        GenerationTarget target = ctx.isTransportModeGrpc() ? GenerationTarget.CLIENT_STEP : GenerationTarget.REST_CLIENT_STEP;
        List<PipelineStepModel> pluginSteps = models.stream()
            .filter(model -> model.enabledTargets().contains(target))
            .filter(PipelineStepModel::sideEffect)
            .toList();
        if (pluginSteps.isEmpty()) {
            return Map.of();
        }
        Map<String, String> parents = new LinkedHashMap<>();
        for (PipelineStepModel plugin : pluginSteps) {
            PipelineStepModel parent = resolveParentForPlugin(plugin, orderedBase);
            if (parent == null) {
                continue;
            }
            parents.put(
                resolveClientStepClassName(plugin, ctx.isTransportModeGrpc()),
                resolveClientStepClassName(parent, ctx.isTransportModeGrpc()));
        }
        return parents;
    }

    private PipelineStepModel resolveParentForPlugin(PipelineStepModel plugin, List<PipelineStepModel> orderedBase) {
        if (plugin == null || orderedBase == null || orderedBase.isEmpty()) {
            return null;
        }
        String typeName = mappingType(plugin.inputMapping());
        if (typeName == null) {
            return null;
        }
        PipelineStepModel outputMatch = null;
        for (PipelineStepModel base : orderedBase) {
            if (matchesType(base.outputMapping(), typeName)) {
                outputMatch = base;
            }
        }
        if (outputMatch != null) {
            return outputMatch;
        }
        PipelineStepModel inputMatch = null;
        for (PipelineStepModel base : orderedBase) {
            if (matchesType(base.inputMapping(), typeName)) {
                inputMatch = base;
            }
        }
        return inputMatch;
    }

    private String mappingType(TypeMapping mapping) {
        if (mapping == null || mapping.domainType() == null) {
            return null;
        }
        return mapping.domainType().toString();
    }

    private String findConsumerStep(
        List<PipelineStepModel> ordered,
        String itemType,
        boolean grpcTransport) {
        for (PipelineStepModel model : ordered) {
            if (matchesType(model.inputMapping(), itemType)) {
                return resolveClientStepClassName(model, grpcTransport);
            }
        }
        if (ordered == null || ordered.isEmpty()) {
            return null;
        }
        return resolveClientStepClassName(ordered.get(0), grpcTransport);
    }

    private boolean matchesType(TypeMapping mapping, String itemType) {
        if (mapping == null || mapping.domainType() == null || itemType == null) {
            return false;
        }
        return itemType.equals(mapping.domainType().toString());
    }

    private String resolveClientStepClassName(PipelineStepModel model, boolean grpcTransport) {
        String suffix = grpcTransport ? "GrpcClientStep" : "RestClientStep";
        return model.servicePackage() + ".pipeline." +
            model.generatedName().replace("Service", "") + suffix;
    }

    private record TelemetryMetadata(
        String itemInputType,
        String itemOutputType,
        String producerStep,
        String consumerStep,
        Map<String, String> stepParents) {
    }

    private record ItemTypes(
        String inputType,
        String outputType) {
    }
}
