package org.pipelineframework.processor.util;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Logger;
import javax.annotation.processing.ProcessingEnvironment;
import javax.tools.StandardLocation;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.squareup.javapoet.TypeName;
import org.pipelineframework.config.pipeline.PipelineYamlAwaitTransport;
import org.pipelineframework.config.pipeline.PipelineYamlConfig;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLoader;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLocator;
import org.pipelineframework.config.pipeline.PipelineYamlStep;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineTransport;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.StreamingShape;

/**
 * Generates deterministic versioned bundle metadata for transition-worker validation.
 */
public class PipelineBundleManifestMetadataGenerator {

    private static final String RESOURCE_PATH = "META-INF/pipeline/bundle-manifest.json";
    private static final Logger LOGGER = Logger.getLogger(PipelineBundleManifestMetadataGenerator.class.getName());
    private static final Gson CANONICAL_GSON = new GsonBuilder().disableHtmlEscaping().create();
    private static final Gson PRETTY_GSON = new GsonBuilder().disableHtmlEscaping().setPrettyPrinting().create();

    private final ProcessingEnvironment processingEnv;

    /**
     * Creates a metadata generator.
     *
     * @param processingEnv processing environment
     */
    public PipelineBundleManifestMetadataGenerator(ProcessingEnvironment processingEnv) {
        this.processingEnv = processingEnv;
    }

    /**
     * Writes META-INF/pipeline/bundle-manifest.json when pipeline steps are available.
     *
     * @param ctx compilation context
     * @throws IOException when writing fails
     */
    public void writeBundleManifest(PipelineCompilationContext ctx) throws IOException {
        if (ctx == null || ctx.getStepModels() == null || ctx.getStepModels().isEmpty()) {
            return;
        }
        List<Map<String, Object>> steps = stepDescriptors(ctx);
        if (steps.isEmpty()) {
            return;
        }

        String pipelineId = resolvePipelineId(ctx);
        Map<String, Object> manifestWithoutHash = new LinkedHashMap<>();
        manifestWithoutHash.put("schemaVersion", 1);
        manifestWithoutHash.put("pipelineId", pipelineId);
        manifestWithoutHash.put("platform", ctx.getPlatformMode() == null ? "COMPUTE" : ctx.getPlatformMode().name());
        manifestWithoutHash.put("transport", ctx.getTransportMode() == null ? "GRPC" : ctx.getTransportMode().name());
        manifestWithoutHash.put("module", blankToNull(ctx.getModuleName()));
        manifestWithoutHash.put("pluginHost", ctx.isPluginHost());
        manifestWithoutHash.put("runtimeLayout", ctx.getRuntimeMapping() == null ? null : ctx.getRuntimeMapping().layout().name());
        manifestWithoutHash.put("steps", steps);
        manifestWithoutHash.put("capabilities", capabilities());

        String bundleHash = sha256(CANONICAL_GSON.toJson(manifestWithoutHash));
        Map<String, Object> finalManifest = new LinkedHashMap<>();
        finalManifest.put("schemaVersion", 1);
        finalManifest.put("pipelineId", pipelineId);
        finalManifest.put("bundleVersionId", "sha256:" + bundleHash);
        finalManifest.put("bundleHash", bundleHash);
        finalManifest.put("platform", manifestWithoutHash.get("platform"));
        finalManifest.put("transport", manifestWithoutHash.get("transport"));
        finalManifest.put("module", manifestWithoutHash.get("module"));
        finalManifest.put("pluginHost", manifestWithoutHash.get("pluginHost"));
        finalManifest.put("runtimeLayout", manifestWithoutHash.get("runtimeLayout"));
        finalManifest.put("steps", steps);
        finalManifest.put("capabilities", manifestWithoutHash.get("capabilities"));

        if (processingEnv != null) {
            javax.tools.FileObject resourceFile = processingEnv.getFiler()
                .createResource(StandardLocation.CLASS_OUTPUT, "", RESOURCE_PATH);
            try (var writer = resourceFile.openWriter()) {
                writer.write(PRETTY_GSON.toJson(finalManifest));
            }
        }
    }

    private List<Map<String, Object>> stepDescriptors(PipelineCompilationContext ctx) {
        PipelineYamlConfig config = loadPipelineConfig(ctx);
        Map<String, PipelineYamlStep> yamlByName = indexYamlSteps(config);
        List<PipelineStepModel> orderedModels = orderModels(ctx.getStepModels(), config);
        List<Map<String, Object>> descriptors = new ArrayList<>();
        for (int i = 0; i < orderedModels.size(); i++) {
            PipelineStepModel model = orderedModels.get(i);
            PipelineYamlStep yamlStep = yamlByName.get(normalizeStepToken(stepTokenFromModel(model)));
            if (yamlStep == null) {
                yamlStep = yamlByName.get(normalizeStepToken(model.serviceName()));
            }
            Map<String, Object> descriptor = new LinkedHashMap<>();
            descriptor.put("index", i);
            descriptor.put("authoredName", yamlStep == null ? stripTrailingService(model.generatedName()) : yamlStep.name());
            descriptor.put("kind", yamlStep == null ? inferKind(model) : yamlStep.kind());
            descriptor.put("cardinality", yamlStep == null ? cardinality(model.streamingShape()) : yamlStep.cardinality());
            descriptor.put("inputTypeId", typeId(model.inputMapping().domainType()));
            descriptor.put("outputTypeId", typeId(model.outputMapping().domainType()));
            descriptor.put("runtimeClass", runtimeClass(model, ctx));
            descriptor.put("clientClass", clientClass(model, ctx));
            descriptor.put("awaitTransport", awaitTransport(yamlStep));
            descriptors.add(descriptor);
        }
        return descriptors;
    }

    private String resolvePipelineId(PipelineCompilationContext ctx) {
        PipelineYamlConfig config = loadPipelineConfig(ctx);
        if (config != null && config.basePackage() != null && !config.basePackage().isBlank()) {
            return config.basePackage();
        }
        if (ctx.getPipelineTemplateConfig() instanceof PipelineTemplateConfig templateConfig
            && templateConfig.basePackage() != null
            && !templateConfig.basePackage().isBlank()) {
            return templateConfig.basePackage();
        }
        if (ctx.getModuleName() != null && !ctx.getModuleName().isBlank()) {
            return ctx.getModuleName();
        }
        return "local-pipeline";
    }

    private PipelineYamlConfig loadPipelineConfig(PipelineCompilationContext ctx) {
        Optional<Path> configPath = resolvePipelineConfigPath(ctx);
        if (configPath.isEmpty()) {
            return null;
        }
        PipelineYamlConfigLoader loader = processingEnv != null
            ? new PipelineYamlConfigLoader(processingEnv.getOptions()::get, System::getenv)
            : new PipelineYamlConfigLoader(key -> null, System::getenv);
        return loader.load(configPath.get());
    }

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
            LOGGER.warning("pipeline.config path not found: " + explicitPath);
        }
        if (ctx.getModuleDir() == null) {
            return Optional.empty();
        }
        return new PipelineYamlConfigLocator().locate(ctx.getModuleDir());
    }

    private static Map<String, PipelineYamlStep> indexYamlSteps(PipelineYamlConfig config) {
        if (config == null || config.steps() == null) {
            return Map.of();
        }
        Map<String, PipelineYamlStep> indexed = new LinkedHashMap<>();
        for (PipelineYamlStep step : config.steps()) {
            if (step == null || step.name() == null) {
                continue;
            }
            indexed.put(normalizeStepToken(step.name()), step);
        }
        return indexed;
    }

    private static List<PipelineStepModel> orderModels(List<PipelineStepModel> models, PipelineYamlConfig config) {
        if (config == null || config.steps() == null || config.steps().isEmpty()) {
            return models.stream().filter(model -> !model.sideEffect()).toList();
        }
        Map<String, PipelineStepModel> byToken = new LinkedHashMap<>();
        for (PipelineStepModel model : models) {
            if (model.sideEffect()) {
                continue;
            }
            byToken.put(normalizeStepToken(stepTokenFromModel(model)), model);
            byToken.put(normalizeStepToken(stripTrailingService(model.generatedName())), model);
            byToken.put(normalizeStepToken(model.serviceName()), model);
        }
        List<PipelineStepModel> ordered = new ArrayList<>();
        Set<PipelineStepModel> added = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        for (PipelineYamlStep step : config.steps()) {
            if (step == null || step.name() == null) {
                continue;
            }
            PipelineStepModel model = byToken.get(normalizeStepToken(step.name()));
            if (model != null && added.add(model)) {
                ordered.add(model);
            }
        }
        for (PipelineStepModel model : models) {
            if (!model.sideEffect() && added.add(model)) {
                ordered.add(model);
            }
        }
        return ordered;
    }

    private static Map<String, Object> capabilities() {
        Map<String, Object> capabilities = new LinkedHashMap<>();
        capabilities.put("localTransitionExecution", true);
        capabilities.put("transitionWorkerProtocols", List.of("local", "rest", "grpc", "sqs"));
        return capabilities;
    }

    private static String inferKind(PipelineStepModel model) {
        return model.enabledTargets().contains(GenerationTarget.AWAIT_CLIENT_STEP) ? "await" : "internal";
    }

    private static String cardinality(StreamingShape shape) {
        return switch (shape) {
            case UNARY_UNARY -> "ONE_TO_ONE";
            case UNARY_STREAMING -> "ONE_TO_MANY";
            case STREAMING_UNARY -> "MANY_TO_ONE";
            case STREAMING_STREAMING -> "MANY_TO_MANY";
        };
    }

    private static String runtimeClass(PipelineStepModel model, PipelineCompilationContext ctx) {
        if (model.enabledTargets().contains(GenerationTarget.AWAIT_CLIENT_STEP)) {
            return clientClass(model, ctx);
        }
        return model.serviceClassName() == null ? null : model.serviceClassName().canonicalName();
    }

    private static String clientClass(PipelineStepModel model, PipelineCompilationContext ctx) {
        String suffix = model.enabledTargets().contains(GenerationTarget.AWAIT_CLIENT_STEP)
            ? "AwaitClientStep"
            : java.util.Objects.requireNonNullElse(ctx.getTransportMode(), PipelineTransport.GRPC).clientStepSuffix();
        return model.servicePackage() + ".pipeline." + stripTrailingService(model.generatedName()) + suffix;
    }

    private static String awaitTransport(PipelineYamlStep step) {
        if (step == null || step.awaitConfig() == null || step.awaitConfig().transport() == null) {
            return null;
        }
        PipelineYamlAwaitTransport transport = step.awaitConfig().transport();
        return transport.type();
    }

    private static String typeId(TypeName typeName) {
        return typeName == null ? null : typeName.toString();
    }

    private static String stripTrailingService(String value) {
        if (value == null) {
            return "";
        }
        return value.endsWith("Service") ? value.substring(0, value.length() - "Service".length()) : value;
    }

    private static String stepTokenFromModel(PipelineStepModel model) {
        String token = stripTrailingService(model.generatedName());
        return token.startsWith("Process") && token.length() > "Process".length()
            ? token.substring("Process".length())
            : token;
    }

    private static String normalizeStepToken(String value) {
        if (value == null) {
            return "";
        }
        return value.replaceAll("[^A-Za-z0-9]", "").toLowerCase(java.util.Locale.ROOT);
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    private static String sha256(String content) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(content.getBytes(StandardCharsets.UTF_8)));
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException("Unable to compute pipeline bundle hash", e);
        }
    }
}
