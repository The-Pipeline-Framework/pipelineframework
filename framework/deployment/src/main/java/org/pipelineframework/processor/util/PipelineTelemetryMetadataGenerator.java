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
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.processor.ir.TransportMode;
import org.pipelineframework.processor.ir.TypeMapping;

/**
 * Writes pipeline telemetry metadata for item-boundary inference.
 */
public class PipelineTelemetryMetadataGenerator {

    private static final String RESOURCE_PATH = "META-INF/pipeline/telemetry.json";
    private static final String REPLAY_TOPOLOGY_RESOURCE_PATH = "META-INF/pipeline/replay-topology.json";
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
     * Generate pipeline telemetry metadata used for item-boundary inference and write it to META-INF/pipeline/telemetry.json.
     *
     * The written metadata contains the pipeline item input/output types, the resolved producer and consumer client step class names,
     * and plugin-to-parent step mappings. If required information cannot be determined (for example, orchestrator not generated or item
     * types unresolved), no resource is produced.
     *
     * @param ctx the compilation context providing step models, transport mode, and processor utilities
     * @throws IOException if creating or writing the resource fails
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
        writeReplayTopologyMetadata(ctx, ordered, models);
        ItemTypes itemTypes = resolveItemTypes(ctx, ordered);
        if (itemTypes == null || itemTypes.inputType() == null || itemTypes.outputType() == null) {
            return;
        }
        TransportMode transportMode = ctx.getTransportMode();
        String consumer = findConsumerStep(ordered, itemTypes.inputType(), transportMode);
        String producer = findProducerStep(ordered, itemTypes.outputType(), transportMode);
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

    private void writeReplayTopologyMetadata(
        PipelineCompilationContext ctx,
        List<PipelineStepModel> orderedBase,
        List<PipelineStepModel> orderedClientModels) throws IOException {
        if (orderedBase == null || orderedBase.isEmpty()) {
            return;
        }
        String pipeline = resolvePipelineName(ctx);
        PipelineYamlConfig pipelineConfig = loadPipelineConfig(ctx);
        Map<String, PipelineYamlStep> configStepsByToken = indexPipelineSteps(pipelineConfig);
        List<ReplayTopologyStep> steps = new ArrayList<>();
        List<ReplayTopologyStep> baseSteps = buildBaseReplaySteps(ctx, orderedBase, configStepsByToken);
        Map<String, String> parentByPluginRuntimeClass = resolveStepParents(ctx, orderedBase);
        Map<String, List<PipelineStepModel>> sideEffectsByParent = new LinkedHashMap<>();
        for (PipelineStepModel model : orderedClientModels) {
            if (model == null || !model.sideEffect()) {
                continue;
            }
            String pluginRuntimeClass = resolveClientStepClassName(model, ctx.getTransportMode());
            String parentRuntimeClass = parentByPluginRuntimeClass.get(pluginRuntimeClass);
            if (parentRuntimeClass == null) {
                throw unresolvedSideEffect(model, "replay-topology", pluginRuntimeClass);
            }
            sideEffectsByParent.computeIfAbsent(parentRuntimeClass, ignored -> new ArrayList<>()).add(model);
        }

        steps.addAll(baseSteps);
        int index = baseSteps.size();
        for (ReplayTopologyStep baseStep : baseSteps) {
            String parentRuntimeClass = baseStep.runtimeStepClass();
            for (PipelineStepModel sideEffect : sideEffectsByParent.getOrDefault(parentRuntimeClass, List.of())) {
                String sideEffectService = sideEffect.generatedName();
                String sideEffectStep = sideEffectService.endsWith("Service")
                    ? sideEffectService.substring(0, sideEffectService.length() - "Service".length())
                    : sideEffectService;
                steps.add(new ReplayTopologyStep(
                    resolveClientStepClassName(sideEffect, ctx.getTransportMode()),
                    sideEffectStep,
                    sideEffectService,
                    cardinality(sideEffect.streamingShape()),
                    index++,
                    true,
                    baseStep.step(),
                    resolvePluginKind(sideEffectService, sideEffectStep),
                    resolvePluginRenderRole(resolvePluginKind(sideEffectService, sideEffectStep)),
                    resolvePluginActorKind(resolvePluginKind(sideEffectService, sideEffectStep))));
            }
        }
        List<ReplayTopologyTransition> transitions = new ArrayList<>();
        for (int i = 0; i < baseSteps.size() - 1; i++) {
            ReplayTopologyStep from = baseSteps.get(i);
            ReplayTopologyStep to = baseSteps.get(i + 1);
            transitions.add(new ReplayTopologyTransition(
                from.step() + "->" + to.step(),
                from.runtimeStepClass(),
                to.runtimeStepClass(),
                from.step(),
                to.step(),
                from.service(),
                to.service(),
                from.cardinality(),
                "primary"));
        }
        index = appendAwaitActors(baseSteps, configStepsByToken, steps, transitions, index);
        index = appendPersistenceStore(baseSteps, steps, transitions, index);
        for (ReplayTopologyStep step : steps) {
            if (!step.sideEffect() || step.parentStep() == null || !shouldAddBranchTransition(step)) {
                continue;
            }
            ReplayTopologyStep parent = steps.stream()
                .filter(candidate -> step.parentStep().equals(candidate.step()))
                .findFirst()
                .orElse(null);
            if (parent == null) {
                continue;
            }
            transitions.add(new ReplayTopologyTransition(
                parent.step() + "->" + step.step(),
                parent.runtimeStepClass(),
                step.runtimeStepClass(),
                parent.step(),
                step.step(),
                parent.service(),
                step.service(),
                step.cardinality(),
                relationKindForBranch(step)));
        }
        ReplayTopologyMetadata metadata = new ReplayTopologyMetadata(pipeline, steps, transitions);
        StringWriter writer = new StringWriter();
        writer.write(gson.toJson(metadata));
        javax.tools.FileObject resourceFile = processingEnv.getFiler()
            .createResource(StandardLocation.CLASS_OUTPUT, "", REPLAY_TOPOLOGY_RESOURCE_PATH,
                (javax.lang.model.element.Element[]) null);
        try (var output = resourceFile.openWriter()) {
            output.write(writer.toString());
        }
    }

    /**
     * Filter pipeline step models to those enabled for the client generation target
     * corresponding to the context's transport mode.
     *
     * @param ctx the pipeline compilation context used to obtain step models and determine the transport mode
     * @return a list of PipelineStepModel instances whose enabledTargets contain the resolved client GenerationTarget;
     *         returns an empty list if the context has no step models or none match the target
     */
    private List<PipelineStepModel> filterClientModels(PipelineCompilationContext ctx) {
        List<PipelineStepModel> models = ctx.getStepModels();
        if (models == null || models.isEmpty()) {
            return List.of();
        }
        GenerationTarget target = resolveClientTarget(ctx.getTransportMode());
        return models.stream()
            .filter(model -> model.enabledTargets().contains(target))
            .toList();
    }

    /**
     * Map a transport mode to its corresponding client-generation target.
     *
     * @param transportMode the transport mode to map
     * @return the GenerationTarget used for client step generation for the given transport mode
     */
    private GenerationTarget resolveClientTarget(TransportMode transportMode) {
        return switch (transportMode) {
            case REST -> GenerationTarget.REST_CLIENT_STEP;
            case LOCAL -> GenerationTarget.LOCAL_CLIENT_STEP;
            case GRPC -> GenerationTarget.CLIENT_STEP;
        };
    }

    /**
     * Loads the configured pipeline item input type from application.properties.
     *
     * @param ctx the pipeline compilation context used to locate application.properties
     * @return the trimmed value of the `pipeline.telemetry.item-input-type` property, or `null` if the property is not present or could not be read
     *         (an IO failure while reading application.properties will emit a compiler warning)
     */
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

    /**
     * Orders the provided base pipeline step models according to the steps defined in the project's pipeline YAML.
     *
     * If no pipeline YAML or no steps are defined, the original models list is returned unchanged. Models whose
     * class-name tokens match entries in the YAML are placed in the same order as the YAML; any models not matched
     * by the YAML are appended after the matched models in their original relative order.
     *
     * @param ctx the pipeline compilation context used to locate and load the pipeline YAML configuration
     * @param models the base (non-side-effect) pipeline step models to order
     * @return a list of pipeline step models ordered to reflect the pipeline YAML, with unmatched models appended
     */
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
            PipelineStepModel match = selectBestMatch(remaining, token, ctx.getTransportMode());
            if (match != null) {
                ordered.add(match);
                remaining.remove(match);
            }
        }
        ordered.addAll(remaining);
        return ordered;
    }

    /**
     * Loads the pipeline YAML configuration using the compilation context's resolved pipeline config path.
     *
     * If no pipeline config path can be resolved (for example when no module directory is available and no explicit
     * configuration option is provided), this method returns {@code null}.
     *
     * @param ctx the pipeline compilation context used to resolve the pipeline config path
     * @return the loaded {@link PipelineYamlConfig}, or {@code null} if no config path could be resolved
     */
    private PipelineYamlConfig loadPipelineConfig(PipelineCompilationContext ctx) {
        var configPath = resolvePipelineConfigPath(ctx);
        if (configPath.isEmpty()) {
            return null;
        }
        PipelineYamlConfigLoader loader = new PipelineYamlConfigLoader(processingEnv.getOptions()::get, System::getenv);
        return loader.load(configPath.get());
    }

    /**
     * Resolve the filesystem path to the pipeline YAML configuration for the given compilation context.
     *
     * <p>Prefers an explicit processor option "pipeline.config" (resolving a relative value against the
     * context's module directory when present). If the explicit path is absent or not found, falls back
     * to locating a pipeline config under the module directory. Compiler warnings are emitted via the
     * processing environment when a relative explicit path cannot be resolved or when an explicit path
     * is not found.
     *
     * @param ctx the pipeline compilation context providing module directory and processor options
     * @return an Optional containing the resolved config Path if found, otherwise an empty Optional
     */
    private Optional<Path> resolvePipelineConfigPath(PipelineCompilationContext ctx) {
        Map<String, String> options = processingEnv != null ? processingEnv.getOptions() : Map.of();
        String explicit = options.get("pipeline.config");
        if (explicit != null && !explicit.isBlank()) {
            Path explicitPath = Path.of(explicit.trim());
            if (!explicitPath.isAbsolute()) {
                if (ctx.getModuleDir() != null) {
                    explicitPath = ctx.getModuleDir().resolve(explicitPath).normalize();
                } else if (processingEnv != null) {
                    processingEnv.getMessager().printMessage(javax.tools.Diagnostic.Kind.WARNING,
                        "pipeline.config is relative but moduleDir is null: '" + explicit + "'");
                }
            }
            if (Files.exists(explicitPath)) {
                return Optional.of(explicitPath);
            }
            if (processingEnv != null) {
                processingEnv.getMessager().printMessage(
                    javax.tools.Diagnostic.Kind.WARNING,
                    "pipeline.config not found at '" + explicitPath + "' (resolved from '" + explicit +
                        "', moduleDir=" + ctx.getModuleDir() + "); falling back to PipelineYamlConfigLocator");
            }
        }
        Path moduleDir = ctx.getModuleDir();
        if (moduleDir == null) {
            return Optional.empty();
        }
        PipelineYamlConfigLocator locator = new PipelineYamlConfigLocator();
        return locator.locate(moduleDir);
    }

    private String resolvePipelineName(PipelineCompilationContext ctx) {
        try {
            Properties properties = loadApplicationProperties(ctx);
            String configured = properties.getProperty("pipeline.telemetry.pipeline-name");
            if (configured != null && !configured.isBlank()) {
                return configured.trim();
            }
        } catch (IOException ignored) {
            // Fall back to structural inference.
        }
        Optional<Path> pipelineConfig = resolvePipelineConfigPath(ctx);
        if (pipelineConfig.isPresent()) {
            Path config = pipelineConfig.get().toAbsolutePath().normalize();
            Path parent = config.getParent();
            if (parent != null) {
                Path candidate = "config".equals(parent.getFileName().toString()) ? parent.getParent() : parent;
                if (candidate != null && candidate.getFileName() != null) {
                    return candidate.getFileName().toString();
                }
            }
        }
        Path moduleDir = ctx.getModuleDir();
        if (moduleDir != null && moduleDir.getParent() != null && moduleDir.getParent().getFileName() != null) {
            return moduleDir.getParent().getFileName().toString();
        }
        if (moduleDir != null && moduleDir.getFileName() != null) {
            return moduleDir.getFileName().toString();
        }
        return "pipeline";
    }

    private String resolvePluginKind(String serviceName, String stepName) {
        String combined = ((serviceName == null ? "" : serviceName) + " " + (stepName == null ? "" : stepName))
            .toLowerCase(Locale.ROOT);
        if (combined.contains("reject")) {
            return "reject";
        }
        if (combined.contains("invalidateall")) {
            return "cache-invalidate-all";
        }
        if (combined.contains("invalidate")) {
            return "cache-invalidate";
        }
        if (combined.contains("cache")) {
            return "cache";
        }
        if (combined.contains("persist")) {
            return "persistence";
        }
        return null;
    }

    private Map<String, PipelineYamlStep> indexPipelineSteps(PipelineYamlConfig config) {
        if (config == null || config.steps() == null || config.steps().isEmpty()) {
            return Map.of();
        }
        LinkedHashMap<String, PipelineYamlStep> indexed = new LinkedHashMap<>();
        for (PipelineYamlStep step : config.steps()) {
            if (step == null || step.name() == null || step.name().isBlank()) {
                continue;
            }
            indexed.put(toClassToken(step.name()), step);
        }
        return Collections.unmodifiableMap(indexed);
    }

    private PipelineYamlStep resolvePipelineStep(String logicalStep, Map<String, PipelineYamlStep> configStepsByToken) {
        if (logicalStep == null || logicalStep.isBlank() || configStepsByToken.isEmpty()) {
            return null;
        }
        String token = toClassToken(logicalStep);
        PipelineYamlStep direct = configStepsByToken.get(token);
        if (direct != null) {
            return direct;
        }
        PipelineYamlStep best = null;
        int bestLength = -1;
        for (Map.Entry<String, PipelineYamlStep> entry : configStepsByToken.entrySet()) {
            if (entry.getKey().contains(token) || token.contains(entry.getKey())) {
                if (entry.getKey().length() > bestLength) {
                    best = entry.getValue();
                    bestLength = entry.getKey().length();
                }
            }
        }
        return best;
    }

    private List<ReplayTopologyStep> buildBaseReplaySteps(
        PipelineCompilationContext ctx,
        List<PipelineStepModel> orderedBase,
        Map<String, PipelineYamlStep> configStepsByToken
    ) {
        if (orderedBase == null || orderedBase.isEmpty()) {
            return List.of();
        }
        Map<String, PipelineStepModel> remainingByToken = new LinkedHashMap<>();
        for (PipelineStepModel model : orderedBase) {
            if (model == null) {
                continue;
            }
            remainingByToken.put(toClassToken(baseLogicalStepName(model.generatedName())), model);
        }
        List<ReplayTopologyStep> baseSteps = new ArrayList<>();
        int index = 0;
        for (PipelineYamlStep configStep : configStepsByToken.values()) {
            if (configStep == null || configStep.name() == null || configStep.name().isBlank()) {
                continue;
            }
            String stepName = toPascalStepName(configStep.name());
            PipelineStepModel model = remainingByToken.remove(toClassToken(stepName));
            if (model != null) {
                baseSteps.add(baseReplayStepFromModel(model, configStep, index++, ctx.getTransportMode()));
                continue;
            }
            baseSteps.add(baseReplayStepFromConfig(stepName, configStep, index++));
        }
        for (PipelineStepModel model : remainingByToken.values()) {
            baseSteps.add(baseReplayStepFromModel(model, null, index++, ctx.getTransportMode()));
        }
        return baseSteps;
    }

    private ReplayTopologyStep baseReplayStepFromModel(
        PipelineStepModel model,
        PipelineYamlStep configStep,
        int index,
        TransportMode transportMode
    ) {
        String service = model.generatedName();
        String logicalStep = baseLogicalStepName(service);
        return new ReplayTopologyStep(
            resolveClientStepClassName(model, transportMode),
            logicalStep,
            service,
            cardinality(model, configStep),
            index,
            false,
            null,
            null,
            resolveBaseRenderRole(configStep, logicalStep),
            resolveBaseActorKind(configStep));
    }

    private ReplayTopologyStep baseReplayStepFromConfig(String logicalStep, PipelineYamlStep configStep, int index) {
        String service = logicalStep + "Service";
        return new ReplayTopologyStep(
            syntheticRuntimeStepClass("runtime::" + logicalStep, "BaseStep"),
            logicalStep,
            service,
            cardinality(configStep),
            index,
            false,
            null,
            null,
            resolveBaseRenderRole(configStep, logicalStep),
            resolveBaseActorKind(configStep));
    }

    private String baseLogicalStepName(String service) {
        return service != null && service.endsWith("Service")
            ? service.substring(0, service.length() - "Service".length())
            : service;
    }

    private String toPascalStepName(String rawName) {
        if (rawName == null || rawName.isBlank()) {
            return "";
        }
        StringBuilder builder = new StringBuilder();
        for (String part : rawName.split("[^A-Za-z0-9]+")) {
            if (part == null || part.isBlank()) {
                continue;
            }
            builder.append(Character.toUpperCase(part.charAt(0)));
            if (part.length() > 1) {
                builder.append(part.substring(1));
            }
        }
        return builder.toString();
    }

    private String resolveBaseRenderRole(PipelineYamlStep configStep, String logicalStep) {
        if (configStep != null && "await".equalsIgnoreCase(configStep.kind())) {
            return "await";
        }
        return logicalStep != null && logicalStep.toLowerCase(Locale.ROOT).contains("await") ? "await" : "primary";
    }

    private String resolveBaseActorKind(PipelineYamlStep configStep) {
        if (configStep == null || configStep.awaitConfig() == null || configStep.awaitConfig().transport() == null) {
            return null;
        }
        return configStep.awaitConfig().transport().type();
    }

    private String resolvePluginRenderRole(String pluginKind) {
        if ("persistence".equals(pluginKind)) {
            return "persistence-plugin";
        }
        if ("reject".equals(pluginKind)) {
            return "reject";
        }
        return "plugin";
    }

    private String resolvePluginActorKind(String pluginKind) {
        if ("persistence".equals(pluginKind)) {
            return "database";
        }
        if ("reject".equals(pluginKind)) {
            return "reject-queue";
        }
        return null;
    }

    private int appendAwaitActors(
        List<ReplayTopologyStep> baseSteps,
        Map<String, PipelineYamlStep> configStepsByToken,
        List<ReplayTopologyStep> steps,
        List<ReplayTopologyTransition> transitions,
        int nextIndex
    ) {
        for (ReplayTopologyStep baseStep : baseSteps) {
            PipelineYamlStep configStep = resolvePipelineStep(baseStep.step(), configStepsByToken);
            if (configStep == null || !"await".equalsIgnoreCase(configStep.kind()) || configStep.awaitConfig() == null
                || configStep.awaitConfig().transport() == null) {
                continue;
            }
            String transportType = configStep.awaitConfig().transport().type();
            if (!"kafka".equalsIgnoreCase(transportType)) {
                continue;
            }
            String brokerStepName = deriveBrokerActorName(baseStep.step());
            ReplayTopologyStep brokerStep = new ReplayTopologyStep(
                syntheticRuntimeStepClass(baseStep.runtimeStepClass(), brokerStepName),
                brokerStepName,
                brokerStepName + "Actor",
                "one-to-one",
                nextIndex++,
                true,
                baseStep.step(),
                null,
                "broker",
                "kafka");
            String providerBaseName = deriveProviderActorName(configStep, baseStep.step());
            ReplayTopologyStep providerStep = new ReplayTopologyStep(
                syntheticRuntimeStepClass(baseStep.runtimeStepClass(), providerBaseName),
                providerBaseName,
                providerBaseName + "Actor",
                "one-to-one",
                nextIndex++,
                true,
                baseStep.step(),
                null,
                "external-provider",
                "provider");
            steps.add(brokerStep);
            steps.add(providerStep);
            transitions.add(new ReplayTopologyTransition(
                baseStep.step() + "->" + brokerStep.step(),
                baseStep.runtimeStepClass(),
                brokerStep.runtimeStepClass(),
                baseStep.step(),
                brokerStep.step(),
                baseStep.service(),
                brokerStep.service(),
                baseStep.cardinality(),
                "await-request"));
            transitions.add(new ReplayTopologyTransition(
                brokerStep.step() + "->" + providerStep.step(),
                brokerStep.runtimeStepClass(),
                providerStep.runtimeStepClass(),
                brokerStep.step(),
                providerStep.step(),
                brokerStep.service(),
                providerStep.service(),
                "one-to-one",
                "await-request"));
            transitions.add(new ReplayTopologyTransition(
                providerStep.step() + "->" + brokerStep.step(),
                providerStep.runtimeStepClass(),
                brokerStep.runtimeStepClass(),
                providerStep.step(),
                brokerStep.step(),
                providerStep.service(),
                brokerStep.service(),
                "one-to-one",
                "await-completion"));
            transitions.add(new ReplayTopologyTransition(
                brokerStep.step() + "->" + baseStep.step(),
                brokerStep.runtimeStepClass(),
                baseStep.runtimeStepClass(),
                brokerStep.step(),
                baseStep.step(),
                brokerStep.service(),
                baseStep.service(),
                "one-to-one",
                "await-completion"));
        }
        return nextIndex;
    }

    private int appendPersistenceStore(
        List<ReplayTopologyStep> baseSteps,
        List<ReplayTopologyStep> steps,
        List<ReplayTopologyTransition> transitions,
        int nextIndex
    ) {
        List<String> persistentParents = steps.stream()
            .filter(ReplayTopologyStep::sideEffect)
            .filter(step -> "persistence-plugin".equals(step.renderRole()))
            .map(ReplayTopologyStep::parentStep)
            .filter(Objects::nonNull)
            .distinct()
            .toList();
        if (persistentParents.isEmpty() || baseSteps.isEmpty()) {
            return nextIndex;
        }
        ReplayTopologyStep anchor = baseSteps.getFirst();
        ReplayTopologyStep storeStep = new ReplayTopologyStep(
            syntheticRuntimeStepClass(anchor.runtimeStepClass(), "Database"),
            "Database",
            "DatabaseStore",
            "one-to-one",
            nextIndex++,
            true,
            null,
            null,
            "store",
            "database");
        steps.add(storeStep);
        Map<String, ReplayTopologyStep> baseByStep = new LinkedHashMap<>();
        for (ReplayTopologyStep baseStep : baseSteps) {
            baseByStep.put(baseStep.step(), baseStep);
        }
        for (String parentStep : persistentParents) {
            ReplayTopologyStep baseStep = baseByStep.get(parentStep);
            if (baseStep == null) {
                continue;
            }
            transitions.add(new ReplayTopologyTransition(
                baseStep.step() + "->" + storeStep.step(),
                baseStep.runtimeStepClass(),
                storeStep.runtimeStepClass(),
                baseStep.step(),
                storeStep.step(),
                baseStep.service(),
                storeStep.service(),
                baseStep.cardinality(),
                "store"));
        }
        return nextIndex;
    }

    private String deriveProviderActorName(PipelineYamlStep configStep, String logicalStep) {
        String rawName = configStep == null ? logicalStep : configStep.name();
        if (rawName == null || rawName.isBlank()) {
            return "ExternalProvider";
        }
        String normalized = rawName.replaceFirst("(?i)^await\\s+", "").trim();
        if (normalized.isBlank()) {
            normalized = logicalStep;
        }
        StringBuilder builder = new StringBuilder();
        for (String part : normalized.split("[^A-Za-z0-9]+")) {
            if (part == null || part.isBlank()) {
                continue;
            }
            builder.append(Character.toUpperCase(part.charAt(0)));
            if (part.length() > 1) {
                builder.append(part.substring(1));
            }
        }
        return builder.length() == 0 ? "ExternalProvider" : builder.toString();
    }

    private String deriveBrokerActorName(String logicalStep) {
        String baseName = deriveProviderActorName(null, logicalStep);
        return (baseName == null || baseName.isBlank() ? "Kafka" : baseName) + "KafkaBroker";
    }

    private String syntheticRuntimeStepClass(String baseRuntimeStepClass, String suffix) {
        String normalizedBase = baseRuntimeStepClass == null || baseRuntimeStepClass.isBlank()
            ? "org.pipelineframework.synthetic.Replay"
            : baseRuntimeStepClass;
        return normalizedBase + "$" + suffix;
    }

    private String relationKindForBranch(ReplayTopologyStep step) {
        if (step == null) {
            return "plugin";
        }
        if ("reject".equals(step.renderRole())) {
            return "reject";
        }
        if ("persistence-plugin".equals(step.renderRole())) {
            return "store";
        }
        return "plugin";
    }

    private boolean shouldAddBranchTransition(ReplayTopologyStep step) {
        if (step == null) {
            return false;
        }
        return "plugin".equals(step.renderRole())
            || "reject".equals(step.renderRole())
            || "persistence-plugin".equals(step.renderRole());
    }

    /**
     * Selects the pipeline step model whose normalized client-step class name best matches the given token.
     *
     * @param candidates   candidate pipeline step models to consider
     * @param token        the matching token to search for in the normalized class name
     * @param transportMode transport mode used to resolve the client-step class name
     * @return             the best-matching PipelineStepModel, or `null` if none match
     */
    private PipelineStepModel selectBestMatch(
        List<PipelineStepModel> candidates,
        String token,
        TransportMode transportMode
    ) {
        PipelineStepModel best = null;
        int bestLength = -1;
        for (PipelineStepModel candidate : candidates) {
            String normalized = normalizeStepToken(resolveClientStepClassName(candidate, transportMode));
            if (normalized.contains(token) && normalized.length() > bestLength) {
                best = candidate;
                bestLength = normalized.length();
            }
        }
        return best;
    }

    /**
     * Converts a step class name into a normalized lowercase alphanumeric token.
     *
     * Removes the package prefix (if present) and common client/service suffixes
     * such as "Service", "GrpcClientStep", "RestClientStep", and "LocalClientStep",
     * then returns the resulting name as a lowercase string containing only
     * letters and digits.
     *
     * @param className the fully-qualified or simple class name of the step
     * @return a lowercase alphanumeric token derived from the class name
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

    /**
     * Convert a string to a lowercase alphanumeric token.
     *
     * <p>All non-alphanumeric characters are removed and the result is lowercased.</p>
     *
     * @param name the input string to convert; may be null
     * @return the input converted to a lowercase string containing only ASCII letters and digits,
     *         or an empty string if {@code name} is null or contains no alphanumeric characters
     */
    private String toClassToken(String name) {
        if (name == null) {
            return "";
        }
        return name.replaceAll("[^A-Za-z0-9]", "").toLowerCase();
    }

    /**
     * Selects the producer (client) step class name for the given item type from an ordered list of pipeline steps.
     *
     * @param ordered       the ordered list of pipeline steps to search
     * @param itemType      the domain type to match against each step's output mapping
     * @param transportMode the transport mode used to resolve the client step class name
     * @return the fully-qualified client step class name whose output mapping matches `itemType` (last matching step is returned);
     *         if no step matches, the client class name of the final step in `ordered` is returned; returns `null` if `ordered` is null or empty
     */
    private String findProducerStep(
        List<PipelineStepModel> ordered,
        String itemType,
        TransportMode transportMode
    ) {
        String lastMatch = null;
        for (PipelineStepModel model : ordered) {
            if (matchesType(model.outputMapping(), itemType)) {
                lastMatch = resolveClientStepClassName(model, transportMode);
            }
        }
        if (lastMatch != null) {
            return lastMatch;
        }
        if (ordered == null || ordered.isEmpty()) {
            return null;
        }
        return resolveClientStepClassName(ordered.get(ordered.size() - 1), transportMode);
    }

    /**
     * Resolve parent mappings for plugin (side-effect) steps to their corresponding base steps.
     *
     * @param ctx         the pipeline compilation context used to access step models and transport mode
     * @param orderedBase ordered list of base (non-side-effect) step models to consult when locating a plugin's parent
     * @return            a map where each key is a plugin step's client step class name and each value is the parent base step's client step class name;
     *                    returns an empty map if no plugin parents are found
     */
    private Map<String, String> resolveStepParents(PipelineCompilationContext ctx, List<PipelineStepModel> orderedBase) {
        List<PipelineStepModel> models = ctx.getStepModels();
        if (models == null || models.isEmpty()) {
            return Map.of();
        }
        GenerationTarget target = resolveClientTarget(ctx.getTransportMode());
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
                throw unresolvedSideEffect(plugin, "step-parent-resolution", resolveClientStepClassName(plugin, ctx.getTransportMode()));
            }
            parents.put(
                resolveClientStepClassName(plugin, ctx.getTransportMode()),
                resolveClientStepClassName(parent, ctx.getTransportMode()));
        }
        return parents;
    }

    private IllegalStateException unresolvedSideEffect(
        PipelineStepModel model,
        String phase,
        String pluginRuntimeClass
    ) {
        return new IllegalStateException(
            "Unable to resolve parent step for side-effect model serviceName='"
                + (model == null ? "unknown" : model.serviceName())
                + "', name='"
                + (model == null ? "unknown" : model.generatedName())
                + "', service='"
                + (model == null ? "unknown" : model.serviceClassName())
                + "', runtimeStepClass='"
                + pluginRuntimeClass
                + "', phase='"
                + phase
                + "'.");
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

    /**
     * Determine the client-side class name of the consumer step that accepts a given item type.
     *
     * @param ordered the ordered list of base pipeline step models to search
     * @param itemType the item domain type to match against each step's input mapping
     * @param transportMode the transport mode used to resolve the client step class name
     * @return the fully-qualified client step class name for the first step whose input matches `itemType`; if none match but `ordered` is non-empty, the first step's client class name; `null` if `ordered` is null or empty
     */
    private String findConsumerStep(
        List<PipelineStepModel> ordered,
        String itemType,
        TransportMode transportMode) {
        for (PipelineStepModel model : ordered) {
            if (matchesType(model.inputMapping(), itemType)) {
                return resolveClientStepClassName(model, transportMode);
            }
        }
        if (ordered == null || ordered.isEmpty()) {
            return null;
        }
        return resolveClientStepClassName(ordered.get(0), transportMode);
    }

    /**
     * Determine whether the given mapping's domain type matches the provided item type string.
     *
     * @param mapping the type mapping to inspect; may be null
     * @param itemType the item type to compare against; may be null
     * @return `true` if the mapping has a non-null domain type equal to `itemType`, `false` otherwise
     */
    private boolean matchesType(TypeMapping mapping, String itemType) {
        if (mapping == null || mapping.domainType() == null || itemType == null) {
            return false;
        }
        return itemType.equals(mapping.domainType().toString());
    }

    /**
     * Builds the fully-qualified client step class name for a pipeline step model.
     *
     * @param model the pipeline step model used to derive package and generated name
     * @param transportMode the transport mode whose client-step suffix is appended
     * @return the fully-qualified client step class name (package + ".pipeline." + generated name without "Service" + transport suffix)
     */
    private String resolveClientStepClassName(PipelineStepModel model, TransportMode transportMode) {
        String suffix = transportMode.clientStepSuffix();
        return model.servicePackage() + ".pipeline." +
            model.generatedName().replace("Service", "") + suffix;
    }

    private String cardinality(StreamingShape shape) {
        if (shape == null) {
            return "one-to-one";
        }
        return switch (shape) {
            case UNARY_UNARY -> "one-to-one";
            case UNARY_STREAMING -> "one-to-many";
            case STREAMING_UNARY -> "many-to-one";
            case STREAMING_STREAMING -> "many-to-many";
        };
    }

    private String cardinality(PipelineStepModel model, PipelineYamlStep step) {
        if (step != null && step.cardinality() != null && !step.cardinality().isBlank()) {
            return cardinality(step);
        }
        return cardinality(model.streamingShape());
    }

    private String cardinality(PipelineYamlStep step) {
        if (step == null || step.cardinality() == null || step.cardinality().isBlank()) {
            return "one-to-one";
        }
        String normalized = step.cardinality().toUpperCase(Locale.ROOT);
        return switch (normalized) {
            case "ONE_TO_ONE" -> "one-to-one";
            case "ONE_TO_MANY", "EXPANSION" -> "one-to-many";
            case "MANY_TO_ONE", "COLLAPSE" -> "many-to-one";
            case "MANY_TO_MANY" -> "many-to-many";
            default -> throw new IllegalArgumentException(
                "Invalid pipeline.yaml cardinality '" + step.cardinality() + "' for step '"
                    + (step.name() == null ? "<unnamed>" : step.name()) + "'."
                    + " Allowed values: ONE_TO_ONE, ONE_TO_MANY, EXPANSION, MANY_TO_ONE, COLLAPSE, MANY_TO_MANY."
            );
        };
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

    private record ReplayTopologyMetadata(
        String pipeline,
        List<ReplayTopologyStep> steps,
        List<ReplayTopologyTransition> transitions) {
    }

    private record ReplayTopologyStep(
        String runtimeStepClass,
        String step,
        String service,
        String cardinality,
        int index,
        boolean sideEffect,
        String parentStep,
        String pluginKind,
        String renderRole,
        String actorKind) {
    }

    private record ReplayTopologyTransition(
        String id,
        String fromRuntimeStepClass,
        String toRuntimeStepClass,
        String from,
        String to,
        String fromService,
        String toService,
        String cardinality,
        String relationKind) {
    }
}
