package org.pipelineframework.processor.util;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.*;
import java.util.logging.Logger;
import javax.annotation.processing.ProcessingEnvironment;
import javax.tools.StandardLocation;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.squareup.javapoet.TypeName;
import org.pipelineframework.config.pipeline.*;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.config.template.PipelineTemplateTypeDefinition;
import org.pipelineframework.config.template.PipelineTemplateTypeModel;
import org.pipelineframework.config.template.PipelineTemplateTypeReference;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.composition.PipelineCompositionContractProjector;
import org.pipelineframework.orchestrator.composition.PipelineCompositionDescriptor;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.PipelineTransport;
import org.pipelineframework.processor.ir.StreamingShape;

/**
 * Generates deterministic semantic pipeline contract metadata for coordinator/worker validation.
 */
public class PipelineContractMetadataGenerator {

    private static final String CONTRACT_RESOURCE_PATH = "META-INF/pipeline/pipeline-contract.json";
    private static final Logger LOGGER = Logger.getLogger(PipelineContractMetadataGenerator.class.getName());
    private static final Gson CANONICAL_GSON = new GsonBuilder().disableHtmlEscaping().create();
    private static final Gson PRETTY_GSON = new GsonBuilder().disableHtmlEscaping().setPrettyPrinting().create();

    private final ProcessingEnvironment processingEnv;

    /**
     * Creates a metadata generator.
     *
     * @param processingEnv processing environment
     */
    public PipelineContractMetadataGenerator(ProcessingEnvironment processingEnv) {
        this.processingEnv = processingEnv;
    }

    /**
     * Writes META-INF/pipeline/pipeline-contract.json when pipeline steps are available.
     *
     * @param ctx compilation context
     * @throws IOException when writing fails
     */
    public void writePipelineContract(PipelineCompilationContext ctx) throws IOException {
        if (ctx == null || ctx.getStepModels() == null || ctx.getStepModels().isEmpty()) {
            return;
        }
        List<Map<String, Object>> steps = stepDescriptors(ctx);
        if (steps.isEmpty()) {
            return;
        }

        String pipelineId = resolvePipelineId(ctx);
        Map<String, Object> contractWithoutHash = new LinkedHashMap<>();
        Map<String, Object> canonicalTypes = canonicalTypes(ctx);
        PipelineCompositionDescriptor composition = ctx.getResolvedPipelineDefinitionGraph()
            .map(graph -> new PipelineCompositionContractProjector().project(graph))
            .orElseGet(PipelineCompositionDescriptor::empty);
        boolean hasContributedTypes = ctx.getPipelineTemplateConfig() instanceof PipelineTemplateConfig config
            && !config.typeModel().contributedTypeIdentities().isEmpty();
        int schemaVersion = composition.present() || hasContributedTypes ? 3 : canonicalTypes.isEmpty() ? 1 : 2;
        String canonicalCatalogFingerprint = sha256(CANONICAL_GSON.toJson(canonicalTypes));
        contractWithoutHash.put("schemaVersion", schemaVersion);
        contractWithoutHash.put("pipelineId", pipelineId);
        contractWithoutHash.put("platform", ctx.getPlatformMode() == null ? "COMPUTE" : ctx.getPlatformMode().name());
        contractWithoutHash.put("transport", ctx.getTransportMode() == null ? "GRPC" : ctx.getTransportMode().name());
        contractWithoutHash.put("module", blankToNull(ctx.getModuleName()));
        contractWithoutHash.put("pluginHost", ctx.isPluginHost());
        contractWithoutHash.put("runtimeLayout", ctx.getRuntimeMapping() == null ? null : ctx.getRuntimeMapping().layout().name());
        contractWithoutHash.put("steps", steps);
        contractWithoutHash.put("canonicalTypes", canonicalTypes);
        contractWithoutHash.put("canonicalCatalogFingerprint", canonicalCatalogFingerprint);
        if (composition.present()) {
            contractWithoutHash.put("composition", composition);
        }
        contractWithoutHash.put("capabilities", capabilities());

        String contractHash = sha256(CANONICAL_GSON.toJson(contractWithoutHash));
        Map<String, Object> finalContract = new LinkedHashMap<>();
        finalContract.put("schemaVersion", schemaVersion);
        finalContract.put("pipelineId", pipelineId);
        finalContract.put("contractVersion", "sha256:" + contractHash);
        finalContract.put("contractHash", contractHash);
        finalContract.put("platform", contractWithoutHash.get("platform"));
        finalContract.put("transport", contractWithoutHash.get("transport"));
        finalContract.put("module", contractWithoutHash.get("module"));
        finalContract.put("pluginHost", contractWithoutHash.get("pluginHost"));
        finalContract.put("runtimeLayout", contractWithoutHash.get("runtimeLayout"));
        finalContract.put("steps", steps);
        finalContract.put("canonicalTypes", canonicalTypes);
        finalContract.put("canonicalCatalogFingerprint", canonicalCatalogFingerprint);
        if (composition.present()) {
            finalContract.put("composition", composition);
        }
        finalContract.put("capabilities", contractWithoutHash.get("capabilities"));

        if (processingEnv != null) {
            javax.tools.FileObject resourceFile = processingEnv.getFiler()
                .createResource(StandardLocation.CLASS_OUTPUT, "", CONTRACT_RESOURCE_PATH);
            try (var writer = resourceFile.openWriter()) {
                writer.write(PRETTY_GSON.toJson(finalContract));
            }
        }
    }

    private Map<String, Object> canonicalTypes(PipelineCompilationContext ctx) {
        if (!(ctx.getPipelineTemplateConfig() instanceof PipelineTemplateConfig config)
            || config.dialect() != org.pipelineframework.config.template.PipelineTemplateDialect.V3) {
            return Map.of();
        }
        PipelineTemplateTypeModel model = config.typeModel();
        Map<String, Object> types = new java.util.TreeMap<>();
        for (Map.Entry<String, PipelineTemplateTypeDefinition> entry : model.definitions().entrySet()) {
            Map<String, Object> definition = definition(entry.getValue());
            String fingerprint = sha256(CANONICAL_GSON.toJson(definition));
            Map<String, Object> binding = new LinkedHashMap<>();
            binding.put("definition", definition);
            binding.put("definitionFingerprint", fingerprint);
            binding.put("runtimeClass", config.basePackage() + ".domain." + entry.getKey());
            model.contributedTypeIdentity(entry.getKey())
                .ifPresent(identity -> binding.put("contributedIdentity", identity.qualifiedName()));
            types.put(entry.getKey(), immutableSortedMap(binding));
        }
        return immutableSortedMap(types);
    }

    private Map<String, Object> definition(PipelineTemplateTypeDefinition definition) {
        Map<String, Object> encoded = new LinkedHashMap<>();
        encoded.put("id", definition.name());
        if (definition instanceof PipelineTemplateTypeDefinition.RecordType record) {
            encoded.put("kind", "record");
            List<Map<String, Object>> fields = record.fields().stream()
                .sorted(java.util.Comparator.comparing(PipelineTemplateTypeDefinition.Field::name))
                .map(this::fieldDefinition)
                .toList();
            encoded.put("fields", fields);
        } else if (definition instanceof PipelineTemplateTypeDefinition.WrapperType wrapper) {
            encoded.put("kind", "wrapper");
            encoded.put("wraps", typeExpression(wrapper.wraps()));
            var constraints = wrapper.constraints();
            constraints.minLength().ifPresent(value -> encoded.put("minLength", value));
            constraints.maxLength().ifPresent(value -> encoded.put("maxLength", value));
            constraints.pattern().ifPresent(value -> encoded.put("pattern", value));
            constraints.format().ifPresent(value -> encoded.put("format", value.name().toLowerCase(java.util.Locale.ROOT)));
            constraints.minimum().ifPresent(value -> encoded.put("minimum", value));
            constraints.minimumExclusive().ifPresent(value -> encoded.put("minimumExclusive", value));
            constraints.maximum().ifPresent(value -> encoded.put("maximum", value));
            constraints.maximumExclusive().ifPresent(value -> encoded.put("maximumExclusive", value));
        } else if (definition instanceof PipelineTemplateTypeDefinition.AliasType alias) {
            encoded.put("kind", "alias");
            encoded.put("target", typeExpression(alias.target()));
        } else if (definition instanceof PipelineTemplateTypeDefinition.UnionType union) {
            encoded.put("kind", "union");
            List<Map<String, Object>> variants = union.variants().values().stream()
                .sorted(java.util.Comparator.comparing(PipelineTemplateTypeDefinition.Variant::discriminator))
                .map(variant -> immutableSortedMap(Map.of(
                    "discriminator", variant.discriminator(), "payload", typeExpression(variant.payload()))))
                .toList();
            encoded.put("variants", variants);
        }
        return immutableSortedMap(encoded);
    }

    private Map<String, Object> fieldDefinition(PipelineTemplateTypeDefinition.Field field) {
        Map<String, Object> encoded = new LinkedHashMap<>();
        encoded.put("name", field.name());
        encoded.put("type", typeExpression(field.type()));
        encoded.put("presence", field.presence().name());
        encoded.put("nullability", field.nullability().name());
        if (field.repeated()) {
            encoded.put("repeated", true);
        }
        return immutableSortedMap(encoded);
    }

    private Map<String, Object> typeExpression(PipelineTemplateTypeReference reference) {
        if (reference instanceof PipelineTemplateTypeReference.Named named) {
            return immutableSortedMap(Map.of("kind", "named", "id", named.name()));
        }
        if (reference instanceof PipelineTemplateTypeReference.Scalar scalar) {
            return immutableSortedMap(Map.of("kind", "scalar", "id", scalar.name()));
        }
        if (reference instanceof PipelineTemplateTypeReference.MapType map) {
            return immutableSortedMap(Map.of(
                "kind", "map", "key", typeExpression(map.keyType()), "value", typeExpression(map.valueType())));
        }
        throw new IllegalArgumentException("Unsupported canonical type expression: " + reference);
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

    private static Map<String, Object> immutableSortedMap(Map<String, ?> values) {
        Map<String, Object> sorted = new LinkedHashMap<>();
        values.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(entry -> sorted.put(entry.getKey(), entry.getValue()));
        return Collections.unmodifiableMap(sorted);
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
        Set<String> addedTokens = new java.util.LinkedHashSet<>();
        for (PipelineYamlStep step : config.steps()) {
            if (step == null || step.name() == null) {
                continue;
            }
            String token = normalizeStepToken(step.name());
            PipelineStepModel model = byToken.get(token);
            if (model != null && added.add(model)) {
                ordered.add(model);
                addedTokens.add(token);
            }
        }
        for (PipelineStepModel model : models) {
            String token = normalizeStepToken(stepTokenFromModel(model));
            if (!model.sideEffect() && !addedTokens.contains(token) && added.add(model)) {
                ordered.add(model);
                addedTokens.add(token);
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
