/*
 * Copyright (c) 2023-2025 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework.config.template;

import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Logger;

import org.pipelineframework.config.PlatformOverrideResolver;
import org.pipelineframework.config.TransportOverrideResolver;
import org.pipelineframework.config.boundary.*;
import org.pipelineframework.config.pipeline.BranchRoutingRules;
import org.pipelineframework.materialization.MaterializationAction;
import org.pipelineframework.materialization.MaterializationPosition;
import org.pipelineframework.materialization.MaterializationScope;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

/**
 * Loads the pipeline template configuration used by the template generator.
 */
public class PipelineTemplateConfigLoader {
    private static final Logger LOG = Logger.getLogger(PipelineTemplateConfigLoader.class.getName());
    private static final int MAX_NESTING_DEPTH = 100;
    private static final String DEFAULT_TRANSPORT = "GRPC";
    private static final PipelinePlatform DEFAULT_PLATFORM = PipelinePlatform.COMPUTE;
    private final Function<String, String> propertyLookup;
    private final Function<String, String> envLookup;
    private final Consumer<String> warningReporter;
    private boolean warnedAuthoredFieldNumber;
    private boolean warnedOptional;
    private boolean warnedAuthoredUnionNumber;

    /**
     * Creates a new PipelineTemplateConfigLoader.
     */
    public PipelineTemplateConfigLoader() {
        this(System::getProperty, System::getenv, LOG::warning);
    }

    /**
     * Creates a loader that uses the provided functions to resolve system property and environment variable lookups.
     *
     * If either function is null, it will be replaced with a lookup that always returns null.
     *
     * @param propertyLookup function that returns a property value for a given key, or null to disable property lookups
     * @param envLookup function that returns an environment value for a given key, or null to disable environment lookups
     */
    public PipelineTemplateConfigLoader(Function<String, String> propertyLookup, Function<String, String> envLookup) {
        this(propertyLookup, envLookup, LOG::warning);
    }

    /**
     * Creates a loader with an injectable warning reporter.
     */
    public PipelineTemplateConfigLoader(
        Function<String, String> propertyLookup,
        Function<String, String> envLookup,
        Consumer<String> warningReporter
    ) {
        this.propertyLookup = propertyLookup == null ? key -> null : propertyLookup;
        this.envLookup = envLookup == null ? key -> null : envLookup;
        this.warningReporter = warningReporter == null ? LOG::warning : warningReporter;
    }

    /**
     * Load a pipeline template YAML file and construct a PipelineTemplateConfig.
     *
     * <p>Parses the YAML at the given path and builds a configuration object. For version 2 and
     * later, top-level and inline message definitions are collected and normalized and step inputs/
     * outputs are resolved; for version 1, the returned config contains no messages.
     *
     * @param configPath the path to the pipeline template YAML file
     * @return a PipelineTemplateConfig built from the file's contents
     * @throws IllegalStateException if the YAML root is not a map or the file cannot be read or parsed
     */
    public PipelineTemplateConfig load(Path configPath) {
        warnedAuthoredFieldNumber = false;
        warnedOptional = false;
        warnedAuthoredUnionNumber = false;
        Object root = loadYaml(configPath);
        if (!(root instanceof Map<?, ?> rootMap)) {
            throw new IllegalStateException("Pipeline template config root is not a map");
        }

        int version = readVersion(rootMap);
        PipelineTemplateDialect dialect = PipelineTemplateDialect.fromVersion(version);
        String appName = readString(rootMap, "appName");
        String basePackage = readString(rootMap, "basePackage");
        String transport = normalizeTransport(readString(rootMap, "transport"));
        PipelinePlatform resolvedPlatform = normalizePlatform(readString(rootMap, "platform"));

        if (dialect == PipelineTemplateDialect.V1 && (rootMap.containsKey("types") || rootMap.containsKey("messages"))) {
            throw new IllegalStateException("Top-level types/messages require version: 2");
        }

        if (dialect == PipelineTemplateDialect.V3) {
            return loadV3(rootMap, version, appName, basePackage, transport, resolvedPlatform);
        }

        Map<String, PipelineTemplateMessage> rawMessages = dialect == PipelineTemplateDialect.V2
            ? readNamedTypes(rootMap)
            : new LinkedHashMap<>();
        Map<String, PipelineTemplateUnion> rawUnions = dialect == PipelineTemplateDialect.V2
            ? readUnions(rootMap)
            : new LinkedHashMap<>();
        Map<String, PipelineObjectSourceConfig> sources = readSources(rootMap);
        Map<String, PipelineObjectPublishConfig> publish = readPublishTargets(rootMap);
        List<PipelineTemplateStep> steps = readSteps(rootMap, version);
        Map<String, PipelineTemplateAspect> aspects = readAspects(rootMap);
        PipelineTemplateMaterialization materialization = readMaterialization(rootMap);
        rejectLegacyConnectors(rootMap);
        String inputContract = readLogicalContract(rootMap, "input", version);
        String outputContract = readLogicalContract(rootMap, "output", version);
        PipelineInputBoundaryConfig input = readInputBoundary(rootMap);
        validateObjectInputSource(input, sources);
        PipelineOutputBoundaryConfig output = readOutputBoundary(rootMap).orElse(null);
        validateObjectOutputTarget(output, publish);

        if (dialect == PipelineTemplateDialect.V2) {
            collectInlineMessages(rawMessages, steps);
            Map<String, PipelineTemplateMessage> normalizedMessages = normalizeMessages(rawMessages);
            Map<String, PipelineTemplateUnion> normalizedUnions = normalizeUnions(rawUnions, normalizedMessages);
            steps = new PipelineTemplateContractNormalizer().normalize(
                inputContract,
                outputContract,
                steps,
                normalizedMessages,
                normalizedUnions);
            validateMaterialization(materialization, normalizedMessages, steps);
            steps = resolveV2Steps(steps, normalizedMessages, normalizedUnions);
            return new PipelineTemplateConfig(
                version,
                appName,
                basePackage,
                transport,
                resolvedPlatform,
                normalizedMessages,
                normalizedUnions,
                sources,
                publish,
                steps,
                aspects,
                input,
                output,
                materialization,
                inputContract,
                outputContract);
        }

        return new PipelineTemplateConfig(
            version,
            appName,
            basePackage,
            transport,
            resolvedPlatform,
            Map.of(),
            Map.of(),
            sources,
            publish,
            steps,
            aspects,
            input,
            output,
            materialization,
            null,
            null);
    }

    private PipelineTemplateConfig loadV3(
        Map<?, ?> rootMap,
        int version,
        String appName,
        String basePackage,
        String transport,
        PipelinePlatform platform
    ) {
        if (rootMap.containsKey("messages")) {
            throw new IllegalStateException("Top-level 'messages' is not supported in version: 3; use 'types'.");
        }
        if (rootMap.containsKey("unions")) {
            throw new IllegalStateException("Top-level 'unions' is not supported in version: 3; declare variants under 'types'.");
        }
        PipelineTemplateTypeModel typeModel = readV3Types(rootMap);
        Map<String, PipelineObjectSourceConfig> sources = readSources(rootMap);
        Map<String, PipelineObjectPublishConfig> publish = readPublishTargets(rootMap);
        List<PipelineTemplateStep> steps = readSteps(rootMap, version);
        Map<String, PipelineTemplateAspect> aspects = readAspects(rootMap);
        PipelineTemplateMaterialization materialization = readMaterialization(rootMap);
        if (!materialization.aspects().isEmpty()) {
            throw new IllegalStateException("Version: 3 does not support materialization declarations.");
        }
        rejectLegacyConnectors(rootMap);
        String inputContract = readLogicalContract(rootMap, "input", version);
        String outputContract = readLogicalContract(rootMap, "output", version);
        PipelineInputBoundaryConfig input = readInputBoundary(rootMap);
        validateObjectInputSource(input, sources);
        PipelineOutputBoundaryConfig output = readOutputBoundary(rootMap).orElse(null);
        validateObjectOutputTarget(output, publish);
        validateV3Contracts(typeModel, inputContract, outputContract, steps);
        return new PipelineTemplateConfig(version, appName, basePackage, transport, platform, Map.of(), Map.of(), sources,
            publish, steps, aspects, input, output, materialization, inputContract, outputContract, typeModel);
    }

    private PipelineTemplateTypeModel readV3Types(Map<?, ?> rootMap) {
        Object typesObj = rootMap.get("types");
        if (!(typesObj instanceof Map<?, ?> typesMap) || typesMap.isEmpty()) {
            throw new IllegalStateException("Version: 3 requires a non-empty top-level 'types' map.");
        }
        Map<String, PipelineTemplateTypeDefinition> definitions = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : typesMap.entrySet()) {
            String name = stringify(entry.getKey());
            if (name == null || name.isBlank()) {
                throw new IllegalStateException("Version: 3 type names must not be blank.");
            }
            if (!(entry.getValue() instanceof Map<?, ?> declaration)) {
                throw new IllegalStateException("Type '" + name + "' must be declared as a YAML map.");
            }
            definitions.put(name, readV3Type(name, declaration));
        }
        return new PipelineTemplateTypeModel(definitions);
    }

    private PipelineTemplateTypeDefinition readV3Type(String name, Map<?, ?> declaration) {
        if (declaration.containsKey("number") || declaration.containsKey("optional") || declaration.containsKey("reserved")) {
            throw new IllegalStateException("Type '" + name + "' cannot declare protobuf wire metadata in version: 3.");
        }
        if (declaration.containsKey("pattern")) {
            throw new IllegalStateException("Type '" + name + "' pattern constraints arrive in #507 and are not supported yet.");
        }
        rejectUnexpectedV3Keys(declaration, name, "fields", "wraps", "alias", "variants");
        boolean fields = declaration.containsKey("fields");
        boolean wraps = declaration.containsKey("wraps");
        boolean alias = declaration.containsKey("alias");
        boolean variants = declaration.containsKey("variants");
        int forms = (fields ? 1 : 0) + (wraps ? 1 : 0) + (alias ? 1 : 0) + (variants ? 1 : 0);
        if (forms != 1) {
            throw new IllegalStateException("Type '" + name + "' must declare exactly one of fields, wraps, alias, or variants.");
        }
        if (fields) {
            return new PipelineTemplateTypeDefinition.RecordType(name, readV3RecordFields(name, declaration.get("fields")));
        }
        if (wraps) {
            String scalar = requiredV3String(declaration, "wraps", name);
            if (!PipelineTemplateTypeMappings.isV3ScalarType(scalar)) {
                throw new IllegalStateException("Type '" + name + "' wraps must reference a supported scalar.");
            }
            return new PipelineTemplateTypeDefinition.WrapperType(name, new PipelineTemplateTypeReference.Scalar(scalar));
        }
        if (alias) {
            return new PipelineTemplateTypeDefinition.AliasType(name,
                readV3Reference(requiredV3String(declaration, "alias", name), name + ".alias"));
        }
        return new PipelineTemplateTypeDefinition.UnionType(name, readV3Variants(name, declaration.get("variants")));
    }

    private List<PipelineTemplateTypeDefinition.Field> readV3RecordFields(String owner, Object fieldsObj) {
        if (!(fieldsObj instanceof Iterable<?> fields)) {
            throw new IllegalStateException("Type '" + owner + "' fields must be a YAML list.");
        }
        List<PipelineTemplateTypeDefinition.Field> result = new ArrayList<>();
        int index = 0;
        for (Object fieldObj : fields) {
            String fieldName;
            String fieldType;
            if (fieldObj instanceof List<?> tuple) {
                if (tuple.size() != 2) {
                    throw new IllegalStateException("Type '" + owner + "' field " + index + " must be exactly [nonBlankName, type].");
                }
                fieldName = stringify(tuple.get(0));
                fieldType = stringify(tuple.get(1));
            } else if (fieldObj instanceof Map<?, ?> fieldMap) {
                if (fieldMap.containsKey("number") || fieldMap.containsKey("optional") || fieldMap.containsKey("reserved")) {
                    throw new IllegalStateException("Type '" + owner + "' field " + index + " cannot declare protobuf wire metadata in version: 3.");
                }
                rejectUnexpectedV3Keys(fieldMap, owner + " field " + index, "name", "type");
                fieldName = stringify(fieldMap.get("name"));
                fieldType = stringify(fieldMap.get("type"));
            } else {
                throw new IllegalStateException("Type '" + owner + "' field " + index + " must be an object or [name, type] tuple.");
            }
            if (fieldName == null || fieldName.isBlank() || fieldType == null || fieldType.isBlank()) {
                throw new IllegalStateException("Type '" + owner + "' field " + index + " must be exactly [nonBlankName, type].");
            }
            result.add(new PipelineTemplateTypeDefinition.Field(fieldName, readV3Reference(fieldType, owner + "." + fieldName)));
            index++;
        }
        return List.copyOf(result);
    }

    private Map<String, PipelineTemplateTypeDefinition.Variant> readV3Variants(String unionName, Object variantsObj) {
        if (!(variantsObj instanceof Map<?, ?> variants) || variants.isEmpty()) {
            throw new IllegalStateException("Union '" + unionName + "' must declare variants as a non-empty YAML map.");
        }
        Map<String, PipelineTemplateTypeDefinition.Variant> result = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : variants.entrySet()) {
            String discriminator = stringify(entry.getKey());
            String payload = stringify(entry.getValue());
            if (discriminator == null || discriminator.isBlank() || payload == null || payload.isBlank()) {
                throw new IllegalStateException("Union '" + unionName + "' variants must map a non-blank discriminator to a named type.");
            }
            if (PipelineTemplateTypeMappings.isV3ScalarType(payload)) {
                throw new IllegalStateException("Union '" + unionName + "' variant '" + discriminator + "' must reference a named type.");
            }
            if (payload.contains("<") || payload.contains(".")) {
                throw new IllegalStateException("Union '" + unionName + "' variant '" + discriminator + "' must reference a named type.");
            }
            result.put(discriminator, new PipelineTemplateTypeDefinition.Variant(discriminator,
                new PipelineTemplateTypeReference.Named(payload)));
        }
        return result;
    }

    private PipelineTemplateTypeReference readV3Reference(String value, String owner) {
        if (value.contains("<") || value.contains(">") || value.contains("[") || value.contains("]")) {
            throw new IllegalStateException("Type '" + owner + "' uses an unsupported v3 type expression '" + value + "'.");
        }
        if (PipelineTemplateTypeMappings.isV3ScalarType(value)) {
            return new PipelineTemplateTypeReference.Scalar(value);
        }
        if (!PipelineTemplateTypeMappings.isMessageReferenceToken(value)) {
            throw new IllegalStateException("Type '" + owner + "' must reference a supported scalar or named type, got '" + value + "'.");
        }
        return new PipelineTemplateTypeReference.Named(value);
    }

    private String requiredV3String(Map<?, ?> values, String key, String owner) {
        String value = stringify(values.get(key));
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Type '" + owner + "' " + key + " must be a non-blank YAML string.");
        }
        return value;
    }

    private void rejectUnexpectedV3Keys(Map<?, ?> values, String owner, String... allowed) {
        Set<String> permitted = Set.of(allowed);
        for (Object rawKey : values.keySet()) {
            String key = stringify(rawKey);
            if (key == null || !permitted.contains(key)) {
                throw new IllegalStateException("Type '" + owner + "' contains unsupported version: 3 property '" + key + "'.");
            }
        }
    }

    private void validateV3Contracts(
        PipelineTemplateTypeModel typeModel,
        String inputContract,
        String outputContract,
        List<PipelineTemplateStep> steps
    ) {
        if (inputContract != null && !typeModel.contains(inputContract)) {
            throw new IllegalStateException("Pipeline contract references unknown type '" + inputContract + "'.");
        }
        if (outputContract != null && !typeModel.contains(outputContract)) {
            throw new IllegalStateException("Pipeline contract references unknown type '" + outputContract + "'.");
        }
        for (PipelineTemplateStep step : steps) {
            requireLogicalContract(step.name(), "input", step.inputTypeName());
            requireLogicalContract(step.name(), "output", step.outputTypeName());
            if (!typeModel.contains(step.inputTypeName())) {
                throw new IllegalStateException("Step '" + step.name() + "' references unknown input type '" + step.inputTypeName() + "'.");
            }
            if (!typeModel.contains(step.outputTypeName())) {
                throw new IllegalStateException("Step '" + step.name() + "' references unknown output type '" + step.outputTypeName() + "'.");
            }
        }
        if (steps.isEmpty()) {
            return;
        }
        PipelineTemplateStep first = steps.getFirst();
        if (inputContract != null && !typeModel.isAssignable(inputContract, first.inputTypeName())) {
            throw new IllegalStateException("Pipeline input contract '" + inputContract + "' is not assignable to first step '"
                + first.name() + "' input '" + first.inputTypeName() + "'.");
        }
        for (int index = 1; index < steps.size(); index++) {
            PipelineTemplateStep previous = steps.get(index - 1);
            PipelineTemplateStep current = steps.get(index);
            if (!typeModel.isAssignable(previous.outputTypeName(), current.inputTypeName())) {
                throw new IllegalStateException("Step '" + previous.name() + "' output '" + previous.outputTypeName()
                    + "' is not assignable to step '" + current.name() + "' input '" + current.inputTypeName() + "'.");
            }
        }
        PipelineTemplateStep last = steps.getLast();
        if (outputContract != null && !typeModel.isAssignable(last.outputTypeName(), outputContract)) {
            throw new IllegalStateException("Final step '" + last.name() + "' output '" + last.outputTypeName()
                + "' is not assignable to pipeline output contract '" + outputContract + "'.");
        }
    }

    /**
     * Load and parse a YAML document from the given file path.
     *
     * @param configPath the path to the YAML configuration file
     * @return the parsed YAML document as an Object (for example, a Map, List, or scalar)
     * @throws IllegalStateException if the file cannot be read or opened
     */
    private Object loadYaml(Path configPath) {
        LoaderOptions loaderOptions = new LoaderOptions();
        loaderOptions.setCodePointLimit(3_000_000);
        loaderOptions.setMaxAliasesForCollections(50);
        loaderOptions.setAllowDuplicateKeys(false);
        Yaml yaml = new Yaml(new SafeConstructor(loaderOptions));
        try (Reader reader = Files.newBufferedReader(configPath)) {
            return yaml.load(reader);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read pipeline template config: " + configPath, e);
        }
    }

    private int readVersion(Map<?, ?> rootMap) {
        Object rawVersion = rootMap.get("version");
        if (rawVersion == null) {
            return 1;
        }
        if (!(rawVersion instanceof Number number)) {
            throw new IllegalStateException("Template version must be numeric, got '" + rawVersion + "'");
        }
        return strictIntegerValue(number, "version");
    }

    /**
     * Normalizes a transport name, applying any configured override and falling back to the class default when unknown.
     *
     * @param transport the transport name from the template config; may be null or blank
     * @return the normalized transport name if recognized, otherwise DEFAULT_TRANSPORT
     */
    private String normalizeTransport(String transport) {
        transport = transport == null ? null : transport.trim();
        String transportOverride = resolveTransportOverride();
        boolean transportFromOverride = transportOverride != null && !transportOverride.isBlank();
        if (transportFromOverride) {
            transport = transportOverride.trim();
        }
        String originalTransport = transport;
        String normalizedTransport = TransportOverrideResolver.normalizeKnownTransport(transport);
        if (normalizedTransport == null) {
            if (originalTransport != null && !originalTransport.isBlank()) {
                if (transportFromOverride) {
                    LOG.warning("Unknown transport override '" + originalTransport + "'; defaulting to "
                        + DEFAULT_TRANSPORT + ".");
                } else {
                    LOG.warning("Unknown transport in template config '" + originalTransport + "'; defaulting to "
                        + DEFAULT_TRANSPORT + ".");
                }
            }
            return DEFAULT_TRANSPORT;
        }
        return normalizedTransport;
    }

    /**
     * Normalize a platform name into a PipelinePlatform, applying any configured override and falling back to the default when the name is missing or unrecognized.
     *
     * @param platform the platform name from the configuration; may be null or blank
     * @return the resolved PipelinePlatform — the override or normalized platform when recognized, otherwise the default platform
     */
    private PipelinePlatform normalizePlatform(String platform) {
        platform = platform == null ? null : platform.trim();
        String platformOverride = resolvePlatformOverride();
        boolean platformFromOverride = platformOverride != null && !platformOverride.isBlank();
        if (platformFromOverride) {
            platform = platformOverride.trim();
        }
        String normalizedPlatform = PlatformOverrideResolver.normalizeKnownPlatform(platform);
        if (normalizedPlatform == null && platform != null && !platform.isBlank()) {
            if (platformFromOverride) {
                LOG.warning("Unknown platform override '" + platform + "'; defaulting to " + DEFAULT_PLATFORM + ".");
            } else {
                LOG.warning("Unknown platform in template config '" + platform
                    + "'; defaulting to " + DEFAULT_PLATFORM + ".");
            }
        }
        return PipelinePlatform.fromStringOptional(normalizedPlatform).orElse(DEFAULT_PLATFORM);
    }

    /**
     * Parses the top-level "messages" section from the provided YAML root map into named message definitions.
     *
     * Reads each entry under the "messages" key (if present and a map), parses its "fields" and "reserved"
     * subsections, and returns a map keyed by message name to the corresponding PipelineTemplateMessage.
     *
     * @param rootMap the parsed YAML root mapping (typically from the config file)
     * @return a map of message name to PipelineTemplateMessage; empty if no valid "messages" section is present
     * @throws IllegalStateException if a message name conflicts with a built-in semantic type
     */
    private Map<String, PipelineTemplateMessage> readNamedTypes(Map<?, ?> rootMap) {
        boolean hasTypes = rootMap.containsKey("types");
        boolean hasMessages = rootMap.containsKey("messages");
        if (hasTypes && hasMessages) {
            throw new IllegalStateException(
                "Pipeline template cannot declare both 'types' and deprecated 'messages'; use 'types' only.");
        }
        if (hasMessages) {
            warningReporter.accept("Top-level 'messages' is deprecated; use 'types'.");
        }
        Object messagesObj = rootMap.get(hasTypes ? "types" : "messages");
        if (!(messagesObj instanceof Map<?, ?> messagesMap)) {
            return new LinkedHashMap<>();
        }

        Map<String, PipelineTemplateMessage> messages = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : messagesMap.entrySet()) {
            String name = stringify(entry.getKey());
            if (name == null || name.isBlank()) {
                continue;
            }
            if (PipelineTemplateTypeMappings.isBuiltinType(name)) {
                throw new IllegalStateException("Message name '" + name + "' conflicts with a built-in semantic type");
            }
            if ("PayloadReference".equals(name)) {
                throw new IllegalStateException("Message name 'PayloadReference' is reserved for payload_ref fields");
            }
            if (!(entry.getValue() instanceof Map<?, ?> messageMap)) {
                LOG.warning("Skipping malformed message entry: key=" + name
                    + " valueType=" + (entry.getValue() == null ? "null" : entry.getValue().getClass().getName()));
                continue;
            }
            List<PipelineTemplateField> fields = readFields(messageMap.get("fields"), 2, "type '" + name + "'");
            PipelineTemplateReserved reserved = readReserved(messageMap.get("reserved"));
            messages.put(name, new PipelineTemplateMessage(name, fields, reserved));
        }
        return messages;
    }

    private Map<String, PipelineTemplateUnion> readUnions(Map<?, ?> rootMap) {
        Object unionsObj = rootMap.get("unions");
        Map<?, ?> unionsMap = asMap(unionsObj);
        if (unionsMap == null) {
            return new LinkedHashMap<>();
        }

        Map<String, PipelineTemplateUnion> unions = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : unionsMap.entrySet()) {
            String name = stringify(entry.getKey());
            if (name == null || name.isBlank()) {
                continue;
            }
            if (PipelineTemplateTypeMappings.isBuiltinType(name)) {
                throw new IllegalStateException("Union name '" + name + "' conflicts with a built-in semantic type");
            }
            Map<?, ?> unionMap = asMap(entry.getValue());
            if (unionMap == null) {
                LOG.warning("Skipping malformed union entry: key=" + name
                    + " valueType=" + (entry.getValue() == null ? "null" : entry.getValue().getClass().getName()));
                continue;
            }
            unions.put(name, new PipelineTemplateUnion(name, readUnionVariants(name, unionMap.get("variants"))));
        }
        return unions;
    }

    private Map<String, PipelineTemplateUnionVariant> readUnionVariants(String unionName, Object variantsObj) {
        Map<?, ?> variantsMap = asMap(variantsObj);
        if (variantsMap == null) {
            throw new IllegalStateException("Union '" + unionName + "' must declare variants as a YAML map");
        }
        Map<String, PipelineTemplateUnionVariant> variants = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : variantsMap.entrySet()) {
            String name = stringify(entry.getKey());
            if (name == null || name.isBlank()) {
                throw new IllegalStateException("Union '" + unionName + "' declares a blank variant name");
            }
            Map<?, ?> variantMap = asMap(entry.getValue());
            if (variantMap == null) {
                throw new IllegalStateException("Union '" + unionName + "' variant '" + name + "' must be a YAML map");
            }
            Integer number = readIntegerObject(variantMap, "number");
            if (number != null && !warnedAuthoredUnionNumber) {
                warningReporter.accept("Union '" + unionName + "' variant '" + name
                    + "' uses deprecated authored number; move it to pipeline.idl.json");
                warnedAuthoredUnionNumber = true;
            }
            variants.put(name, new PipelineTemplateUnionVariant(
                name,
                readString(variantMap, "type"),
                number));
        }
        return variants;
    }

    @SuppressWarnings("unchecked")
    private Map<?, ?> asMap(Object value) {
        if (!Map.class.isInstance(value)) {
            return null;
        }
        return (Map<?, ?>) value;
    }

    /**
     * Scans steps for inline message definitions and adds them to the provided messages map.
     *
     * For each step, inspects its input and output type names and field lists and ensures any
     * inline message definitions present on the step are collected into the messages map.
     *
     * @param messages map to populate with discovered inline message definitions; keys are message names
     * @param steps    list of pipeline steps to scan for inline message definitions
     */
    private void collectInlineMessages(Map<String, PipelineTemplateMessage> messages, List<PipelineTemplateStep> steps) {
        for (PipelineTemplateStep step : steps) {
            collectInlineMessage(messages, step.inputTypeName(), step.inputFields());
            collectInlineMessage(messages, step.outputTypeName(), step.outputFields());
        }
    }

    /**
     * Adds an inline message definition to the provided messages map or validates it against an existing definition.
     *
     * If the type name is null/blank or inline fields are null/empty, this method does nothing. If no message with
     * the given name exists, a new PipelineTemplateMessage is created (with empty reserved lists) and stored. If a
     * message already exists, its fields must exactly match the provided inline fields; otherwise an
     * IllegalStateException is thrown.
     *
     * @param messages     map of message name to PipelineTemplateMessage to update
     * @param typeName     name of the inline message type
     * @param inlineFields fields defined inline for the message
     * @throws IllegalStateException if a message with the same name exists but its fields differ from inlineFields
     */
    private void collectInlineMessage(
        Map<String, PipelineTemplateMessage> messages,
        String typeName,
        List<PipelineTemplateField> inlineFields
    ) {
        if (typeName == null || typeName.isBlank() || inlineFields == null || inlineFields.isEmpty()) {
            return;
        }
        if (PipelineTemplateTypeMappings.isBuiltinType(typeName)) {
            throw new IllegalStateException("Message name '" + typeName + "' conflicts with a built-in semantic type");
        }
        if ("PayloadReference".equals(typeName)) {
            throw new IllegalStateException("Message name 'PayloadReference' is reserved for payload_ref fields");
        }
        PipelineTemplateMessage existing = messages.get(typeName);
        if (existing == null) {
            messages.put(typeName, new PipelineTemplateMessage(typeName, inlineFields, new PipelineTemplateReserved(List.of(), List.of())));
            return;
        }
        if (!existing.fields().equals(inlineFields)) {
            throw new IllegalStateException(
                "Inline message '" + typeName + "' conflicts with existing definition. Existing fields: "
                    + summarizeFields(existing.fields()) + "; inline fields: " + summarizeFields(inlineFields));
        }
    }

    /**
     * Normalize each message's fields against the set of known message names and validate reserved entries.
     *
     * @param rawMessages map of message name to raw PipelineTemplateMessage to normalize
     * @return a new map of message name to PipelineTemplateMessage with fields normalized and reserved validated
     * @throws IllegalArgumentException if a message's reserved definitions or field references are invalid
     */
    private Map<String, PipelineTemplateMessage> normalizeMessages(Map<String, PipelineTemplateMessage> rawMessages) {
        Set<String> knownNames = new LinkedHashSet<>(rawMessages.keySet());
        List<String> knownNamesList = List.copyOf(knownNames);
        Map<String, PipelineTemplateMessage> normalized = new LinkedHashMap<>();
        for (Map.Entry<String, PipelineTemplateMessage> entry : rawMessages.entrySet()) {
            String name = entry.getKey();
            PipelineTemplateMessage rawMessage = entry.getValue();
            List<PipelineTemplateField> normalizedFields = new ArrayList<>();
            for (PipelineTemplateField field : rawMessage.fields()) {
                normalizedFields.add(PipelineTemplateTypeMappings.normalizeV2Field(field, knownNamesList));
            }
            PipelineTemplateReserved reserved = rawMessage.reserved();
            validateReserved(name, normalizedFields, reserved);
            validateReferenceableMetadata(name, normalizedFields);
            normalized.put(name, new PipelineTemplateMessage(name, normalizedFields, reserved));
        }
        return normalized;
    }

    private void validateReferenceableMetadata(String messageName, List<PipelineTemplateField> fields) {
        Map<String, PipelineTemplateField> fieldsByName = new LinkedHashMap<>();
        for (PipelineTemplateField field : fields) {
            fieldsByName.put(field.name(), field);
        }
        for (PipelineTemplateField field : fields) {
            PipelineTemplateFieldReference reference = field.referenceable();
            if (reference == null) {
                continue;
            }
            validateReferenceableField("field metadata", messageName, field, fieldsByName);
        }
    }

    private Map<String, PipelineTemplateUnion> normalizeUnions(
        Map<String, PipelineTemplateUnion> rawUnions,
        Map<String, PipelineTemplateMessage> messages
    ) {
        if (rawUnions == null || rawUnions.isEmpty()) {
            return Map.of();
        }
        Map<String, PipelineTemplateUnion> normalized = new LinkedHashMap<>();
        for (Map.Entry<String, PipelineTemplateUnion> entry : rawUnions.entrySet()) {
            String unionName = entry.getKey();
            PipelineTemplateUnion union = entry.getValue();
            if (messages.containsKey(unionName)) {
                throw new IllegalStateException("Union name '" + unionName + "' conflicts with a message name");
            }
            Set<String> variantNames = new LinkedHashSet<>();
            Set<Integer> variantNumbers = new LinkedHashSet<>();
            Map<String, PipelineTemplateUnionVariant> variants = new LinkedHashMap<>();
            for (PipelineTemplateUnionVariant variant : union.variants().values()) {
                if (variant.name() == null || variant.name().isBlank()) {
                    throw new IllegalStateException("Union '" + unionName + "' declares a blank variant name");
                }
                if (!variantNames.add(variant.name())) {
                    throw new IllegalStateException(
                        "Duplicate variant name '" + variant.name() + "' in union '" + unionName + "'");
                }
                if (variant.number() != null && variant.number() <= 0) {
                    throw new IllegalStateException(
                        "Union '" + unionName + "' variant '" + variant.name() + "' number must be positive");
                }
                if (variant.number() != null && !variantNumbers.add(variant.number())) {
                    throw new IllegalStateException(
                        "Duplicate variant number " + variant.number() + " in union '" + unionName + "'");
                }
                if (variant.type() == null || variant.type().isBlank()) {
                    throw new IllegalStateException(
                        "Union '" + unionName + "' variant '" + variant.name() + "' must declare type");
                }
                if (!messages.containsKey(variant.type()) && !PipelineTemplateTypeMappings.isV3ScalarType(variant.type())) {
                    throw new IllegalStateException(
                        "Union '" + unionName + "' variant '" + variant.name()
                            + "' references unknown message '" + variant.type() + "'");
                }
                variants.put(variant.name(), variant);
            }
            if (variants.isEmpty()) {
                throw new IllegalStateException("Union '" + unionName + "' must declare at least one variant");
            }
            normalized.put(unionName, new PipelineTemplateUnion(unionName, variants));
        }
        return normalized;
    }

    /**
     * Resolve each step's input and output field lists against the provided top-level message definitions.
     *
     * @param steps    the list of steps whose input/output fields should be resolved
     * @param messages a map from message name to message definition used to resolve step field lists
     * @return a new list of PipelineTemplateStep where each step has its inputFields and outputFields replaced by the resolved field lists
     */
    private List<PipelineTemplateStep> resolveV2Steps(
        List<PipelineTemplateStep> steps,
        Map<String, PipelineTemplateMessage> messages,
        Map<String, PipelineTemplateUnion> unions
    ) {
        List<PipelineTemplateStep> resolved = new ArrayList<>();
        for (PipelineTemplateStep step : steps) {
            requireLogicalContract(step.name(), "input", step.inputTypeName());
            requireLogicalContract(step.name(), "output", step.outputTypeName());
            List<PipelineTemplateField> inputFields = resolveStepFields(
                step.inputTypeName(), step.inputFields(), messages, unions, step.name(), "input");
            List<PipelineTemplateField> outputFields = resolveStepFields(
                step.outputTypeName(), step.outputFields(), messages, unions, step.name(), "output");
            if (step.execution() != null && step.execution().isRemote()
                && !"ONE_TO_ONE".equalsIgnoreCase(step.cardinality())) {
                throw new IllegalStateException(
                    "Step '" + step.name() + "' remote execution currently supports only cardinality ONE_TO_ONE");
            }
            resolved.add(new PipelineTemplateStep(
                step.name(),
                step.cardinality(),
                step.inputTypeName(),
                inputFields,
                step.inboundMapper(),
                step.outputTypeName(),
                outputFields,
                step.outboundMapper(),
                step.execution(),
                step.accepts(),
                step.terminal()));
        }
        return resolved;
    }

    private void requireLogicalContract(String stepName, String direction, String contract) {
        if (contract == null || contract.isBlank()) {
            throw new IllegalStateException("Step '" + stepName + "' requires a declared logical " + direction
                + " contract. Use '" + direction + "' for the named pipeline type; put any Java binding under 'java."
                + direction + "'.");
        }
    }

    /**
     * Resolve the final field list for a step's referenced message type (input or output).
     *
     * If {@code typeName} is null or blank, returns a copy of {@code inlineFields} or an empty list.
     *
     * @param typeName the referenced top-level message name, or null/blank to use inline fields
     * @param inlineFields inline field definitions declared on the step; may be null
     * @param messages map of top-level message name to message definition
     * @param stepName the step's name (used in error messages)
     * @param direction textual direction ("input" or "output") used in error messages
     * @return the resolved list of fields for the referenced message
     * @throws IllegalStateException if {@code typeName} is not found in {@code messages}, or if {@code inlineFields}
     *                               are provided but do not match the top-level message definition
     */
    private List<PipelineTemplateField> resolveStepFields(
        String typeName,
        List<PipelineTemplateField> inlineFields,
        Map<String, PipelineTemplateMessage> messages,
        Map<String, PipelineTemplateUnion> unions,
        String stepName,
        String direction
    ) {
        List<String> knownMessages = List.copyOf(messages.keySet());
        if (typeName == null || typeName.isBlank()) {
            if (inlineFields == null || inlineFields.isEmpty()) {
                return List.of();
            }
            List<PipelineTemplateField> normalizedInline = new ArrayList<>();
            for (PipelineTemplateField field : inlineFields) {
                normalizedInline.add(PipelineTemplateTypeMappings.normalizeV2Field(field, knownMessages));
            }
            validateReserved(stepName + " anonymous " + direction + " contract", normalizedInline,
                new PipelineTemplateReserved(List.of(), List.of()));
            return List.copyOf(normalizedInline);
        }
        if (unions.containsKey(typeName)) {
            if (inlineFields != null && !inlineFields.isEmpty()) {
                throw new IllegalStateException(
                    "Step '" + stepName + "' cannot define inline " + direction
                        + " fields for union '" + typeName + "'");
            }
            return List.of();
        }
        PipelineTemplateMessage message = messages.get(typeName);
        if (message == null) {
            throw new IllegalStateException(
                "Step '" + stepName + "' references unknown " + direction + " message or union '" + typeName + "'");
        }
        if (inlineFields != null && !inlineFields.isEmpty()) {
            List<PipelineTemplateField> normalizedInline = new ArrayList<>();
            for (PipelineTemplateField field : inlineFields) {
                normalizedInline.add(PipelineTemplateTypeMappings.normalizeV2Field(field, knownMessages));
            }
            if (!normalizedInline.equals(message.fields())) {
                throw new IllegalStateException(
                    "Step '" + stepName + "' defines inline " + direction + " fields for '" + typeName
                        + "' that do not match the top-level message definition");
            }
        }
        return message.fields();
    }

    /**
     * Read the top-level "steps" entry from the provided YAML root map and build a list of PipelineTemplateStep objects.
     *
     * <p>Each element under "steps" is expected to be a map with keys such as "name", "cardinality",
     * "inputTypeName", "outputTypeName", "inputFields", and "outputFields". Field entries are parsed
     * according to the supplied configuration version.</p>
     *
     * @param rootMap the root YAML mapping to read the "steps" section from; may be any Map-like object
     * @param version the configuration schema version used to determine how fields are parsed
     * @return a list of parsed PipelineTemplateStep instances; returns an empty list if no valid "steps"
     *         iterable is present or if individual entries are not map-like (those entries are skipped)
     */
    private List<PipelineTemplateStep> readSteps(Map<?, ?> rootMap, int version) {
        Object stepsObj = rootMap.get("steps");
        if (!(stepsObj instanceof Iterable<?> steps)) {
            return List.of();
        }

        List<PipelineTemplateStep> stepInfos = new ArrayList<>();
        for (Object stepObj : steps) {
            if (!(stepObj instanceof Map<?, ?> stepMap)) {
                continue;
            }
            String name = readString(stepMap, "name");
            rejectBranchPredicateKeys(stepMap, name);
            String cardinality = readString(stepMap, "cardinality");
            PipelineTemplateStepContractSyntax.StepContracts contracts =
                PipelineTemplateStepContractSyntax.normalize(stepMap, version, name);
            if (contracts.usesLegacyFqcn()) {
                warningReporter.accept("Step '" + name + "' uses deprecated fully qualified '" + "input/output"
                    + "' contracts; use logical input/output with java.input/java.output instead.");
            }
            String inputType = contracts.logicalInput().orElse(null);
            String outputType = contracts.logicalOutput().orElse(null);
            if (version == 3 && (stepMap.containsKey("inputFields") || stepMap.containsKey("outputFields"))) {
                throw new IllegalStateException("Step '" + name
                    + "' inline inputFields/outputFields are not supported in version: 3; declare types at the top level.");
            }
            List<PipelineTemplateField> inputFields = readFields(stepMap.get("inputFields"), version);
            List<PipelineTemplateField> outputFields = readFields(stepMap.get("outputFields"), version);
            String inboundMapper = readString(stepMap, "inboundMapper");
            String outboundMapper = readString(stepMap, "outboundMapper");
            PipelineTemplateStepExecution execution = readExecution(stepMap.get("execution"), version, name);
            List<String> accepts = readStringList(stepMap, "accepts");
            boolean terminal = readBoolean(stepMap, "terminal", false);
            if (version < 2 && (stepMap.containsKey("accepts") || terminal)) {
                throw new IllegalStateException(
                    "Step '" + name + "' declares accepts/terminal, but branch-aware routing requires version: 2");
            }
            stepInfos.add(new PipelineTemplateStep(
                name,
                cardinality,
                inputType,
                inputFields,
                inboundMapper,
                outputType,
                outputFields,
                outboundMapper,
                execution,
                accepts,
                terminal));
        }
        return stepInfos;
    }

    private void rejectBranchPredicateKeys(Map<?, ?> stepMap, String stepName) {
        BranchRoutingRules.rejectPredicateKeys(stepMap, stepName, IllegalStateException::new);
    }

    private PipelineTemplateStepExecution readExecution(Object executionObj, int version, String stepName) {
        if (executionObj == null) {
            return null;
        }
        if (version < 2) {
            throw new IllegalStateException(
                "Step '" + stepName + "' declares execution metadata, but execution blocks require version: 2");
        }
        if (!(executionObj instanceof Map<?, ?> executionMap)) {
            throw new IllegalStateException("Step '" + stepName + "' execution block must be a map");
        }

        PipelineTemplateStepExecution execution = new PipelineTemplateStepExecution(
            readString(executionMap, "mode"),
            readString(executionMap, "operatorId"),
            readString(executionMap, "protocol"),
            readIntegerObject(executionMap, "timeoutMs"),
            readRemoteTarget(executionMap.get("target"), stepName));
        validateExecution(execution, stepName);
        return execution;
    }

    private PipelineTemplateRemoteTarget readRemoteTarget(Object targetObj, String stepName) {
        if (targetObj == null) {
            return null;
        }
        if (!(targetObj instanceof Map<?, ?> targetMap)) {
            throw new IllegalStateException("Step '" + stepName + "' target block must be a map");
        }
        return new PipelineTemplateRemoteTarget(
            readString(targetMap, "url"),
            readString(targetMap, "urlConfigKey"));
    }

    private void validateExecution(PipelineTemplateStepExecution execution, String stepName) {
        if (execution == null) {
            return;
        }
        if (!execution.isRemote()) {
            throw new IllegalStateException(
                "Step '" + stepName + "' execution block is only supported for REMOTE mode, got '"
                    + execution.mode() + "'");
        }
        if (execution.operatorId() == null) {
            throw new IllegalStateException("Step '" + stepName + "' remote execution requires execution.operatorId");
        }
        if (!"PROTOBUF_HTTP_V1".equalsIgnoreCase(execution.protocol())
            && !"ENVELOPE_HTTP_V1".equalsIgnoreCase(execution.protocol())) {
            throw new IllegalStateException(
                "Step '" + stepName
                    + "' remote execution requires execution.protocol=PROTOBUF_HTTP_V1 or ENVELOPE_HTTP_V1");
        }
        PipelineTemplateRemoteTarget target = execution.target();
        if (target == null) {
            throw new IllegalStateException("Step '" + stepName + "' remote execution requires execution.target");
        }
        boolean hasLiteralUrl = target.url() != null;
        boolean hasConfigKey = target.urlConfigKey() != null;
        if (hasLiteralUrl == hasConfigKey) {
            throw new IllegalStateException(
                "Step '" + stepName + "' remote execution requires exactly one of execution.target.url or execution.target.urlConfigKey");
        }
        if (execution.timeoutMs() != null && execution.timeoutMs() <= 0) {
            throw new IllegalStateException("Step '" + stepName + "' execution.timeoutMs must be > 0");
        }
    }

    /**
     * Parses a collection of field definitions into a list of PipelineTemplateField objects.
     *
     * If {@code fieldsObj} is not iterable an empty list is returned. Each iterable element
     * must be a map to be processed; non-map elements are ignored. For {@code version} >= 2
     * each map is parsed as a v2-style field; otherwise it is parsed as a legacy-style field.
     *
     * @param fieldsObj the raw fields value from the YAML (expected to be an iterable of maps)
     * @param version the template config version to determine v2 vs legacy parsing rules
     * @return a list of parsed PipelineTemplateField instances (empty if {@code fieldsObj} is not iterable)
     */
    private List<PipelineTemplateField> readFields(Object fieldsObj, int version) {
        return readFields(fieldsObj, version, "inline contract");
    }

    private List<PipelineTemplateField> readFields(Object fieldsObj, int version, String owner) {
        if (!(fieldsObj instanceof Iterable<?> fields)) {
            return List.of();
        }

        List<PipelineTemplateField> fieldInfos = new ArrayList<>();
        int index = 0;
        for (Object fieldObj : fields) {
            if (version >= 2 && fieldObj instanceof List<?> tuple) {
                fieldInfos.add(readV2FieldTuple(tuple, owner, index));
                index++;
                continue;
            }
            if (!(fieldObj instanceof Map<?, ?> fieldMap)) {
                index++;
                continue;
            }
            if (version >= 2) {
                fieldInfos.add(readV2Field(fieldMap));
            } else {
                fieldInfos.add(readLegacyField(fieldMap));
            }
            index++;
        }
        return fieldInfos;
    }

    private PipelineTemplateField readV2FieldTuple(List<?> tuple, String owner, int index) {
        String expected = owner + " field " + index
            + " must be exactly [nonBlankName, type]";
        if (tuple.size() == 2) {
            String fieldName = stringify(tuple.get(0));
            String fieldType = stringify(tuple.get(1));
            if (fieldName == null || fieldName.isBlank() || fieldType == null || fieldType.isBlank()) {
                throw new IllegalStateException(expected);
            }
            Map<String, Object> fieldMap = new LinkedHashMap<>();
            fieldMap.put("name", fieldName);
            fieldMap.put("type", fieldType);
            return readV2Field(fieldMap);
        }
        if (tuple.size() != 3 || !(tuple.get(0) instanceof Number number)) {
            throw new IllegalStateException(expected);
        }
        int fieldNumber = number.intValue();
        String fieldName = stringify(tuple.get(1));
        String fieldType = stringify(tuple.get(2));
        if (fieldNumber <= 0 || number.doubleValue() != fieldNumber || fieldName == null || fieldName.isBlank()
            || fieldType == null || fieldType.isBlank()) {
            throw new IllegalStateException(expected);
        }
        Map<String, Object> fieldMap = new LinkedHashMap<>();
        fieldMap.put("number", fieldNumber);
        fieldMap.put("name", fieldName);
        fieldMap.put("type", fieldType);
        return readV2Field(fieldMap);
    }

    /**
     * Create and normalize a legacy-style PipelineTemplateField from a field definition map.
     *
     * @param fieldMap a map containing legacy field properties; expected keys are "name", "type", and "protoType"
     * @return a normalized PipelineTemplateField constructed from the map's "name", "type", and "protoType" values
     */
    private PipelineTemplateField readLegacyField(Map<?, ?> fieldMap) {
        String name = readString(fieldMap, "name");
        String type = readString(fieldMap, "type");
        String protoType = readString(fieldMap, "protoType");
        return PipelineTemplateTypeMappings.normalizeLegacyField(new PipelineTemplateField(name, type, protoType));
    }

    /**
     * Create a PipelineTemplateField from a V2-style field definition map.
     *
     * <p>Recognizes the following keys in fieldMap: "name", "type", "number", "optional",
     * "repeated", "deprecated", "keyType", "valueType", "since", "deprecatedSince", "comment",
     * and "overrides". Values will be converted and assigned to the corresponding PipelineTemplateField
     * properties; missing numeric or string entries result in null, and missing boolean entries default
     * to false.
     *
     * @param fieldMap a map representing a V2 field definition (string keys to scalar or nested values)
     * @return a PipelineTemplateField populated from the provided map
     */
    private PipelineTemplateField readV2Field(Map<?, ?> fieldMap) {
        String name = readString(fieldMap, "name");
        String type = readString(fieldMap, "type");
        Integer number = readIntegerObject(fieldMap, "number");
        if (fieldMap.containsKey("number") && !warnedAuthoredFieldNumber) {
            warningReporter.accept("Field '" + name + "' uses deprecated authored number; move it to pipeline.idl.json");
            warnedAuthoredFieldNumber = true;
        }
        if (fieldMap.containsKey("optional") && !warnedOptional) {
            warningReporter.accept("Field '" + name + "' uses deprecated optional; eligible scalars always have explicit presence");
            warnedOptional = true;
        }
        boolean repeated = readBoolean(fieldMap, "repeated", false);
        boolean deprecated = readBoolean(fieldMap, "deprecated", false);
        String keyType = readString(fieldMap, "keyType");
        String valueType = readString(fieldMap, "valueType");
        String since = readString(fieldMap, "since");
        String deprecatedSince = readString(fieldMap, "deprecatedSince");
        String comment = readString(fieldMap, "comment");
        PipelineTemplateFieldOverrides overrides = readOverrides(fieldMap.get("overrides"));
        PipelineTemplateFieldReference referenceable = readReferenceable(fieldMap.get("referenceable"), name);
        return new PipelineTemplateField(
            number,
            name,
            type,
            null,
            null,
            null,
            null,
            keyType,
            valueType,
            false,
            repeated,
            deprecated,
            since,
            deprecatedSince,
            comment,
            overrides,
            referenceable);
    }

    private PipelineTemplateFieldReference readReferenceable(Object referenceableObj, String fieldName) {
        if (referenceableObj == null) {
            return null;
        }
        if (referenceableObj instanceof Boolean enabled) {
            if (!enabled) {
                return null;
            }
            throw new IllegalStateException("Field '" + fieldName + "' referenceable must declare refField");
        }
        if (!(referenceableObj instanceof Map<?, ?> referenceableMap)) {
            throw new IllegalStateException("Field '" + fieldName + "' referenceable must be a YAML map");
        }
        String refField = readString(referenceableMap, "refField");
        if (refField == null) {
            throw new IllegalStateException("Field '" + fieldName + "' referenceable.refField must not be blank");
        }
        return new PipelineTemplateFieldReference(refField);
    }

    /**
     * Parses an overrides object and extracts a proto encoding override when present.
     *
     * @param overridesObj the raw overrides value (expected to be a Map containing a "proto" Map)
     * @return a {@link PipelineTemplateFieldOverrides} containing a {@link PipelineTemplateProtoOverride}
     *         created from the proto "encoding" value, or `null` if the input is not a map or no proto map is present
     */
    private PipelineTemplateFieldOverrides readOverrides(Object overridesObj) {
        if (!(overridesObj instanceof Map<?, ?> overridesMap)) {
            return null;
        }
        Object protoObj = overridesMap.get("proto");
        if (!(protoObj instanceof Map<?, ?> protoMap)) {
            return null;
        }
        return new PipelineTemplateFieldOverrides(new PipelineTemplateProtoOverride(readString(protoMap, "encoding")));
    }

    /**
     * Parses the YAML 'reserved' section into a PipelineTemplateReserved.
     *
     * @param reservedObj the raw 'reserved' value from the parsed YAML (expected to be a Map
     *                    possibly containing "numbers" and "names" entries)
     * @return a PipelineTemplateReserved containing parsed reserved numbers and names; returns
     *         empty lists when the corresponding entries are absent or not iterable
     * @throws IllegalArgumentException if any entry in "numbers" cannot be parsed as an integer
     */
    private PipelineTemplateReserved readReserved(Object reservedObj) {
        if (!(reservedObj instanceof Map<?, ?> reservedMap)) {
            return new PipelineTemplateReserved(List.of(), List.of());
        }
        List<Integer> numbers = new ArrayList<>();
        Object numbersObj = reservedMap.get("numbers");
        if (numbersObj instanceof Iterable<?> numberList) {
            for (Object numberObj : numberList) {
                if (numberObj instanceof Number number) {
                    numbers.add(strictIntegerValue(number, "reserved.numbers"));
                } else if (numberObj != null) {
                    String text = numberObj.toString().trim();
                    try {
                        numbers.add(Integer.parseInt(text));
                    } catch (NumberFormatException ex) {
                        throw new IllegalArgumentException(
                            "Invalid reserved number value '" + text + "' in reserved.numbers", ex);
                    }
                }
            }
        }
        List<String> names = new ArrayList<>();
        Object namesObj = reservedMap.get("names");
        if (namesObj instanceof Iterable<?> nameList) {
            for (Object nameObj : nameList) {
                String value = stringify(nameObj);
                if (value != null && !value.isBlank()) {
                    names.add(value);
                }
            }
        }
        return new PipelineTemplateReserved(numbers, names);
    }

    /**
     * Ensures reserved field numbers and names for a message are valid and do not conflict with the message's fields.
     *
     * @param messageName the name of the message being validated
     * @param fields the list of fields defined on the message
     * @param reserved the reserved numbers and names to validate
     * @throws IllegalStateException if any reserved number is null or not positive, if any reserved number is duplicated,
     *                               if any reserved name is null or blank, if any reserved name is duplicated,
     *                               or if a defined field reuses a reserved number or reserved name
     */
    private void validateReserved(
        String messageName,
        List<PipelineTemplateField> fields,
        PipelineTemplateReserved reserved
    ) {
        Set<Integer> fieldNumbers = new LinkedHashSet<>();
        Set<String> fieldNames = new LinkedHashSet<>();
        for (PipelineTemplateField field : fields) {
            if (field.number() != null && !fieldNumbers.add(field.number())) {
                throw new IllegalStateException(
                    "Duplicate field number " + field.number() + " in message '" + messageName + "'");
            }
            if (field.name() != null && !field.name().isBlank() && !fieldNames.add(field.name())) {
                throw new IllegalStateException(
                    "Duplicate field name '" + field.name() + "' in message '" + messageName + "'");
            }
        }
        Set<Integer> numbers = new LinkedHashSet<>();
        for (Integer number : reserved.numbers()) {
            if (number == null || number <= 0) {
                throw new IllegalStateException("Reserved number must be positive in message '" + messageName + "'");
            }
            if (!numbers.add(number)) {
                throw new IllegalStateException("Duplicate reserved number " + number + " in message '" + messageName + "'");
            }
        }
        Set<String> names = new LinkedHashSet<>();
        for (String name : reserved.names()) {
            if (name == null || name.isBlank()) {
                throw new IllegalStateException("Reserved field name must not be blank in message '" + messageName + "'");
            }
            if (!names.add(name)) {
                throw new IllegalStateException("Duplicate reserved name '" + name + "' in message '" + messageName + "'");
            }
        }
        for (PipelineTemplateField field : fields) {
            if (field.number() != null && numbers.contains(field.number())) {
                throw new IllegalStateException(
                    "Field '" + field.name() + "' in message '" + messageName + "' reuses reserved number " + field.number());
            }
            if (names.contains(field.name())) {
                throw new IllegalStateException(
                    "Field '" + field.name() + "' in message '" + messageName + "' reuses a reserved field name");
            }
        }
    }

    /**
     * Parse the optional top-level "aspects" section of the YAML into a map of PipelineTemplateAspect instances.
     *
     * The method reads an "aspects" map from the provided YAML root and constructs a LinkedHashMap
     * keyed by aspect name. If the "aspects" entry is missing or not a map, an empty map is returned.
     *
     * Defaults applied when an aspect's fields are missing or blank:
     * - enabled: true
     * - position: "AFTER_STEP"
     * - scope: "GLOBAL"
     * - order: 0
     * - config: empty map
     *
     * @param rootMap the YAML-derived root map that may contain an "aspects" entry
     * @return a map keyed by aspect name to its PipelineTemplateAspect; empty if no valid "aspects" map is present
     */
    private Map<String, PipelineTemplateAspect> readAspects(Map<?, ?> rootMap) {
        Object aspectsObj = rootMap.get("aspects");
        if (!(aspectsObj instanceof Map<?, ?> aspectsMap)) {
            return Map.of();
        }

        Map<String, PipelineTemplateAspect> aspects = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : aspectsMap.entrySet()) {
            String name = entry.getKey() == null ? "" : entry.getKey().toString();
            if (!(entry.getValue() instanceof Map<?, ?> aspectConfig)) {
                continue;
            }

            boolean enabled = readBoolean(aspectConfig, "enabled", true);
            String position = readString(aspectConfig, "position");
            if (position == null || position.isBlank()) {
                position = "AFTER_STEP";
            }
            String scope = readString(aspectConfig, "scope");
            if (scope == null || scope.isBlank()) {
                scope = "GLOBAL";
            }
            int order = readInt(aspectConfig, "order", 0);
            Map<String, Object> config = readConfigMap(aspectConfig.get("config"));
            aspects.put(name, new PipelineTemplateAspect(enabled, scope, position, order, config));
        }
        return aspects;
    }

    private Map<String, PipelineObjectSourceConfig> readSources(Map<?, ?> rootMap) {
        Object sourcesObj = rootMap.get("sources");
        if (sourcesObj == null) {
            return Map.of();
        }
        if (!(sourcesObj instanceof Map<?, ?> sourcesMap)) {
            throw new IllegalArgumentException("pipeline sources must be defined as a YAML map");
        }
        Map<String, PipelineObjectSourceConfig> sources = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : sourcesMap.entrySet()) {
            String name = stringify(entry.getKey());
            if (name == null) {
                throw new IllegalArgumentException("pipeline source name must not be blank");
            }
            if (!(entry.getValue() instanceof Map<?, ?> sourceMap)) {
                throw new IllegalArgumentException("source '" + name + "' must be declared as a YAML map");
            }
            sources.put(name, new PipelineObjectSourceConfig(
                name,
                readRequiredString(sourceMap, "kind", "source '" + name + "'"),
                readRequiredString(sourceMap, "provider", "source '" + name + "'"),
                readObjectMap(sourceMap, "location"),
                readObjectFilter(sourceMap),
                readObjectPoll(sourceMap),
                readObjectIdentity(sourceMap),
                readObjectPayload(sourceMap)));
        }
        return Map.copyOf(sources);
    }

    private Map<String, PipelineObjectPublishConfig> readPublishTargets(Map<?, ?> rootMap) {
        Object publishObj = rootMap.get("publish");
        if (publishObj == null) {
            return Map.of();
        }
        if (!(publishObj instanceof Map<?, ?> publishMap)) {
            throw new IllegalArgumentException("pipeline publish targets must be defined as a YAML map");
        }
        Map<String, PipelineObjectPublishConfig> targets = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : publishMap.entrySet()) {
            String name = stringify(entry.getKey());
            if (name == null) {
                throw new IllegalArgumentException("pipeline publish target name must not be blank");
            }
            if (!(entry.getValue() instanceof Map<?, ?> targetMap)) {
                throw new IllegalArgumentException("publish target '" + name + "' must be declared as a YAML map");
            }
            targets.put(name, new PipelineObjectPublishConfig(
                name,
                readRequiredString(targetMap, "kind", "publish target '" + name + "'"),
                readRequiredString(targetMap, "provider", "publish target '" + name + "'"),
                readObjectMap(targetMap, "location"),
                readObjectNaming(targetMap),
                readObjectPublishPayload(targetMap),
                readObjectPublishGrouping(targetMap)));
        }
        return Map.copyOf(targets);
    }

    private PipelineObjectNamingConfig readObjectNaming(Map<?, ?> targetMap) {
        Object namingObj = targetMap.get("naming");
        if (namingObj == null) {
            return PipelineObjectNamingConfig.defaults();
        }
        if (!(namingObj instanceof Map<?, ?> namingMap)) {
            throw new IllegalArgumentException("publish.naming must be declared as a YAML map");
        }
        return new PipelineObjectNamingConfig(readString(namingMap, "keyTemplate"));
    }

    private PipelineObjectPublishPayloadConfig readObjectPublishPayload(Map<?, ?> targetMap) {
        Object payloadObj = targetMap.get("payload");
        if (payloadObj == null) {
            return PipelineObjectPublishPayloadConfig.defaults();
        }
        if (!(payloadObj instanceof Map<?, ?> payloadMap)) {
            throw new IllegalArgumentException("publish.payload must be declared as a YAML map");
        }
        return new PipelineObjectPublishPayloadConfig(
            readString(payloadMap, "contentType"),
            readCharset(payloadMap, "charset", StandardCharsets.UTF_8));
    }

    private PipelineObjectPublishGroupingConfig readObjectPublishGrouping(Map<?, ?> targetMap) {
        Object groupingObj = targetMap.get("grouping");
        if (groupingObj == null) {
            return PipelineObjectPublishGroupingConfig.defaults();
        }
        if (!(groupingObj instanceof Map<?, ?> groupingMap)) {
            throw new IllegalArgumentException("publish.grouping must be declared as a YAML map");
        }
        return new PipelineObjectPublishGroupingConfig(readInt(groupingMap, "maxOpenGroups", 32));
    }

    private PipelineObjectFilterConfig readObjectFilter(Map<?, ?> sourceMap) {
        Object filterObj = sourceMap.get("filter");
        if (filterObj == null) {
            return PipelineObjectFilterConfig.defaults();
        }
        if (!(filterObj instanceof Map<?, ?> filterMap)) {
            throw new IllegalArgumentException("source.filter must be declared as a YAML map");
        }
        return new PipelineObjectFilterConfig(
            readStringList(filterMap, "include"),
            readStringList(filterMap, "exclude"));
    }

    private PipelineObjectPollConfig readObjectPoll(Map<?, ?> sourceMap) {
        Object pollObj = sourceMap.get("poll");
        if (pollObj == null) {
            return PipelineObjectPollConfig.defaults();
        }
        if (!(pollObj instanceof Map<?, ?> pollMap)) {
            throw new IllegalArgumentException("source.poll must be declared as a YAML map");
        }
        return new PipelineObjectPollConfig(
            readBoolean(pollMap, "enabled", false),
            readDuration(pollMap, "interval", Duration.ofSeconds(30)),
            readInt(pollMap, "batchSize", 100));
    }

    private PipelineObjectIdentityConfig readObjectIdentity(Map<?, ?> sourceMap) {
        Object identityObj = sourceMap.get("identity");
        if (identityObj == null) {
            return PipelineObjectIdentityConfig.defaults();
        }
        if (!(identityObj instanceof Map<?, ?> identityMap)) {
            throw new IllegalArgumentException("source.identity must be declared as a YAML map");
        }
        return new PipelineObjectIdentityConfig(readStringList(identityMap, "fields"));
    }

    private PipelineObjectPayloadConfig readObjectPayload(Map<?, ?> sourceMap) {
        Object payloadObj = sourceMap.get("payload");
        if (payloadObj == null) {
            return PipelineObjectPayloadConfig.reference();
        }
        if (!(payloadObj instanceof Map<?, ?> payloadMap)) {
            throw new IllegalArgumentException("source.payload must be declared as a YAML map");
        }
        return new PipelineObjectPayloadConfig(
            readString(payloadMap, "mode"),
            readString(payloadMap, "refField"),
            readLong(payloadMap, "maxBytes", 0L),
            readCharset(payloadMap, "charset", StandardCharsets.UTF_8));
    }

    private PipelineTemplateMaterialization readMaterialization(Map<?, ?> rootMap) {
        Object materializationObj = rootMap.get("materialization");
        if (materializationObj == null) {
            return new PipelineTemplateMaterialization(List.of());
        }
        if (!(materializationObj instanceof Map<?, ?> materializationMap)) {
            throw new IllegalArgumentException("materialization must be defined as a YAML map");
        }
        Object aspectsObj = materializationMap.get("aspects");
        if (aspectsObj == null) {
            return new PipelineTemplateMaterialization(List.of());
        }
        if (!(aspectsObj instanceof Iterable<?> aspectItems)) {
            throw new IllegalArgumentException("materialization.aspects must be declared as a YAML list");
        }
        List<PipelineTemplateMaterializationAspect> aspects = new ArrayList<>();
        for (Object aspectObj : aspectItems) {
            if (!(aspectObj instanceof Map<?, ?> aspectMap)) {
                throw new IllegalArgumentException("materialization aspect must be declared as a YAML map");
            }
            String position = readString(aspectMap, "position");
            String scope = readString(aspectMap, "scope");
            String action = readString(aspectMap, "action");
            aspects.add(new PipelineTemplateMaterializationAspect(
                readRequiredString(aspectMap, "name", "materialization.aspect"),
                readBoolean(aspectMap, "enabled", true),
                MaterializationScope.from(scope == null ? "GLOBAL" : scope),
                MaterializationPosition.from(position == null ? "AFTER_STEP" : position),
                readInt(aspectMap, "order", 0),
                MaterializationAction.from(action),
                readRequiredString(aspectMap, "message", "materialization.aspect"),
                readMaterializationStringList(aspectMap, "fields"),
                readMaterializationStringList(aspectMap, "targetSteps")));
        }
        return new PipelineTemplateMaterialization(aspects);
    }

    private List<String> readMaterializationStringList(Map<?, ?> map, String key) {
        Object value = map.get(key);
        if (value == null) {
            return List.of();
        }
        if (!(value instanceof Iterable<?> values)) {
            throw new IllegalArgumentException("materialization.aspect." + key + " must be declared as a YAML list");
        }
        List<String> items = new ArrayList<>();
        for (Object element : values) {
            String text = stringify(element);
            if (text == null) {
                throw new IllegalArgumentException("materialization.aspect." + key + " must not contain blank entries");
            }
            items.add(text);
        }
        return items;
    }

    private void validateMaterialization(
        PipelineTemplateMaterialization materialization,
        Map<String, PipelineTemplateMessage> messages,
        List<PipelineTemplateStep> steps
    ) {
        if (materialization == null || materialization.aspects().isEmpty()) {
            return;
        }
        Set<String> stepNames = new LinkedHashSet<>();
        for (PipelineTemplateStep step : steps) {
            if (step != null && step.name() != null) {
                stepNames.add(step.name());
            }
        }
        for (PipelineTemplateMaterializationAspect aspect : materialization.aspects()) {
            validateMaterializationAspect(aspect, messages, stepNames);
        }
    }

    private void validateMaterializationAspect(
        PipelineTemplateMaterializationAspect aspect,
        Map<String, PipelineTemplateMessage> messages,
        Set<String> stepNames
    ) {
        if (aspect.name() == null) {
            throw new IllegalStateException("materialization aspect name must not be blank");
        }
        PipelineTemplateMessage message = messages.get(aspect.message());
        if (message == null) {
            throw new IllegalStateException(
                "Materialization aspect '" + aspect.name() + "' references unknown message '" + aspect.message() + "'");
        }
        if (aspect.fields().isEmpty()) {
            throw new IllegalStateException("Materialization aspect '" + aspect.name() + "' must declare at least one field");
        }
        if (aspect.scope() == MaterializationScope.STEPS && aspect.targetSteps().isEmpty()) {
            throw new IllegalStateException(
                "Materialization aspect '" + aspect.name() + "' with scope STEPS must declare targetSteps");
        }
        for (String targetStep : aspect.targetSteps()) {
            if (!stepNames.contains(targetStep)) {
                throw new IllegalStateException(
                    "Materialization aspect '" + aspect.name() + "' targets unknown step '" + targetStep + "'");
            }
        }
        Map<String, PipelineTemplateField> fieldsByName = new LinkedHashMap<>();
        for (PipelineTemplateField field : message.fields()) {
            fieldsByName.put(field.name(), field);
        }
        for (String fieldName : aspect.fields()) {
            PipelineTemplateField field = fieldsByName.get(fieldName);
            if (field == null) {
                throw new IllegalStateException(
                    "Materialization aspect '" + aspect.name() + "' references unknown field '" + fieldName
                        + "' on message '" + message.name() + "'");
            }
            validateReferenceableField(aspect.name(), message.name(), field, fieldsByName);
        }
    }

    private void validateReferenceableField(
        String aspectName,
        String messageName,
        PipelineTemplateField field,
        Map<String, PipelineTemplateField> fieldsByName
    ) {
        PipelineTemplateFieldReference reference = field.referenceable();
        if (reference == null || reference.refField() == null) {
            throw new IllegalStateException(
                "Materialization aspect '" + aspectName + "' field '" + field.name()
                    + "' on message '" + messageName + "' is not referenceable");
        }
        if (field.repeated() || field.isMap()) {
            throw new IllegalStateException(
                "Materialization aspect '" + aspectName + "' field '" + field.name()
                    + "' on message '" + messageName + "' cannot be repeated or map in v1");
        }
        if (!"string".equals(field.canonicalType()) && !"bytes".equals(field.canonicalType())) {
            throw new IllegalStateException(
                "Materialization aspect '" + aspectName + "' field '" + field.name()
                    + "' on message '" + messageName + "' must be string or bytes in v1");
        }
        PipelineTemplateField refField = fieldsByName.get(reference.refField());
        if (refField == null) {
            throw new IllegalStateException(
                "Referenceable field '" + field.name() + "' on message '" + messageName
                    + "' points to missing refField '" + reference.refField() + "'");
        }
        if (!"payload_ref".equals(refField.canonicalType())) {
            throw new IllegalStateException(
                "Referenceable field '" + field.name() + "' on message '" + messageName
                    + "' must point to a payload_ref field, got '" + refField.type() + "'");
        }
    }

    private String readLogicalContract(Map<?, ?> rootMap, String key, int version) {
        Object contracts = rootMap.get("contract");
        if (contracts == null) {
            return null;
        }
        if (version < 2) {
            throw new IllegalStateException("Pipeline logical contracts require version: 2");
        }
        if (!(contracts instanceof Map<?, ?> contractMap)) {
            throw new IllegalArgumentException("pipeline contract must be defined as a YAML map");
        }
        Object value = contractMap.get(key);
        if (value == null) {
            return null;
        }
        if (!(value instanceof String contract)) {
            throw new IllegalArgumentException("contract." + key + " must be declared as a named type");
        }
        return contract.isBlank() ? null : contract.trim();
    }

    private PipelineInputBoundaryConfig readInputBoundary(Map<?, ?> rootMap) {
        Object inputObj = rootMap.get("input");
        if (inputObj == null) {
            return null;
        }
        if (!(inputObj instanceof Map<?, ?> inputMap)) {
            throw new IllegalArgumentException(
                "pipeline input boundary must be defined as a YAML map; declare a logical input as contract.input");
        }
        Object subscriptionObj = inputMap.get("subscription");
        Object objectObj = inputMap.get("object");
        boolean hasInlineObject = inputMap.get("from") != null || inputMap.get("emits") != null;
        if (subscriptionObj != null && (objectObj != null || hasInlineObject)) {
            throw new IllegalArgumentException("pipeline input boundary cannot declare both subscription and object");
        }
        if (objectObj != null && hasInlineObject) {
            throw new IllegalArgumentException("pipeline input boundary cannot mix input.object with inline from/emits");
        }
        if (subscriptionObj != null) {
            if (!(subscriptionObj instanceof Map<?, ?> subscriptionMap)) {
                throw new IllegalArgumentException("input.subscription must be declared as a YAML map");
            }
            return new PipelineInputBoundaryConfig(new PipelineSubscriptionConfig(
                readRequiredString(subscriptionMap, "publication", "input.subscription"),
                readString(subscriptionMap, "mapper")));
        }
        if (objectObj != null || hasInlineObject) {
            Map<?, ?> objectMap;
            if (objectObj == null) {
                objectMap = inputMap;
            } else if (objectObj instanceof Map<?, ?> map) {
                objectMap = map;
            } else {
                throw new IllegalArgumentException("input.object must be declared as a YAML map");
            }
            return new PipelineInputBoundaryConfig(null, readObjectInput(objectMap));
        }
        return null;
    }

    private PipelineObjectInputConfig readObjectInput(Map<?, ?> inputMap) {
        Object emitsObj = inputMap.get("emits");
        if (!(emitsObj instanceof Map<?, ?> emitsMap)) {
            throw new IllegalArgumentException("input.object.emits must be declared as a YAML map");
        }
        String source = readString(inputMap, "source");
        String from = readString(inputMap, "from");
        if (source != null && from != null) {
            throw new IllegalArgumentException("input.object must declare only one of source or from");
        }
        String resolvedSource = firstNonBlank(source, from);
        if (resolvedSource == null || resolvedSource.isBlank()) {
            throw new IllegalArgumentException("input.object must declare source or from");
        }
        return new PipelineObjectInputConfig(
            resolvedSource,
            readRequiredString(emitsMap, "type", "input.object.emits"),
            readString(emitsMap, "typeName"),
            readRequiredString(emitsMap, "mapper", "input.object.emits"));
    }

    private void validateObjectInputSource(
        PipelineInputBoundaryConfig input,
        Map<String, PipelineObjectSourceConfig> sources
    ) {
        if (input != null && input.object() != null && !sources.containsKey(input.object().source())) {
            throw new IllegalArgumentException("input.object source not found: " + input.object().source());
        }
    }

    private Optional<PipelineOutputBoundaryConfig> readOutputBoundary(Map<?, ?> rootMap) {
        Object outputObj = rootMap.get("output");
        if (outputObj == null) {
            return Optional.empty();
        }
        if (!(outputObj instanceof Map<?, ?> outputMap)) {
            throw new IllegalArgumentException(
                "pipeline output boundary must be defined as a YAML map; declare a logical output as contract.output");
        }
        Object checkpointObj = outputMap.get("checkpoint");
        Object objectObj = outputMap.get("object");
        boolean hasInlineObject = outputMap.get("to") != null || outputMap.get("consumes") != null;
        if (checkpointObj != null && (objectObj != null || hasInlineObject)) {
            throw new IllegalArgumentException("pipeline output boundary cannot declare both checkpoint and object");
        }
        if (objectObj != null && hasInlineObject) {
            throw new IllegalArgumentException("pipeline output boundary cannot mix output.object with inline to/consumes");
        }
        if (checkpointObj != null) {
            if (!(checkpointObj instanceof Map<?, ?> checkpointMap)) {
                throw new IllegalArgumentException("output.checkpoint must be declared as a YAML map");
            }
            Object idempotencyKeyFields = checkpointMap.get("idempotencyKeyFields");
            if (idempotencyKeyFields != null && !(idempotencyKeyFields instanceof Iterable<?>)) {
                throw new IllegalArgumentException("output.checkpoint.idempotencyKeyFields must be declared as a YAML list");
            }
            return Optional.of(new PipelineOutputBoundaryConfig(new PipelineCheckpointConfig(
                readRequiredString(checkpointMap, "publication", "output.checkpoint"),
                readStringList(checkpointMap, "idempotencyKeyFields"))));
        }
        if (objectObj != null || hasInlineObject) {
            Map<?, ?> objectMap;
            if (objectObj == null) {
                objectMap = outputMap;
            } else if (objectObj instanceof Map<?, ?> map) {
                objectMap = map;
            } else {
                throw new IllegalArgumentException("output.object must be declared as a YAML map");
            }
            return Optional.of(new PipelineOutputBoundaryConfig(null, readObjectOutput(objectMap)));
        }
        return Optional.empty();
    }

    private PipelineObjectOutputConfig readObjectOutput(Map<?, ?> outputMap) {
        Object consumesObj = outputMap.get("consumes");
        if (!(consumesObj instanceof Map<?, ?> consumesMap)) {
            throw new IllegalArgumentException("output.object.consumes must be declared as a YAML map");
        }
        String target = readString(outputMap, "target");
        String to = readString(outputMap, "to");
        if (target != null && to != null) {
            throw new IllegalArgumentException("output.object must declare only one of target or to");
        }
        String resolvedTarget = firstNonBlank(target, to);
        if (resolvedTarget == null || resolvedTarget.isBlank()) {
            throw new IllegalArgumentException("output.object must declare target or to");
        }
        return new PipelineObjectOutputConfig(
            resolvedTarget,
            readRequiredString(consumesMap, "type", "output.object.consumes"),
            readString(consumesMap, "typeName"),
            readRequiredString(consumesMap, "mapper", "output.object.consumes"));
    }

    private void validateObjectOutputTarget(
        PipelineOutputBoundaryConfig output,
        Map<String, PipelineObjectPublishConfig> publish
    ) {
        if (output != null && output.object() != null && !publish.containsKey(output.object().target())) {
            throw new IllegalArgumentException("output.object publish target not found: " + output.object().target());
        }
    }

    private void rejectLegacyConnectors(Map<?, ?> rootMap) {
        if (rootMap.get("connectors") != null) {
            throw new IllegalArgumentException(
                "Top-level connectors are no longer supported; use input.subscription and output.checkpoint");
        }
    }

    private String readRequiredString(Map<?, ?> map, String key, String context) {
        String value = readString(map, key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(context + "." + key + " must not be blank");
        }
        return value.trim();
    }

    /**
     * Convert an arbitrary object into a Map<String, Object> by copying entries when the object is a Map; otherwise produce an empty map.
     *
     * @param configObj an object expected to be a Map; may be null or a non-map value
     * @return a map whose keys are the stringified non-null keys from the input map and whose values are the original values; returns an empty map if the input is not a map
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> readConfigMap(Object configObj) {
        if (!(configObj instanceof Map<?, ?> configMap)) {
            return Map.of();
        }
        Map<String, Object> values = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : configMap.entrySet()) {
            if (entry.getKey() != null) {
                values.put(entry.getKey().toString(), normalizeConfigValue(entry.getValue()));
            }
        }
        return Map.copyOf(values);
    }

    private Object normalizeConfigValue(Object value) {
        return normalizeConfigValue(value, 0);
    }

    private Object normalizeConfigValue(Object value, int depth) {
        if (depth >= MAX_NESTING_DEPTH) {
            throw new IllegalArgumentException(
                "pipeline template nested configuration exceeds maximum depth of " + MAX_NESTING_DEPTH);
        }
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> normalized = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (entry.getKey() != null) {
                    normalized.put(entry.getKey().toString(), normalizeConfigValue(entry.getValue(), depth + 1));
                }
            }
            return Map.copyOf(normalized);
        }
        if (value instanceof Iterable<?> iterable) {
            List<Object> normalized = new ArrayList<>();
            for (Object item : iterable) {
                normalized.add(normalizeConfigValue(item, depth + 1));
            }
            return List.copyOf(normalized);
        }
        return value;
    }

    private Map<String, Object> readObjectMap(Map<?, ?> map, String key) {
        Object value = map.get(key);
        if (value == null) {
            return Map.of();
        }
        if (!(value instanceof Map<?, ?> rawMap)) {
            throw new IllegalArgumentException(key + " must be declared as a YAML map");
        }
        return readConfigMap(rawMap);
    }

    /**
     * Get the value for the given key from the map as a trimmed string.
     *
     * @param map the source map to read from
     * @param key the key whose value should be stringified
     * @return the trimmed string value, or `null` if the value is null or blank
     */
    private String readString(Map<?, ?> map, String key) {
        Object value = map.get(key);
        return stringify(value);
    }

    /**
     * Convert an arbitrary value to a trimmed string, returning null for null or blank input.
     *
     * @param value the object to stringify
     * @return `null` if {@code value} is null or its string form is blank, otherwise the trimmed string
     */
    private String stringify(Object value) {
        if (value == null) {
            return null;
        }
        String text = value.toString();
        return text == null || text.isBlank() ? null : text.trim();
    }

    private String firstNonBlank(String primary, String fallback) {
        return primary == null || primary.isBlank() ? fallback : primary;
    }

    /**
     * Retrieve an integer value from the given map by key, returning a default when the value is absent or null.
     *
     * @param map the map to read the value from
     * @param key the key whose associated value should be returned
     * @param defaultValue the value to return if the map contains no mapping for the key or the mapping is null
     * @return the integer value associated with the key, or {@code defaultValue} if none is present
     */
    private int readInt(Map<?, ?> map, String key, int defaultValue) {
        Integer value = readIntegerObject(map, key);
        return value == null ? defaultValue : value;
    }

    private long readLong(Map<?, ?> map, String key, long defaultValue) {
        Object value = map.get(key);
        if (value == null || value.toString().isBlank()) {
            return defaultValue;
        }
        if (value instanceof Number number) {
            return strictLongValue(number, key);
        }
        try {
            return Long.parseLong(value.toString().trim());
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(
                "Invalid long value '" + value + "' for key '" + key + "'", ex);
        }
    }

    private long strictLongValue(Number number, String context) {
        if (number instanceof Byte || number instanceof Short || number instanceof Integer || number instanceof Long) {
            return number.longValue();
        }
        if (number instanceof BigInteger bigInteger) {
            if (bigInteger.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) < 0
                || bigInteger.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0) {
                throw new IllegalArgumentException("Invalid long value '" + number + "' for key '" + context + "'");
            }
            return bigInteger.longValueExact();
        }
        try {
            BigDecimal decimal = new BigDecimal(number.toString());
            if (decimal.stripTrailingZeros().scale() > 0) {
                throw new IllegalArgumentException(
                    "Invalid long value '" + number + "' for " + context + ": fractional values are not allowed");
            }
            return decimal.longValueExact();
        } catch (ArithmeticException | NumberFormatException ex) {
            throw new IllegalArgumentException(
                "Invalid long value '" + number + "' for " + context, ex);
        }
    }

    private Duration readDuration(Map<?, ?> map, String key, Duration defaultValue) {
        Object value = map.get(key);
        if (value == null || value.toString().isBlank()) {
            return defaultValue;
        }
        try {
            return Duration.parse(value.toString().trim());
        } catch (RuntimeException ex) {
            throw new IllegalArgumentException(
                "Invalid duration value '" + value + "' for key '" + key + "'", ex);
        }
    }

    private Charset readCharset(Map<?, ?> map, String key, Charset defaultValue) {
        Object value = map.get(key);
        if (value == null || value.toString().isBlank()) {
            return defaultValue;
        }
        try {
            return Charset.forName(value.toString().trim());
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Invalid charset value '" + value + "' for key '" + key + "'", e);
        }
    }

    /**
     * Retrieves an Integer from a map value for the given key, accepting numeric objects or parseable strings.
     *
     * @param map the map containing the value
     * @param key the key whose associated value should be parsed as an Integer
     * @return the Integer value, or null if the key is absent or the value is blank
     * @throws IllegalArgumentException if the value cannot be parsed as an integer
     */
    private Integer readIntegerObject(Map<?, ?> map, String key) {
        Object value = map.get(key);
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return strictIntegerValue(number, key);
        }
        String text = value.toString();
        if (text == null || text.isBlank()) {
            return null;
        }
        try {
            return Integer.parseInt(text.trim());
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(
                "Invalid integer value '" + text + "' for key '" + key + "'", ex);
        }
    }

    private int strictIntegerValue(Number number, String context) {
        try {
            BigDecimal decimal = new BigDecimal(number.toString());
            if (decimal.stripTrailingZeros().scale() > 0) {
                throw new IllegalStateException(
                    "Invalid integer value '" + number + "' for " + context + ": fractional values are not allowed");
            }
            return decimal.intValueExact();
        } catch (ArithmeticException | NumberFormatException ex) {
            throw new IllegalStateException(
                "Invalid integer value '" + number + "' for " + context, ex);
        }
    }

    /**
     * Builds a compact string summary of the given fields as "name:type" pairs.
     *
     * @param fields list of fields to summarize; may be empty
     * @return a string in the form "[name:type, ...]" listing each field's name and type
     */
    private String summarizeFields(List<PipelineTemplateField> fields) {
        List<String> summary = new ArrayList<>();
        for (PipelineTemplateField field : fields) {
            summary.add(field.name() + ":" + field.type());
        }
        return summary.toString();
    }

    /**
     * Retrieve a boolean value from the given map, falling back to the supplied default when absent.
     *
     * @param map the map to read the value from; may contain a Boolean or a value convertible to a string
     * @param key the key whose value should be interpreted as a boolean
     * @param defaultValue the value to return when the map does not contain the key
     * @return `true` if the resolved value is true, `false` otherwise (or the provided default when the key is missing)
     */
    private boolean readBoolean(Map<?, ?> map, String key, boolean defaultValue) {
        Object value = map.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Boolean flag) {
            return flag;
        }
        return Boolean.parseBoolean(value.toString());
    }

    /**
     * Read a list of strings from the map entry at the given key by converting each iterable element to a trimmed string and omitting null or blank results.
     *
     * @param map the source map to read from; may contain any object at {@code key}
     * @param key the key whose associated value is expected to be an iterable of elements to convert
     * @return a list of trimmed, non-null strings produced from the iterable at {@code key}, or an empty list if the entry is missing or not iterable
     */
    private List<String> readStringList(Map<?, ?> map, String key) {
        Object value = map.get(key);
        if (!(value instanceof Iterable<?> values)) {
            return List.of();
        }
        List<String> items = new ArrayList<>();
        for (Object element : values) {
            String text = stringify(element);
            if (text != null) {
                items.add(text);
            }
        }
        return items;
    }

    /**
     * Retrieve the configured transport override, if any.
     *
     * @return the transport override string, or {@code null} if no override is configured
     */
    private String resolveTransportOverride() {
        return TransportOverrideResolver.resolveOverride(propertyLookup, envLookup);
    }

    /**
     * Retrieves a platform override using the configured property and environment lookups.
     *
     * @return the platform override string if present, or null if no override is configured
     */
    private String resolvePlatformOverride() {
        return PlatformOverrideResolver.resolveOverride(propertyLookup, envLookup);
    }
}
