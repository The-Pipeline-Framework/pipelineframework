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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.logging.Logger;

import org.pipelineframework.config.PlatformOverrideResolver;
import org.pipelineframework.config.TransportOverrideResolver;
import org.pipelineframework.config.connector.ConnectorBrokerConfig;
import org.pipelineframework.config.connector.ConnectorConfig;
import org.pipelineframework.config.connector.ConnectorSourceConfig;
import org.pipelineframework.config.connector.ConnectorTargetConfig;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.constructor.SafeConstructor;

/**
 * Loads the pipeline template configuration used by the template generator.
 */
public class PipelineTemplateConfigLoader {
    private static final Logger LOG = Logger.getLogger(PipelineTemplateConfigLoader.class.getName());
    private static final String DEFAULT_TRANSPORT = "GRPC";
    private static final PipelinePlatform DEFAULT_PLATFORM = PipelinePlatform.COMPUTE;
    private final Function<String, String> propertyLookup;
    private final Function<String, String> envLookup;

    /**
     * Creates a new PipelineTemplateConfigLoader.
     */
    public PipelineTemplateConfigLoader() {
        this(System::getProperty, System::getenv);
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
        this.propertyLookup = propertyLookup == null ? key -> null : propertyLookup;
        this.envLookup = envLookup == null ? key -> null : envLookup;
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
        Object root = loadYaml(configPath);
        if (!(root instanceof Map<?, ?> rootMap)) {
            throw new IllegalStateException("Pipeline template config root is not a map");
        }

        int version = readVersion(rootMap);
        String appName = readString(rootMap, "appName");
        String basePackage = readString(rootMap, "basePackage");
        String transport = normalizeTransport(readString(rootMap, "transport"));
        PipelinePlatform resolvedPlatform = normalizePlatform(readString(rootMap, "platform"));

        Map<String, PipelineTemplateMessage> rawMessages = version >= 2
            ? readMessages(rootMap)
            : new LinkedHashMap<>();
        List<PipelineTemplateStep> steps = readSteps(rootMap, version);
        Map<String, PipelineTemplateAspect> aspects = readAspects(rootMap);
        List<ConnectorConfig> connectors = readConnectors(rootMap);

        if (version >= 2) {
            collectInlineMessages(rawMessages, steps);
            Map<String, PipelineTemplateMessage> normalizedMessages = normalizeMessages(rawMessages);
            steps = resolveV2Steps(steps, normalizedMessages);
            return new PipelineTemplateConfig(
                version,
                appName,
                basePackage,
                transport,
                resolvedPlatform,
                normalizedMessages,
                steps,
                aspects,
                connectors);
        }

        return new PipelineTemplateConfig(
            version,
            appName,
            basePackage,
            transport,
            resolvedPlatform,
            Map.of(),
            steps,
            aspects,
            connectors);
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
    private Map<String, PipelineTemplateMessage> readMessages(Map<?, ?> rootMap) {
        Object messagesObj = rootMap.get("messages");
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
            if (!(entry.getValue() instanceof Map<?, ?> messageMap)) {
                LOG.warning("Skipping malformed message entry: key=" + name
                    + " valueType=" + (entry.getValue() == null ? "null" : entry.getValue().getClass().getName()));
                continue;
            }
            List<PipelineTemplateField> fields = readFields(messageMap.get("fields"), 2);
            PipelineTemplateReserved reserved = readReserved(messageMap.get("reserved"));
            messages.put(name, new PipelineTemplateMessage(name, fields, reserved));
        }
        return messages;
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
            normalized.put(name, new PipelineTemplateMessage(name, normalizedFields, reserved));
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
        Map<String, PipelineTemplateMessage> messages
    ) {
        List<PipelineTemplateStep> resolved = new ArrayList<>();
        for (PipelineTemplateStep step : steps) {
            List<PipelineTemplateField> inputFields = resolveStepFields(step.inputTypeName(), step.inputFields(), messages, step.name(), "input");
            List<PipelineTemplateField> outputFields = resolveStepFields(step.outputTypeName(), step.outputFields(), messages, step.name(), "output");
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
                step.outputTypeName(),
                outputFields,
                step.execution()));
        }
        return resolved;
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
        PipelineTemplateMessage message = messages.get(typeName);
        if (message == null) {
            throw new IllegalStateException("Step '" + stepName + "' references unknown " + direction + " message '" + typeName + "'");
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
            String cardinality = readString(stepMap, "cardinality");
            String inputType = readString(stepMap, "inputTypeName");
            String outputType = readString(stepMap, "outputTypeName");
            List<PipelineTemplateField> inputFields = readFields(stepMap.get("inputFields"), version);
            List<PipelineTemplateField> outputFields = readFields(stepMap.get("outputFields"), version);
            PipelineTemplateStepExecution execution = readExecution(stepMap.get("execution"), version, name);
            stepInfos.add(new PipelineTemplateStep(
                name,
                cardinality,
                inputType,
                inputFields,
                outputType,
                outputFields,
                execution));
        }
        return stepInfos;
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
        if (!"PROTOBUF_HTTP_V1".equalsIgnoreCase(execution.protocol())) {
            throw new IllegalStateException(
                "Step '" + stepName + "' remote execution requires execution.protocol=PROTOBUF_HTTP_V1");
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
        if (!(fieldsObj instanceof Iterable<?> fields)) {
            return List.of();
        }

        List<PipelineTemplateField> fieldInfos = new ArrayList<>();
        for (Object fieldObj : fields) {
            if (!(fieldObj instanceof Map<?, ?> fieldMap)) {
                continue;
            }
            if (version >= 2) {
                fieldInfos.add(readV2Field(fieldMap));
            } else {
                fieldInfos.add(readLegacyField(fieldMap));
            }
        }
        return fieldInfos;
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
        boolean optional = readBoolean(fieldMap, "optional", false);
        boolean repeated = readBoolean(fieldMap, "repeated", false);
        boolean deprecated = readBoolean(fieldMap, "deprecated", false);
        String keyType = readString(fieldMap, "keyType");
        String valueType = readString(fieldMap, "valueType");
        String since = readString(fieldMap, "since");
        String deprecatedSince = readString(fieldMap, "deprecatedSince");
        String comment = readString(fieldMap, "comment");
        PipelineTemplateFieldOverrides overrides = readOverrides(fieldMap.get("overrides"));
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
            optional,
            repeated,
            deprecated,
            since,
            deprecatedSince,
            comment,
            overrides);
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

    /**
     * Parses the optional top-level "connectors" section and constructs a list of ConnectorConfig objects.
     *
     * <p>Malformed entries (non-map) and entries with missing or duplicate names are skipped. If the
     * "connectors" key is absent or not iterable, an empty list is returned.
     *
     * @param rootMap the parsed YAML root map to read the "connectors" section from
     * @return a list of ConnectorConfig for each valid connector definition; empty list if none found
     */
    private List<ConnectorConfig> readConnectors(Map<?, ?> rootMap) {
        Object connectorsObj = rootMap.get("connectors");
        if (!(connectorsObj instanceof Iterable<?> connectors)) {
            if (connectorsObj == null) {
                return List.of();
            }
            throw new IllegalArgumentException("connectors must be declared as a YAML list");
        }

        List<ConnectorConfig> values = new ArrayList<>();
        Set<String> seenNames = new LinkedHashSet<>();
        for (Object connectorObj : connectors) {
            if (!(connectorObj instanceof Map<?, ?> connectorMap)) {
                throw new IllegalArgumentException(
                    "connectors entries must be maps; found: " + connectorObj);
            }
            String name = readString(connectorMap, "name");
            if (name == null || name.isBlank()) {
                throw new IllegalArgumentException("connector entry is missing a non-blank name: " + connectorMap);
            }
            if (!seenNames.add(name)) {
                throw new IllegalArgumentException("duplicate connector declaration '" + name + "'");
            }
            values.add(new ConnectorConfig(
                name,
                readBoolean(connectorMap, "enabled", true),
                readConnectorSource(connectorMap),
                readConnectorTarget(connectorMap),
                readString(connectorMap, "mapper"),
                readString(connectorMap, "transport"),
                readString(connectorMap, "idempotency"),
                readString(connectorMap, "backpressure"),
                readString(connectorMap, "failureMode"),
                readInt(connectorMap, "backpressureBufferCapacity", 256),
                readInt(connectorMap, "idempotencyMaxKeys", 10000),
                readStringList(connectorMap, "idempotencyKeyFields"),
                readConnectorBroker(connectorMap)));
        }
        return values;
    }

    /**
     * Parses the optional "source" block of a connector and returns its ConnectorSourceConfig.
     *
     * @param connectorMap the connector definition map that may contain a "source" entry
     * @return a ConnectorSourceConfig built from the source block's "kind", "step", and "type" keys, or null if the source entry is missing or not a map
     */
    private ConnectorSourceConfig readConnectorSource(Map<?, ?> connectorMap) {
        Object sourceObj = connectorMap.get("source");
        if (!(sourceObj instanceof Map<?, ?> sourceMap)) {
            throw new IllegalArgumentException(
                "ConnectorConfig '" + readString(connectorMap, "name") + "' requires a source section defined as a map");
        }
        return new ConnectorSourceConfig(
            readString(sourceMap, "kind"),
            readString(sourceMap, "step"),
            readString(sourceMap, "type"));
    }

    /**
     * Parse the optional "target" block of a connector configuration into a ConnectorTargetConfig.
     *
     * @param connectorMap the connector configuration map parsed from YAML
     * @return a ConnectorTargetConfig constructed from the "target" block, or `null` if the "target" entry is missing or not a map
     */
    private ConnectorTargetConfig readConnectorTarget(Map<?, ?> connectorMap) {
        Object targetObj = connectorMap.get("target");
        if (!(targetObj instanceof Map<?, ?> targetMap)) {
            throw new IllegalArgumentException(
                "ConnectorConfig '" + readString(connectorMap, "name") + "' requires a target section defined as a map");
        }
        return new ConnectorTargetConfig(
            readString(targetMap, "kind"),
            readString(targetMap, "pipeline"),
            readString(targetMap, "type"),
            readString(targetMap, "adapter"));
    }

    /**
     * Parses the "broker" sub-object from a connector map into a ConnectorBrokerConfig.
     *
     * @param connectorMap the connector configuration map that may contain a "broker" entry
     * @return a ConnectorBrokerConfig built from the broker's provider, destination, and adapter, or {@code null} if the "broker" entry is absent or not a map
     */
    private ConnectorBrokerConfig readConnectorBroker(Map<?, ?> connectorMap) {
        Object brokerObj = connectorMap.get("broker");
        if (!(brokerObj instanceof Map<?, ?> brokerMap)) {
            return null;
        }
        return new ConnectorBrokerConfig(
            readString(brokerMap, "provider"),
            readString(brokerMap, "destination"),
            readString(brokerMap, "adapter"));
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
                values.put(entry.getKey().toString(), entry.getValue());
            }
        }
        return values;
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
