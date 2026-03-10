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
import org.yaml.snakeyaml.Yaml;

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
     * Creates a loader with custom property/environment lookup functions.
     *
     * @param propertyLookup lookup used for system properties
     * @param envLookup lookup used for environment variables
     */
    public PipelineTemplateConfigLoader(Function<String, String> propertyLookup, Function<String, String> envLookup) {
        this.propertyLookup = propertyLookup == null ? key -> null : propertyLookup;
        this.envLookup = envLookup == null ? key -> null : envLookup;
    }

    /**
     * Load and parse the pipeline template configuration from the specified file path.
     *
     * @param configPath the path to the pipeline template YAML file
     * @return a PipelineTemplateConfig built from the file's contents
     * @throws IllegalStateException if the YAML root is not a map or the file cannot be read/parsed
     */
    public PipelineTemplateConfig load(Path configPath) {
        Object root = loadYaml(configPath);
        if (!(root instanceof Map<?, ?> rootMap)) {
            throw new IllegalStateException("Pipeline template config root is not a map");
        }

        int version = readInt(rootMap, "version", 1);
        String appName = readString(rootMap, "appName");
        String basePackage = readString(rootMap, "basePackage");
        String transport = normalizeTransport(readString(rootMap, "transport"));
        PipelinePlatform resolvedPlatform = normalizePlatform(readString(rootMap, "platform"));

        Map<String, PipelineTemplateMessage> rawMessages = version >= 2
            ? readMessages(rootMap)
            : new LinkedHashMap<>();
        List<PipelineTemplateStep> steps = readSteps(rootMap, version);
        Map<String, PipelineTemplateAspect> aspects = readAspects(rootMap);

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
                aspects);
        }

        return new PipelineTemplateConfig(version, appName, basePackage, transport, resolvedPlatform, Map.of(), steps, aspects);
    }

    private Object loadYaml(Path configPath) {
        Yaml yaml = new Yaml();
        try (Reader reader = Files.newBufferedReader(configPath)) {
            return yaml.load(reader);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read pipeline template config: " + configPath, e);
        }
    }

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
                continue;
            }
            List<PipelineTemplateField> fields = readFields(messageMap.get("fields"), 2);
            PipelineTemplateReserved reserved = readReserved(messageMap.get("reserved"));
            messages.put(name, new PipelineTemplateMessage(name, fields, reserved));
        }
        return messages;
    }

    private void collectInlineMessages(Map<String, PipelineTemplateMessage> messages, List<PipelineTemplateStep> steps) {
        for (PipelineTemplateStep step : steps) {
            collectInlineMessage(messages, step.inputTypeName(), step.inputFields());
            collectInlineMessage(messages, step.outputTypeName(), step.outputFields());
        }
    }

    private void collectInlineMessage(
        Map<String, PipelineTemplateMessage> messages,
        String typeName,
        List<PipelineTemplateField> inlineFields
    ) {
        if (typeName == null || typeName.isBlank() || inlineFields == null || inlineFields.isEmpty()) {
            return;
        }
        messages.putIfAbsent(typeName, new PipelineTemplateMessage(typeName, inlineFields, new PipelineTemplateReserved(List.of(), List.of())));
    }

    private Map<String, PipelineTemplateMessage> normalizeMessages(Map<String, PipelineTemplateMessage> rawMessages) {
        Set<String> knownNames = new LinkedHashSet<>(rawMessages.keySet());
        Map<String, PipelineTemplateMessage> normalized = new LinkedHashMap<>();
        for (Map.Entry<String, PipelineTemplateMessage> entry : rawMessages.entrySet()) {
            String name = entry.getKey();
            PipelineTemplateMessage rawMessage = entry.getValue();
            List<PipelineTemplateField> normalizedFields = new ArrayList<>();
            for (PipelineTemplateField field : rawMessage.fields()) {
                normalizedFields.add(PipelineTemplateTypeMappings.normalizeV2Field(field, List.copyOf(knownNames)));
            }
            PipelineTemplateReserved reserved = rawMessage.reserved();
            validateReserved(name, normalizedFields, reserved);
            normalized.put(name, new PipelineTemplateMessage(name, normalizedFields, reserved));
        }
        return normalized;
    }

    private List<PipelineTemplateStep> resolveV2Steps(
        List<PipelineTemplateStep> steps,
        Map<String, PipelineTemplateMessage> messages
    ) {
        List<PipelineTemplateStep> resolved = new ArrayList<>();
        for (PipelineTemplateStep step : steps) {
            List<PipelineTemplateField> inputFields = resolveStepFields(step.inputTypeName(), step.inputFields(), messages, step.name(), "input");
            List<PipelineTemplateField> outputFields = resolveStepFields(step.outputTypeName(), step.outputFields(), messages, step.name(), "output");
            resolved.add(new PipelineTemplateStep(
                step.name(),
                step.cardinality(),
                step.inputTypeName(),
                inputFields,
                step.outputTypeName(),
                outputFields));
        }
        return resolved;
    }

    private List<PipelineTemplateField> resolveStepFields(
        String typeName,
        List<PipelineTemplateField> inlineFields,
        Map<String, PipelineTemplateMessage> messages,
        String stepName,
        String direction
    ) {
        if (typeName == null || typeName.isBlank()) {
            return inlineFields == null ? List.of() : List.copyOf(inlineFields);
        }
        PipelineTemplateMessage message = messages.get(typeName);
        if (message == null) {
            throw new IllegalStateException("Step '" + stepName + "' references unknown " + direction + " message '" + typeName + "'");
        }
        if (inlineFields != null && !inlineFields.isEmpty()) {
            List<PipelineTemplateField> normalizedInline = new ArrayList<>();
            List<String> knownMessages = List.copyOf(messages.keySet());
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
            stepInfos.add(new PipelineTemplateStep(
                name,
                cardinality,
                inputType,
                inputFields,
                outputType,
                outputFields));
        }
        return stepInfos;
    }

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

    private PipelineTemplateField readLegacyField(Map<?, ?> fieldMap) {
        String name = readString(fieldMap, "name");
        String type = readString(fieldMap, "type");
        String protoType = readString(fieldMap, "protoType");
        return PipelineTemplateTypeMappings.normalizeLegacyField(new PipelineTemplateField(name, type, protoType));
    }

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

    private PipelineTemplateReserved readReserved(Object reservedObj) {
        if (!(reservedObj instanceof Map<?, ?> reservedMap)) {
            return new PipelineTemplateReserved(List.of(), List.of());
        }
        List<Integer> numbers = new ArrayList<>();
        Object numbersObj = reservedMap.get("numbers");
        if (numbersObj instanceof Iterable<?> numberList) {
            for (Object numberObj : numberList) {
                if (numberObj instanceof Number number) {
                    numbers.add(number.intValue());
                } else if (numberObj != null) {
                    numbers.add(Integer.parseInt(numberObj.toString().trim()));
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

    private void validateReserved(
        String messageName,
        List<PipelineTemplateField> fields,
        PipelineTemplateReserved reserved
    ) {
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

    private String readString(Map<?, ?> map, String key) {
        Object value = map.get(key);
        return stringify(value);
    }

    private String stringify(Object value) {
        if (value == null) {
            return null;
        }
        String text = value.toString();
        return text == null || text.isBlank() ? null : text.trim();
    }

    private int readInt(Map<?, ?> map, String key, int defaultValue) {
        Integer value = readIntegerObject(map, key);
        return value == null ? defaultValue : value;
    }

    private Integer readIntegerObject(Map<?, ?> map, String key) {
        Object value = map.get(key);
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        String text = value.toString();
        if (text == null || text.isBlank()) {
            return null;
        }
        return Integer.parseInt(text.trim());
    }

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

    private String resolveTransportOverride() {
        return TransportOverrideResolver.resolveOverride(propertyLookup, envLookup);
    }

    private String resolvePlatformOverride() {
        return PlatformOverrideResolver.resolveOverride(propertyLookup, envLookup);
    }
}
