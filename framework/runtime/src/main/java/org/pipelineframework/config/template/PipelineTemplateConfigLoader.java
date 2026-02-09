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
import java.util.List;
import java.util.Map;

import org.pipelineframework.config.PlatformOverrideResolver;
import org.pipelineframework.config.TransportOverrideResolver;
import org.yaml.snakeyaml.Yaml;

/**
 * Loads the pipeline template configuration used by the template generator.
 */
public class PipelineTemplateConfigLoader {
    private static final String DEFAULT_TRANSPORT = "GRPC";
    private static final PipelinePlatform DEFAULT_PLATFORM = PipelinePlatform.COMPUTE;

    /**
     * Creates a new PipelineTemplateConfigLoader.
     */
    public PipelineTemplateConfigLoader() {
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

        String appName = readString(rootMap, "appName");
        String basePackage = readString(rootMap, "basePackage");
        String transport = readString(rootMap, "transport");
        transport = transport == null ? null : transport.trim();
        String platform = readString(rootMap, "platform");
        platform = platform == null ? null : platform.trim();
        String transportOverride = resolveTransportOverride();
        if (transportOverride != null && !transportOverride.isBlank()) {
            transport = transportOverride.trim();
        }
        String normalizedTransport = TransportOverrideResolver.normalizeKnownTransport(transport);
        transport = normalizedTransport != null ? normalizedTransport : DEFAULT_TRANSPORT;
        String platformOverride = resolvePlatformOverride();
        if (platformOverride != null && !platformOverride.isBlank()) {
            platform = platformOverride.trim();
        }
        String normalizedPlatform = PlatformOverrideResolver.normalizeKnownPlatform(platform);
        PipelinePlatform resolvedPlatform = PipelinePlatform.fromStringOptional(normalizedPlatform)
            .orElse(DEFAULT_PLATFORM);
        List<PipelineTemplateStep> steps = readSteps(rootMap);
        Map<String, PipelineTemplateAspect> aspects = readAspects(rootMap);

        return new PipelineTemplateConfig(appName, basePackage, transport, resolvedPlatform, steps, aspects);
    }

    private Object loadYaml(Path configPath) {
        Yaml yaml = new Yaml();
        try (Reader reader = Files.newBufferedReader(configPath)) {
            return yaml.load(reader);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read pipeline template config: " + configPath, e);
        }
    }

    private List<PipelineTemplateStep> readSteps(Map<?, ?> rootMap) {
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
            List<PipelineTemplateField> inputFields = readFields(stepMap.get("inputFields"));
            List<PipelineTemplateField> outputFields = readFields(stepMap.get("outputFields"));
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

    private List<PipelineTemplateField> readFields(Object fieldsObj) {
        if (!(fieldsObj instanceof Iterable<?> fields)) {
            return List.of();
        }

        List<PipelineTemplateField> fieldInfos = new ArrayList<>();
        for (Object fieldObj : fields) {
            if (!(fieldObj instanceof Map<?, ?> fieldMap)) {
                continue;
            }
            String name = readString(fieldMap, "name");
            String type = readString(fieldMap, "type");
            String protoType = readString(fieldMap, "protoType");
            fieldInfos.add(new PipelineTemplateField(name, type, protoType));
        }
        return fieldInfos;
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

    /**
     * Retrieve the string representation of the value associated with the given key in the map.
     *
     * @param map the map to read from
     * @param key the key whose value should be returned
     * @return the value's string representation, or {@code null} if the key is absent or maps to {@code null}
     */
    private String readString(Map<?, ?> map, String key) {
        Object value = map.get(key);
        return value == null ? null : value.toString();
    }

    /**
     * Resolve transport override from system property or environment variable.
     *
     * Checks the JVM system property "pipeline.transport" first; if it is null or blank,
     * falls back to the environment variable "PIPELINE_TRANSPORT".
     *
     * @return the resolved transport override value, or {@code null} if neither is set
     */
    private String resolveTransportOverride() {
        return TransportOverrideResolver.resolveOverride(System::getProperty, System::getenv);
    }

    /**
     * Resolve platform override from system property or environment variable.
     *
     * Checks the JVM system property {@code pipeline.platform} first; if it is null or blank,
     * falls back to the environment variable {@code PIPELINE_PLATFORM}.
     *
     * @return the resolved platform override value, or {@code null} if neither is set
     */
    private String resolvePlatformOverride() {
        return PlatformOverrideResolver.resolveOverride(System::getProperty, System::getenv);
    }

    /**
     * Resolve a boolean configuration value from the map, falling back to the provided default.
     *
     * @param map the map containing configuration values
     * @param key the key to look up in the map
     * @param defaultValue the value to return when the map contains no value for the key
     * @return the boolean value from the map for the given key; if the value is a Boolean it is returned directly,
     *         if it is non-null it is parsed with Boolean.parseBoolean, and if the key is absent the defaultValue is returned
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

    private int readInt(Map<?, ?> map, String key, int defaultValue) {
        Object value = map.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        try {
            return Integer.parseInt(value.toString());
        } catch (NumberFormatException ignored) {
            return defaultValue;
        }
    }
}
