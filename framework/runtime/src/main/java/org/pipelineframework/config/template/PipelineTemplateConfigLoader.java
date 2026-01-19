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

import org.yaml.snakeyaml.Yaml;

/**
 * Loads the pipeline template configuration used by the template generator.
 */
public class PipelineTemplateConfigLoader {

    /**
     * Creates a new PipelineTemplateConfigLoader.
     */
    public PipelineTemplateConfigLoader() {
    }

    /**
     * Load the pipeline template configuration from the given file path.
     *
     * @param configPath the pipeline template config path
     * @return the parsed pipeline template config
     */
    public PipelineTemplateConfig load(Path configPath) {
        Object root = loadYaml(configPath);
        if (!(root instanceof Map<?, ?> rootMap)) {
            throw new IllegalStateException("Pipeline template config root is not a map");
        }

        String appName = readString(rootMap, "appName");
        String basePackage = readString(rootMap, "basePackage");
        String transport = readString(rootMap, "transport");
        List<PipelineTemplateStep> steps = readSteps(rootMap);
        Map<String, PipelineTemplateAspect> aspects = readAspects(rootMap);

        return new PipelineTemplateConfig(appName, basePackage, transport, steps, aspects);
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

    private String readString(Map<?, ?> map, String key) {
        Object value = map.get(key);
        return value == null ? null : value.toString();
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
