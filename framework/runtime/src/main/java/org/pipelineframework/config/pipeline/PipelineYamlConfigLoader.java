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

package org.pipelineframework.config.pipeline;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

/**
 * Loads pipeline.yaml configuration for runtime usage.
 */
public class PipelineYamlConfigLoader {

    /**
     * Creates a new PipelineYamlConfigLoader.
     */
    public PipelineYamlConfigLoader() {
    }

    /**
     * Load pipeline configuration from a file path.
     *
     * @param configPath the pipeline config path
     * @return the parsed pipeline configuration
     */
    public PipelineYamlConfig load(Path configPath) {
        Object root = loadYaml(configPath);
        return parseRoot(root, "pipeline config: " + configPath);
    }

    /**
     * Load pipeline configuration from an input stream.
     *
     * @param inputStream the input stream containing YAML
     * @return the parsed pipeline configuration
     */
    public PipelineYamlConfig load(InputStream inputStream) {
        Object root = loadYaml(inputStream);
        return parseRoot(root, "pipeline config resource");
    }

    /**
     * Load pipeline configuration from a reader.
     *
     * @param reader the reader providing YAML content
     * @return the parsed pipeline configuration
     */
    public PipelineYamlConfig load(Reader reader) {
        Object root = loadYaml(reader);
        return parseRoot(root, "pipeline config reader");
    }

    private Object loadYaml(Path configPath) {
        Yaml yaml = new Yaml();
        try (Reader reader = Files.newBufferedReader(configPath)) {
            return yaml.load(reader);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read pipeline config: " + configPath, e);
        }
    }

    private Object loadYaml(InputStream inputStream) {
        Yaml yaml = new Yaml();
        try (Reader reader = new InputStreamReader(inputStream)) {
            return yaml.load(reader);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read pipeline config from input stream", e);
        }
    }

    private Object loadYaml(Reader reader) {
        Yaml yaml = new Yaml();
        return yaml.load(reader);
    }

    private PipelineYamlConfig parseRoot(Object root, String source) {
        if (!(root instanceof Map<?, ?> rootMap)) {
            throw new IllegalStateException("Pipeline config root is not a map for " + source);
        }

        String basePackage = readString(rootMap, "basePackage");
        String transport = readString(rootMap, "transport");
        List<PipelineYamlStep> steps = readSteps(rootMap);
        List<PipelineYamlAspect> aspects = readAspects(rootMap);

        return new PipelineYamlConfig(basePackage, transport, steps, aspects);
    }

    private List<PipelineYamlStep> readSteps(Map<?, ?> rootMap) {
        Object stepsObj = rootMap.get("steps");
        if (!(stepsObj instanceof Iterable<?> steps)) {
            return List.of();
        }

        List<PipelineYamlStep> stepInfos = new ArrayList<>();
        for (Object stepObj : steps) {
            if (!(stepObj instanceof Map<?, ?> stepMap)) {
                continue;
            }
            String name = readString(stepMap, "name");
            String inputType = readString(stepMap, "inputTypeName");
            String outputType = readString(stepMap, "outputTypeName");
            if (name != null && !name.isBlank() && outputType != null && !outputType.isBlank()) {
                stepInfos.add(new PipelineYamlStep(name, inputType, outputType));
            }
        }
        return stepInfos;
    }

    private List<PipelineYamlAspect> readAspects(Map<?, ?> rootMap) {
        Object aspectsObj = rootMap.get("aspects");
        if (!(aspectsObj instanceof Map<?, ?> aspectsMap)) {
            return List.of();
        }

        List<PipelineYamlAspect> aspects = new ArrayList<>();
        for (Map.Entry<?, ?> entry : aspectsMap.entrySet()) {
            String name = entry.getKey() == null ? "" : entry.getKey().toString();
            if (!(entry.getValue() instanceof Map<?, ?> aspectConfig)) {
                continue;
            }

            boolean enabled = readBoolean(aspectConfig, "enabled", false);
            String position = readString(aspectConfig, "position");
            if (position == null || position.isBlank()) {
                position = "AFTER_STEP";
            }
            String scope = readString(aspectConfig, "scope");
            if (scope == null || scope.isBlank()) {
                scope = "GLOBAL";
            }
            List<String> targetSteps = readTargetSteps(aspectConfig);
            aspects.add(new PipelineYamlAspect(name, enabled, scope, position, targetSteps));
        }
        return aspects;
    }

    private List<String> readTargetSteps(Map<?, ?> aspectConfig) {
        Object configObj = aspectConfig.get("config");
        if (!(configObj instanceof Map<?, ?> configMap)) {
            return List.of();
        }

        Object targetsObj = configMap.get("targetSteps");
        if (!(targetsObj instanceof Iterable<?> targets)) {
            return List.of();
        }

        List<String> values = new ArrayList<>();
        for (Object target : targets) {
            if (target != null) {
                String name = target.toString();
                if (!name.isBlank()) {
                    values.add(name);
                }
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
}
