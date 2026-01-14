package org.pipelineframework.processor.config;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import org.pipelineframework.processor.ir.AspectPosition;
import org.pipelineframework.processor.ir.AspectScope;
import org.pipelineframework.processor.ir.PipelineAspectModel;
import org.yaml.snakeyaml.Yaml;

/**
 * Loads pipeline aspect configuration from a YAML file.
 */
public class PipelineAspectConfigLoader {

    /**
     * Creates a new PipelineAspectConfigLoader.
     */
    public PipelineAspectConfigLoader() {
    }

    /**
     * Load aspect models from the specified YAML file.
     *
     * @param configPath the pipeline configuration file path
     * @return list of enabled aspect models, possibly empty
     */
    public List<PipelineAspectModel> load(Path configPath) {
        Object root = loadYaml(configPath);
        if (!(root instanceof Map<?, ?> rootMap)) {
            return List.of();
        }

        Object aspectsValue = rootMap.get("aspects");
        if (aspectsValue == null) {
            return List.of();
        }
        if (!(aspectsValue instanceof Map<?, ?> aspectsMap)) {
            throw new IllegalArgumentException("Expected 'aspects' to be a map in " + configPath);
        }

        List<PipelineAspectModel> aspects = new ArrayList<>();
        for (Map.Entry<?, ?> entry : aspectsMap.entrySet()) {
            String name = String.valueOf(entry.getKey());
            if (!(entry.getValue() instanceof Map<?, ?> aspectConfig)) {
                throw new IllegalArgumentException("Aspect '" + name + "' must be a map in " + configPath);
            }

            boolean enabled = getBoolean(aspectConfig.get("enabled"), true);
            if (!enabled) {
                continue;
            }

            String scopeValue = getString(aspectConfig.get("scope"));
            String positionValue = getString(aspectConfig.get("position"));
            int order = getInt(aspectConfig.get("order"), 0);
            Map<String, Object> config = getConfigMap(aspectConfig.get("config"));

            AspectScope scope = AspectScope.valueOf(scopeValue);
            AspectPosition position = AspectPosition.valueOf(positionValue);

            aspects.add(new PipelineAspectModel(name, scope, position, order, config));
        }

        return aspects;
    }

    private Object loadYaml(Path configPath) {
        Yaml yaml = new Yaml();
        try (Reader reader = Files.newBufferedReader(configPath, StandardCharsets.UTF_8)) {
            return yaml.load(reader);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read pipeline config: " + configPath, e);
        }
    }

    private boolean getBoolean(Object value, boolean defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Boolean booleanValue) {
            return booleanValue;
        }
        return Boolean.parseBoolean(String.valueOf(value));
    }

    private int getInt(Object value, int defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        return Integer.parseInt(String.valueOf(value));
    }

    private String getString(Object value) {
        if (value == null) {
            throw new IllegalArgumentException("Missing required aspect field");
        }
        return String.valueOf(value);
    }

    private Map<String, Object> getConfigMap(Object value) {
        if (value == null) {
            return Collections.emptyMap();
        }
        if (value instanceof Map<?, ?> mapValue) {
            Map<String, Object> config = new HashMap<>();
            for (Map.Entry<?, ?> entry : mapValue.entrySet()) {
                config.put(String.valueOf(entry.getKey()), entry.getValue());
            }
            return config;
        }
        throw new IllegalArgumentException("Aspect config must be a map");
    }
}
