package org.pipelineframework.processor.config;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import org.pipelineframework.processor.mapping.PipelineRuntimeMapping;
import org.pipelineframework.processor.mapping.PipelineRuntimeMapping.Defaults;
import org.pipelineframework.processor.mapping.PipelineRuntimeMapping.Layout;
import org.pipelineframework.processor.mapping.PipelineRuntimeMapping.SyntheticDefaults;
import org.pipelineframework.processor.mapping.PipelineRuntimeMapping.Validation;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

/**
 * Loads pipeline runtime mapping configuration from YAML.
 */
public class PipelineRuntimeMappingLoader {

    /**
     * Creates a new PipelineRuntimeMappingLoader.
     */
    public PipelineRuntimeMappingLoader() {
    }

    /**
     * Load the runtime mapping configuration from the given file path.
     *
     * @param configPath the runtime mapping config path
     * @return the parsed runtime mapping configuration
     */
    public PipelineRuntimeMapping load(Path configPath) {
        Object root = loadYaml(configPath);
        Map<?, ?> rootMap;
        if (root == null) {
            rootMap = Map.of();
        } else if (root instanceof Map<?, ?> map) {
            rootMap = map;
        } else {
            throw new IllegalStateException("Runtime mapping root is not a map");
        }

        Layout layout = Layout.fromString(readString(rootMap.get("layout")));
        Validation validation = Validation.fromString(readString(rootMap.get("validation")));

        Defaults defaults = readDefaults(rootMap.get("defaults"));
        Map<String, String> runtimes = readRuntimes(rootMap.get("runtimes"));
        Map<String, String> modules = readModules(rootMap.get("modules"), defaults.runtime());
        Map<String, String> steps = readMapping(rootMap.get("steps"));
        Map<String, String> synthetics = readMapping(rootMap.get("synthetics"));

        return new PipelineRuntimeMapping(layout, validation, defaults, runtimes, modules, steps, synthetics);
    }

    private Object loadYaml(Path configPath) {
        LoaderOptions options = new LoaderOptions();
        Yaml yaml = new Yaml(new SafeConstructor(options));
        try (Reader reader = Files.newBufferedReader(configPath, StandardCharsets.UTF_8)) {
            return yaml.load(reader);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read runtime mapping: " + configPath, e);
        }
    }

    private Defaults readDefaults(Object defaultsObj) {
        if (!(defaultsObj instanceof Map<?, ?> defaultsMap)) {
            return Defaults.defaultValues();
        }
        String runtime = readString(defaultsMap.get("runtime"));
        String module = readString(defaultsMap.get("module"));
        SyntheticDefaults synthetic = readSyntheticDefaults(defaultsMap.get("synthetic"));
        return new Defaults(runtime, module, synthetic);
    }

    private SyntheticDefaults readSyntheticDefaults(Object syntheticObj) {
        if (!(syntheticObj instanceof Map<?, ?> syntheticMap)) {
            return SyntheticDefaults.defaultValues();
        }
        String module = readString(syntheticMap.get("module"));
        return new SyntheticDefaults(module);
    }

    private Map<String, String> readRuntimes(Object runtimesObj) {
        if (!(runtimesObj instanceof Map<?, ?> runtimesMap)) {
            return Map.of();
        }
        Map<String, String> runtimes = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : runtimesMap.entrySet()) {
            if (entry.getKey() == null) {
                continue;
            }
            String name = String.valueOf(entry.getKey()).trim();
            if (name.isBlank()) {
                continue;
            }
            String value = "";
            if (entry.getValue() instanceof Map<?, ?>) {
                value = name;
            } else {
                value = readString(entry.getValue());
            }
            if (value.isBlank()) {
                value = name;
            }
            runtimes.put(name, value);
        }
        return runtimes;
    }

    private Map<String, String> readModules(Object modulesObj, String defaultRuntime) {
        if (!(modulesObj instanceof Map<?, ?> modulesMap)) {
            return Map.of();
        }
        Map<String, String> modules = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : modulesMap.entrySet()) {
            if (entry.getKey() == null) {
                continue;
            }
            String name = String.valueOf(entry.getKey()).trim();
            if (name.isBlank()) {
                continue;
            }
            String runtime = defaultRuntime;
            Object value = entry.getValue();
            if (value instanceof Map<?, ?> moduleMap) {
                String runtimeValue = readString(moduleMap.get("runtime"));
                if (!runtimeValue.isBlank()) {
                    runtime = runtimeValue;
                }
            }
            modules.put(name, runtime);
        }
        return modules;
    }

    private Map<String, String> readMapping(Object mappingObj) {
        if (!(mappingObj instanceof Map<?, ?> mappingMap)) {
            return Map.of();
        }
        Map<String, String> mapping = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : mappingMap.entrySet()) {
            if (entry.getKey() == null) {
                continue;
            }
            String key = String.valueOf(entry.getKey()).trim();
            if (key.isBlank()) {
                continue;
            }
            String module = readMappingModule(entry.getValue());
            if (!module.isBlank()) {
                mapping.put(key, module);
            }
        }
        return mapping;
    }

    private String readMappingModule(Object value) {
        if (value instanceof Map<?, ?> mapValue) {
            return readString(mapValue.get("module"));
        }
        return readString(value);
    }

    private String readString(Object value) {
        if (value == null) {
            return "";
        }
        return String.valueOf(value).trim();
    }
}
