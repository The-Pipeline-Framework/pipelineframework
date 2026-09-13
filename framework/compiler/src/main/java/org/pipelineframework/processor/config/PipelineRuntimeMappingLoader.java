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
     * Load a PipelineRuntimeMapping from the YAML configuration at the given path.
     *
     * The YAML may contain top-level sections for layout, validation, defaults (including synthetic defaults),
     * runtimes, modules, steps, and synthetics; each section is parsed and used to construct the returned mapping.
     *
     * @param configPath the path to the runtime mapping YAML file
     * @return a PipelineRuntimeMapping built from the configuration file
     * @throws IllegalStateException if the YAML root is not a map or if the file cannot be read
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

    /**
     * Load and parse YAML content from the given file path.
     *
     * @param configPath the path to the YAML configuration file
     * @return the parsed YAML root object (for example a Map, List, scalar, or `null` if the document is empty)
     * @throws IllegalStateException if the file cannot be read
     */
    private Object loadYaml(Path configPath) {
        LoaderOptions options = new LoaderOptions();
        Yaml yaml = new Yaml(new SafeConstructor(options));
        try (Reader reader = Files.newBufferedReader(configPath, StandardCharsets.UTF_8)) {
            return yaml.load(reader);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read runtime mapping: " + configPath, e);
        }
    }

    /**
     * Parses the "defaults" section of the runtime mapping YAML and returns a Defaults object.
     *
     * @param defaultsObj the parsed YAML value for the "defaults" section; expected to be a Map, otherwise defaults are used
     * @return a Defaults instance constructed from the "runtime", "module", and nested "synthetic" entries (or Defaults.defaultValues() when input is missing or not a map)
     */
    private Defaults readDefaults(Object defaultsObj) {
        if (!(defaultsObj instanceof Map<?, ?> defaultsMap)) {
            return Defaults.defaultValues();
        }
        String runtime = readString(defaultsMap.get("runtime"));
        String module = readString(defaultsMap.get("module"));
        SyntheticDefaults synthetic = readSyntheticDefaults(defaultsMap.get("synthetic"));
        return new Defaults(runtime, module, synthetic);
    }

    /**
     * Parse the synthetic defaults section and produce a corresponding SyntheticDefaults instance.
     *
     * @param syntheticObj the parsed YAML value for the `synthetic` section (expected to be a Map containing a `module` entry); if not a Map, defaults are used
     * @return a SyntheticDefaults initialized from the map's `module` value, or SyntheticDefaults.defaultValues() if the input is not a map
     */
    private SyntheticDefaults readSyntheticDefaults(Object syntheticObj) {
        if (!(syntheticObj instanceof Map<?, ?> syntheticMap)) {
            return SyntheticDefaults.defaultValues();
        }
        String module = readString(syntheticMap.get("module"));
        return new SyntheticDefaults(module);
    }

    /**
     * Parse the "runtimes" section from a YAML node into an ordered map of runtime names to runtime identifiers.
     *
     * @param runtimesObj the raw YAML node for the "runtimes" section; expected to be a Map of keys to values
     * @return a LinkedHashMap preserving insertion order where each key is the trimmed runtime name and each value is the resolved runtime identifier; returns an empty map if {@code runtimesObj} is not a map
     */
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

    /**
     * Parse the `modules` YAML section into an insertion-ordered map of module name to runtime.
     *
     * @param modulesObj    the parsed YAML value for the `modules` section; expected to be a map where
     *                      each key is a module name and each value is either a runtime string or a
     *                      map that may contain a `runtime` entry
     * @param defaultRuntime the runtime to use when a module entry does not specify a runtime
     * @return               a LinkedHashMap preserving the original order that maps each non-blank
     *                       module name to its resolved runtime; returns an empty map if
     *                       `modulesObj` is not a map
     */
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
            } else {
                String runtimeValue = readString(value);
                if (!runtimeValue.isBlank()) {
                    runtime = runtimeValue;
                }
            }
            modules.put(name, runtime);
        }
        return modules;
    }

    /**
     * Parse a YAML/heterogeneous mapping into an ordered key→module mapping.
     *
     * Accepts a Map-like input where each entry's key is converted to a trimmed string and each value is resolved
     * into a module name (via readMappingModule). Entries with null or blank keys are ignored; entries whose
     * resolved module is blank are omitted.
     *
     * @param mappingObj the raw mapping object (expected to be a Map); other types yield an empty mapping
     * @return an insertion-ordered map from trimmed keys to non-blank module names, or an empty map if the input is not a map
     */
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

    /**
     * Resolve the module name for a mapping entry.
     *
     * @param value the mapping entry value; either a Map containing a "module" key or a scalar value
     * @return the resolved module name as a trimmed string (empty string if the value or key is missing)
     */
    private String readMappingModule(Object value) {
        if (value instanceof Map<?, ?> mapValue) {
            return readString(mapValue.get("module"));
        }
        return readString(value);
    }

    /**
     * Convert an arbitrary object to its trimmed string representation.
     *
     * @param value the object to convert; may be null
     * @return `""` if `value` is null, otherwise `String.valueOf(value).trim()`
     */
    private String readString(Object value) {
        if (value == null) {
            return "";
        }
        return String.valueOf(value).trim();
    }
}
