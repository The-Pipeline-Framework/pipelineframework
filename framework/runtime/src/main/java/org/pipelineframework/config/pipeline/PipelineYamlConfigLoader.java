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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Logger;

import org.pipelineframework.config.PlatformOverrideResolver;
import org.pipelineframework.config.TransportOverrideResolver;
import org.pipelineframework.config.boundary.PipelineCheckpointConfig;
import org.pipelineframework.config.boundary.PipelineInputBoundaryConfig;
import org.pipelineframework.config.boundary.PipelineOutputBoundaryConfig;
import org.pipelineframework.config.boundary.PipelineSubscriptionConfig;
import org.yaml.snakeyaml.Yaml;

/**
 * Loads pipeline.yaml configuration for runtime usage.
 */
public class PipelineYamlConfigLoader {
    private static final Logger LOG = Logger.getLogger(PipelineYamlConfigLoader.class.getName());
    private final Function<String, String> propertyLookup;
    private final Function<String, String> envLookup;

    /**
         * Construct a loader that reads system properties and environment variables.
         *
         * This default constructor delegates to the configurable constructor using
         * System::getProperty for property lookup and System::getenv for environment lookup.
         */
    public PipelineYamlConfigLoader() {
        this(System::getProperty, System::getenv);
    }

    /**
     * Creates a PipelineYamlConfigLoader that uses the provided lookup functions to resolve
     * system properties and environment variables when parsing pipeline YAML.
     *
     * @param propertyLookup function that maps a property name to its value; if null, a lookup that always returns null is used
     * @param envLookup      function that maps an environment variable name to its value; if null, a lookup that always returns null is used
     */
    public PipelineYamlConfigLoader(Function<String, String> propertyLookup, Function<String, String> envLookup) {
        this.propertyLookup = propertyLookup == null ? key -> null : propertyLookup;
        this.envLookup = envLookup == null ? key -> null : envLookup;
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

    /**
     * Create a PipelineYamlConfig by parsing the provided YAML root map.
     *
     * Reads top-level keys (basePackage, transport, platform), applies environment/property overrides,
     * normalizes or defaults transport and platform values, and reads steps, aspects, and boundary declarations.
     *
     * @param root   the deserialized YAML root; must be a Map (otherwise an exception is thrown)
     * @param source descriptive source used in error messages (for example a file path or resource)
     * @return a PipelineYamlConfig populated from the provided YAML root
     * @throws IllegalStateException if {@code root} is not a Map
     */
    private PipelineYamlConfig parseRoot(Object root, String source) {
        if (!(root instanceof Map<?, ?> rootMap)) {
            throw new IllegalStateException("Pipeline config root is not a map for " + source);
        }
        rejectLegacyConnectors(rootMap);

        String basePackage = readString(rootMap, "basePackage");
        String transport = resolveConfigValue(
            readString(rootMap, "transport"),
            this::resolveTransportOverride,
            TransportOverrideResolver::normalizeKnownTransport,
            "GRPC",
            "transport");
        String platform = resolveConfigValue(
            readString(rootMap, "platform"),
            this::resolvePlatformOverride,
            PlatformOverrideResolver::normalizeKnownPlatform,
            "COMPUTE",
            "platform");
        List<PipelineYamlStep> steps = readSteps(rootMap);
        List<PipelineYamlAspect> aspects = readAspects(rootMap);
        PipelineInputBoundaryConfig input = readInputBoundary(rootMap);
        PipelineOutputBoundaryConfig output = readOutputBoundary(rootMap);

        return new PipelineYamlConfig(basePackage, transport, platform, steps, aspects, input, output);
    }

    /**
     * Resolve a transport override using the configured property and environment lookups.
     *
     * @return the transport override value if present, or {@code null} when no override is configured
     */
    private String resolveTransportOverride() {
        return TransportOverrideResolver.resolveOverride(propertyLookup, envLookup);
    }

    /**
     * Resolve a platform override using the configured property and environment lookups.
     *
     * @return the platform override value if present, or {@code null} when no override is configured
     */
    private String resolvePlatformOverride() {
        return PlatformOverrideResolver.resolveOverride(propertyLookup, envLookup);
    }

    private String resolveConfigValue(
            String rawValue,
            Supplier<String> overrideSupplier,
            Function<String, String> normalizer,
            String defaultValue,
            String label) {
        String effectiveValue = rawValue;
        String overrideValue = overrideSupplier == null ? null : overrideSupplier.get();
        boolean fromOverride = overrideValue != null && !overrideValue.isBlank();
        if (fromOverride) {
            effectiveValue = overrideValue;
        }
        if (effectiveValue == null || effectiveValue.isBlank()) {
            return defaultValue;
        }

        String normalized = normalizer.apply(effectiveValue);
        if (normalized != null) {
            return normalized;
        }

        if (fromOverride) {
            LOG.warning("Unknown " + label + " override '" + effectiveValue
                + "'; defaulting pipeline " + label + " to " + defaultValue + ".");
        } else {
            LOG.warning("Unknown " + label + " in YAML config '" + effectiveValue
                + "'; defaulting pipeline " + label + " to " + defaultValue + ".");
        }
        return defaultValue;
    }

    /**
     * Parse the "steps" entry from the YAML root map into a list of PipelineYamlStep objects.
     *
     * @param rootMap the parsed YAML root map potentially containing a "steps" entry
     * @return a list of PipelineYamlStep instances for entries that have a non-blank name and non-blank output type; returns an empty list if no valid "steps" section is present
     */
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

    /**
     * Parses the "aspects" section from the provided YAML root map and returns a list of aspect configurations.
     *
     * The method looks for an "aspects" entry whose value is a map of aspect-name -> aspect-config map.
     * For each aspect it reads the "enabled" flag (defaults to `false`), "position" (defaults to `"AFTER_STEP"`),
     * "scope" (defaults to `"GLOBAL"`), and the configured target steps.
     *
     * @param rootMap the deserialized YAML root map; expected to contain an "aspects" mapping of aspect names to config maps
     * @return a list of PipelineYamlAspect objects parsed from the "aspects" section, or an empty list if none are present
     */
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

    /**
     * Parses the optional root-level input boundary.
     *
     * @param rootMap the deserialized YAML root map
     * @return the input boundary config, or {@code null} when no input boundary is declared
     */
    private PipelineInputBoundaryConfig readInputBoundary(Map<?, ?> rootMap) {
        Object inputObj = rootMap.get("input");
        if (inputObj == null) {
            return null;
        }
        if (!(inputObj instanceof Map<?, ?> inputMap)) {
            throw new IllegalArgumentException("pipeline input boundary must be defined as a map");
        }
        Object subscriptionObj = inputMap.get("subscription");
        if (subscriptionObj == null) {
            return null;
        }
        if (!(subscriptionObj instanceof Map<?, ?> subscriptionMap)) {
            throw new IllegalArgumentException("input.subscription must be defined as a map");
        }
        return new PipelineInputBoundaryConfig(new PipelineSubscriptionConfig(
            readRequiredString(subscriptionMap, "publication", "input.subscription"),
            readString(subscriptionMap, "mapper")));
    }

    /**
     * Parses the optional root-level output boundary.
     *
     * @param rootMap the deserialized YAML root map
     * @return the output boundary config, or {@code null} when no output boundary is declared
     */
    private PipelineOutputBoundaryConfig readOutputBoundary(Map<?, ?> rootMap) {
        Object outputObj = rootMap.get("output");
        if (outputObj == null) {
            return null;
        }
        if (!(outputObj instanceof Map<?, ?> outputMap)) {
            throw new IllegalArgumentException("pipeline output boundary must be defined as a map");
        }
        Object checkpointObj = outputMap.get("checkpoint");
        if (checkpointObj == null) {
            return null;
        }
        if (!(checkpointObj instanceof Map<?, ?> checkpointMap)) {
            throw new IllegalArgumentException("output.checkpoint must be defined as a map");
        }
        return new PipelineOutputBoundaryConfig(new PipelineCheckpointConfig(
            readRequiredString(checkpointMap, "publication", "output.checkpoint"),
            readStringList(checkpointMap, "idempotencyKeyFields")));
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
     * Extracts a list of trimmed, non-blank strings from an iterable value stored at the given map key.
     *
     * @param map the map to read from
     * @param key the key whose value is expected to be an iterable of items
     * @return a list of trimmed, non-blank strings from the iterable at {@code key}, or an empty list if the key is absent or the value is not iterable
     */
    private List<String> readStringList(Map<?, ?> map, String key) {
        Object value = map.get(key);
        if (!(value instanceof Iterable<?> values)) {
            return List.of();
        }
        List<String> items = new ArrayList<>();
        for (Object element : values) {
            if (element == null) {
                continue;
            }
            String text = element.toString().trim();
            if (!text.isBlank()) {
                items.add(text);
            }
        }
        return items;
    }

    /**
     * Return the list of target step names defined at "config.targetSteps" for an aspect.
     *
     * @param aspectConfig the aspect map (expected to contain a "config" map)
     * @return the list of target step names from "config.targetSteps", or an empty list if the entry is missing or not a list
     */
    private List<String> readTargetSteps(Map<?, ?> aspectConfig) {
        Object configObj = aspectConfig.get("config");
        if (!(configObj instanceof Map<?, ?> configMap)) {
            return List.of();
        }
        return readStringList(configMap, "targetSteps");
    }

    /**
     * Retrieve the value for a key from the map as a string, or null if the key is absent or maps to null.
     *
     * @param map the map to read from
     * @param key the key whose value should be returned
     * @return the value's string representation, or null if the key is not present or maps to null
     */
    private String readString(Map<?, ?> map, String key) {
        Object value = map.get(key);
        return value == null ? null : value.toString();
    }

    /**
     * Interpret the value at the given key in the map as a boolean, returning a fallback when the key is absent.
     *
     * The method returns {@code true} if the value is a {@code Boolean} equal to {@code true} or a string that
     * parses to {@code true} (case-insensitive). If the map contains no entry for the key, {@code defaultValue}
     * is returned; otherwise the parsed boolean value is returned (or {@code false} if parsing yields {@code false}).
     *
     * @param map the source map containing configuration values
     * @param key the key whose value should be interpreted as a boolean
     * @param defaultValue the value to return when the map does not contain the key
     * @return {@code true} if the value for {@code key} is a {@code Boolean} {@code true} or a string that parses to {@code true}, {@code defaultValue} if the key is absent, {@code false} otherwise
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
     * Reads an integer value for a given key from a map, applying parsing and fallback rules.
     *
     * The method accepts numeric values or string representations. If the key is missing or the value is blank,
     * the provided defaultValue is returned.
     *
     * @param map the source map containing the value
     * @param key the key whose value should be read and converted to an int
     * @param defaultValue the value to return when the key is absent or the value is blank
     * @return the integer value for the key, or {@code defaultValue} if absent or blank
     * @throws IllegalStateException if a non-blank, non-numeric value is present and cannot be parsed as an integer
     */
    private int readInt(Map<?, ?> map, String key, int defaultValue) {
        Object value = map.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number number) {
            return exactIntegerValue(number, key);
        }
        String text = value.toString();
        if (text.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(text.trim());
        } catch (NumberFormatException ex) {
            throw new IllegalStateException("Invalid integer value '" + text + "' for key '" + key + "'", ex);
        }
    }

    private int exactIntegerValue(Number number, String key) {
        if (number instanceof Byte || number instanceof Short || number instanceof Integer) {
            return number.intValue();
        }
        if (number instanceof Long longValue) {
            if (longValue < Integer.MIN_VALUE || longValue > Integer.MAX_VALUE) {
                throw new IllegalStateException("Invalid integer value '" + number + "' for key '" + key + "'");
            }
            return longValue.intValue();
        }
        if (number instanceof BigInteger bigInteger) {
            if (bigInteger.compareTo(BigInteger.valueOf(Integer.MIN_VALUE)) < 0
                || bigInteger.compareTo(BigInteger.valueOf(Integer.MAX_VALUE)) > 0) {
                throw new IllegalStateException("Invalid integer value '" + number + "' for key '" + key + "'");
            }
            return bigInteger.intValueExact();
        }
        if (number instanceof BigDecimal bigDecimal) {
            try {
                return bigDecimal.intValueExact();
            } catch (ArithmeticException ex) {
                throw new IllegalStateException("Invalid integer value '" + number + "' for key '" + key + "'", ex);
            }
        }
        if (number instanceof Float || number instanceof Double) {
            double doubleValue = number.doubleValue();
            if (!Double.isFinite(doubleValue)
                || doubleValue != Math.rint(doubleValue)
                || doubleValue < Integer.MIN_VALUE
                || doubleValue > Integer.MAX_VALUE) {
                throw new IllegalStateException("Invalid integer value '" + number + "' for key '" + key + "'");
            }
            return (int) doubleValue;
        }
        long longValue = number.longValue();
        if (longValue < Integer.MIN_VALUE || longValue > Integer.MAX_VALUE || number.doubleValue() != longValue) {
            throw new IllegalStateException("Invalid integer value '" + number + "' for key '" + key + "'");
        }
        return (int) longValue;
    }
}
