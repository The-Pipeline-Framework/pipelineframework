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

package org.pipelineframework.extension;

import io.quarkus.deployment.annotations.BuildStep;
import jakarta.enterprise.inject.spi.DeploymentException;
import org.jboss.logging.Logger;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLocator;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Loads delegated operator step configuration from pipeline YAML at Quarkus build time.
 */
public final class PipelineConfigBuildSteps {

    private static final Logger LOG = Logger.getLogger(PipelineConfigBuildSteps.class);
    private static final String PROP_PIPELINE_CONFIG = "pipeline.config";
    private static final String ENV_PIPELINE_CONFIG = "PIPELINE_CONFIG";

    /**
     * Loads delegated operator step configurations from the pipeline YAML for use at build time.
     *
     * @return a PipelineConfigBuildItem containing the list of delegated step configurations, or an empty build item if no pipeline config path is found
     */
    @BuildStep
    PipelineConfigBuildItem loadPipelineConfig() {
        Optional<Path> configPath = resolveConfigPath();
        if (configPath.isEmpty()) {
            return new PipelineConfigBuildItem(List.of());
        }
        return new PipelineConfigBuildItem(readDelegatedSteps(configPath.get()));
    }

    /**
     * Determines the pipeline YAML configuration file path to use.
     *
     * Checks for an explicit path provided via the system property "pipeline.config" or the
     * environment variable "PIPELINE_CONFIG" (first non-blank wins). If an explicit path is present,
     * returns the path resolved to an absolute filesystem path (relative paths are resolved against
     * the current working directory) after verifying it exists and is a regular file. If no explicit
     * path is provided, delegates to PipelineYamlConfigLocator.locate to discover the config.
     *
     * @return an Optional containing the resolved config Path if found, or an empty Optional if none was located
     * @throws DeploymentException if an explicit path is provided but does not resolve to an existing regular file
     */
    private Optional<Path> resolveConfigPath() {
        String explicit = firstNonBlank(System.getProperty(PROP_PIPELINE_CONFIG), System.getenv(ENV_PIPELINE_CONFIG));
        Path cwd = Path.of(System.getProperty("user.dir", ".")).toAbsolutePath().normalize();
        if (explicit != null) {
            Path candidate = Path.of(explicit);
            if (!candidate.isAbsolute()) {
                candidate = cwd.resolve(candidate).normalize();
            }
            if (!Files.isRegularFile(candidate)) {
                throw new DeploymentException("pipeline config path does not exist: '" + explicit + "' (resolved to '" + candidate + "')");
            }
            return Optional.of(candidate);
        }
        return new PipelineYamlConfigLocator().locate(cwd);
    }

    /**
     * Load delegated step configurations from the given pipeline YAML file.
     *
     * Parses the top-level "steps" sequence and converts entries that describe delegated operators
     * into a list of PipelineConfigBuildItem.StepConfig objects.
     *
     * @param configPath the path to the pipeline YAML file to read
     * @return a list of delegated step configurations; empty if no valid delegated steps are present
     * @throws DeploymentException if the YAML root is not a mapping, the file cannot be read, or parsing detects invalid configuration
     */
    private List<PipelineConfigBuildItem.StepConfig> readDelegatedSteps(Path configPath) {
        Object root = readYaml(configPath);
        if (!(root instanceof Map<?, ?> rootMap)) {
            throw new DeploymentException("pipeline config root must be a map: " + configPath);
        }

        Object stepsObj = rootMap.get("steps");
        if (!(stepsObj instanceof Iterable<?> steps)) {
            return List.of();
        }

        List<PipelineConfigBuildItem.StepConfig> delegatedSteps = new ArrayList<>();
        for (Object item : steps) {
            if (!(item instanceof Map<?, ?> stepMap)) {
                LOG.warnf("Ignoring malformed step entry in pipeline config %s: value=%s, type=%s",
                        configPath,
                        item,
                        item == null ? "null" : item.getClass().getName());
                continue;
            }
            PipelineConfigBuildItem.StepConfig step = parseDelegatedStep(stepMap);
            if (step != null) {
                delegatedSteps.add(step);
            } else if (stepMap.containsKey("operator") || stepMap.containsKey("delegate")) {
                LOG.warnf("Ignoring malformed delegated step in pipeline config %s: parse returned null for %s",
                        configPath, stepMap);
            }
        }
        return delegatedSteps;
    }

    /**
     * Parse a single YAML step entry into a delegated StepConfig or skip internal steps.
     *
     * @param stepMap a map of step properties (expected keys: "name", "operator", "delegate", "exposeRest", "exposeGrpc")
     * @return a configured PipelineConfigBuildItem.StepConfig when the entry defines an operator/delegate, or `null` for internal (service-based) steps
     * @throws DeploymentException if the step declares an operator/delegate but has a blank name, if both operator and delegate are present with different values, or if creating the StepConfig fails (the original cause is wrapped)
     */
    private PipelineConfigBuildItem.StepConfig parseDelegatedStep(Map<?, ?> stepMap) {
        String name = stringValue(stepMap, "name");
        String nameTrimmed = name == null ? null : name.trim();
        String operator = stringValue(stepMap, "operator");
        String delegate = stringValue(stepMap, "delegate");
        String operatorTrimmed = operator == null ? null : operator.trim();
        String delegateTrimmed = delegate == null ? null : delegate.trim();
        if (isBlank(operatorTrimmed) && isBlank(delegateTrimmed)) {
            // Internal step (service-based): not resolved through operator metadata.
            return null;
        }
        if (isBlank(nameTrimmed)) {
            throw new DeploymentException("Delegated step name must be non-blank when operator/delegate is configured");
        }
        if (!isBlank(operatorTrimmed) && !isBlank(delegateTrimmed) && !operatorTrimmed.equals(delegateTrimmed)) {
            throw new DeploymentException("Step '" + nameTrimmed + "' defines both operator and delegate with different values");
        }

        String effectiveOperatorTrimmed = !isBlank(operatorTrimmed) ? operatorTrimmed : delegateTrimmed;
        boolean exposeRest = booleanValue(stepMap, "exposeRest", false);
        boolean exposeGrpc = booleanValue(stepMap, "exposeGrpc", false);
        try {
            return new PipelineConfigBuildItem.StepConfig(
                    nameTrimmed,
                    effectiveOperatorTrimmed,
                    exposeRest,
                    exposeGrpc);
        } catch (IllegalArgumentException | NullPointerException e) {
            throw new DeploymentException("Invalid delegated step configuration for step '" + nameTrimmed + "': " + e.getMessage(), e);
        }
    }

    /**
     * Load and parse YAML content from the provided file path.
     *
     * @param configPath the path to the YAML pipeline configuration file
     * @return the parsed YAML root object — typically a Map, List, scalar value, or `null` if the file is empty
     * @throws DeploymentException if the configuration file cannot be read
     */
    private Object readYaml(Path configPath) {
        LoaderOptions loaderOptions = new LoaderOptions();
        Yaml yaml = new Yaml(new SafeConstructor(loaderOptions));
        try (Reader reader = Files.newBufferedReader(configPath)) {
            return yaml.load(reader);
        } catch (IOException e) {
            throw new DeploymentException("Failed to read pipeline config: " + configPath, e);
        }
    }

    /**
     * Retrieve the value for a key from a map and return its string representation.
     *
     * @param map the map to read from
     * @param key the key whose value should be retrieved
     * @return `null` if the key is absent or maps to `null`, otherwise the value's string representation
     */
    private String stringValue(Map<?, ?> map, String key) {
        Object value = map.get(key);
        return value == null ? null : String.valueOf(value);
    }

    /**
     * Parse a boolean-like value from the given map for the specified key or fall back to a default.
     *
     * @param map the map to read the value from
     * @param key the key whose associated value should be interpreted as a boolean
     * @param defaultValue the value to return when the map contains no mapping for the key
     * @return `true` if the mapped value is the Boolean `true` or parses as `"true"`, `false` if it parses as `"false"` (or other non-true text), and `defaultValue` when the key is absent
     */
    private boolean booleanValue(Map<?, ?> map, String key, boolean defaultValue) {
        Object value = map.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Boolean flag) {
            return flag;
        }
        return Boolean.parseBoolean(String.valueOf(value));
    }

    /**
     * Finds the first candidate string that is not null or blank.
     *
     * @param candidates candidate strings to examine, in order
     * @return the first non-blank candidate trimmed, or {@code null} if none found
     */
    private String firstNonBlank(String... candidates) {
        for (String candidate : candidates) {
            if (!isBlank(candidate)) {
                return candidate.trim();
            }
        }
        return null;
    }

    /**
     * Checks whether the given string is null or contains only whitespace.
     *
     * @param value the string to test
     * @return `true` if the string is null or blank (only whitespace), `false` otherwise
     */
    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
