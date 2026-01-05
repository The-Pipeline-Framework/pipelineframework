package org.pipelineframework.processor.config;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

/**
 * Loads pipeline step configuration metadata from a YAML file.
 */
public class PipelineStepConfigLoader {

    /**
     * Minimal step configuration extracted from the pipeline YAML.
     *
     * @param basePackage the configured base package
     * @param inputTypes the list of input type names declared in steps
     * @param outputTypes the list of output type names declared in steps
     */
    public record StepConfig(String basePackage, String transport, List<String> inputTypes, List<String> outputTypes) {}

    /**
     * Load output type names and base package from the pipeline configuration.
     *
     * @param configPath the pipeline configuration file path
     * @return a StepConfig containing the base package and output type names
     */
    public StepConfig load(Path configPath) {
        Object root = loadYaml(configPath);
        if (!(root instanceof Map<?, ?> rootMap)) {
            return new StepConfig("", "", List.of(), List.of());
        }

        String basePackage = getString(rootMap.get("basePackage"));
        String transport = getString(rootMap.get("transport"));
        Object stepsValue = rootMap.get("steps");
        if (!(stepsValue instanceof List<?> steps)) {
            return new StepConfig(basePackage, transport, List.of(), List.of());
        }

        List<String> inputTypes = new ArrayList<>();
        List<String> outputTypes = new ArrayList<>();
        for (Object step : steps) {
            if (!(step instanceof Map<?, ?> stepMap)) {
                continue;
            }
            Object inputTypeName = stepMap.get("inputTypeName");
            if (inputTypeName != null) {
                inputTypes.add(String.valueOf(inputTypeName));
            }
            Object outputTypeName = stepMap.get("outputTypeName");
            if (outputTypeName != null) {
                outputTypes.add(String.valueOf(outputTypeName));
            }
        }

        return new StepConfig(basePackage, transport, inputTypes, outputTypes);
    }

    private Object loadYaml(Path configPath) {
        Yaml yaml = new Yaml();
        try (Reader reader = Files.newBufferedReader(configPath, StandardCharsets.UTF_8)) {
            return yaml.load(reader);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read pipeline config: " + configPath, e);
        }
    }

    private String getString(Object value) {
        if (value == null) {
            return "";
        }
        return String.valueOf(value);
    }
}
