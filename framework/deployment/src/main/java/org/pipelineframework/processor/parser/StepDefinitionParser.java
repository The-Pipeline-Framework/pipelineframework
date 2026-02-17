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

package org.pipelineframework.processor.parser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.squareup.javapoet.ClassName;
import org.jboss.logging.Logger;
import org.pipelineframework.processor.ir.StepDefinition;
import org.pipelineframework.processor.ir.StepKind;

/**
 * Parser for extracting StepDefinition objects from pipeline template YAML files.
 */
public class StepDefinitionParser {

    private static final Logger LOG = Logger.getLogger(StepDefinitionParser.class);
    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

    /**
     * Creates a new StepDefinitionParser.
     */
    public StepDefinitionParser() {
    }
    
    /**
     * Parses step definitions from a pipeline template YAML file.
     *
     * @param templatePath the path to the pipeline template YAML file
     * @return a list of StepDefinition objects extracted from the template
     * @throws IOException if there's an error reading or parsing the file
     */
    public List<StepDefinition> parseStepDefinitions(Path templatePath) throws IOException {
        if (!Files.exists(templatePath)) {
            LOG.warnf("Pipeline template file does not exist: %s", templatePath);
            return List.of();
        }

        String yamlContent = Files.readString(templatePath);
        Map<String, Object> templateData = YAML_MAPPER.readValue(yamlContent, Map.class);

        Object stepsObj = templateData.get("steps");
        if (!(stepsObj instanceof List)) {
            LOG.debugf("No 'steps' array found in pipeline template");
            return List.of();
        }

        List<?> stepsList = (List<?>) stepsObj;
        List<StepDefinition> stepDefinitions = new ArrayList<>();
        for (Object stepObj : stepsList) {
            if (!(stepObj instanceof Map)) {
                LOG.warnf("Skipping non-map entry in steps array: %s", stepObj);
                continue;
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> stepData = (Map<String, Object>) stepObj;
            StepDefinition stepDef = parseStepDefinition(stepData);
            if (stepDef != null) {
                stepDefinitions.add(stepDef);
            }
        }

        return stepDefinitions;
    }
    
    /**
     * Parses a single step definition from a map of step data.
     *
     * @param stepData the map containing step configuration data
     * @return a StepDefinition object, or null if the step data is invalid
     */
    private StepDefinition parseStepDefinition(Map<String, Object> stepData) {
        String name = getStringValue(stepData, "name");
        if (isBlank(name)) {
            LOG.warnf("Skipping step with null or blank name: %s", stepData);
            return null;
        }

        // Check if it's a delegated step (has 'delegate' field) or internal step (has 'service' field)
        String delegateClassName = getStringValue(stepData, "delegate");
        String serviceClassName = getStringValue(stepData, "service");

        if (!isBlank(delegateClassName) && !isBlank(serviceClassName)) {
            LOG.warnf("Skipping step '%s': 'service' and 'delegate' are mutually exclusive", name);
            return null;
        }

        StepKind kind;
        String executionClassName;

        if (!isBlank(delegateClassName)) {
            kind = StepKind.DELEGATED;
            executionClassName = delegateClassName;
        } else if (!isBlank(serviceClassName)) {
            kind = StepKind.INTERNAL;
            executionClassName = serviceClassName;
        } else {
            // Legacy template-format steps are handled by template model extraction for backward compatibility.
            LOG.debugf("Skipping legacy step '%s' from YAML-driven StepDefinition parsing", name);
            return null;
        }

        // Parse input and output types
        String inputTypeName = getStringValue(stepData, "input");
        String outputTypeName = getStringValue(stepData, "output");

        // If input/output types are not specified in the new format, 
        // they might be in the legacy format (inputTypeName/outputTypeName)
        if (isBlank(inputTypeName)) {
            inputTypeName = getStringValue(stepData, "inputTypeName");
        }
        if (isBlank(outputTypeName)) {
            outputTypeName = getStringValue(stepData, "outputTypeName");
        }

        // Keep delegated input/output optional so they can be derived from delegate generics.
        ClassName inputType = parseOptionalClassName(inputTypeName, name, "input");
        ClassName outputType = parseOptionalClassName(outputTypeName, name, "output");

        // Parse external mapper if present
        String externalMapperName = getStringValue(stepData, "externalMapper");
        ClassName externalMapper = null;
        if (!isBlank(externalMapperName)) {
            externalMapper = parseClassName(externalMapperName);
            if (externalMapper == null) {
                LOG.warnf("Skipping step '%s': invalid external mapper class name '%s'", name, externalMapperName);
                return null;
            }
        }

        if (kind == StepKind.INTERNAL) {
            if (externalMapper != null) {
                LOG.warnf("Ignoring 'externalMapper' on internal step '%s'; this field is only used for delegated steps", name);
                externalMapper = null;
            }
        }

        if (kind == StepKind.DELEGATED) {
            boolean hasInput = !isBlank(inputTypeName);
            boolean hasOutput = !isBlank(outputTypeName);
            if (hasInput != hasOutput) {
                LOG.warnf("Skipping step '%s': delegated steps must provide both 'input' and 'output' together", name);
                return null;
            }
        }

        // Create the execution class name
        ClassName executionClass = parseClassName(executionClassName);
        if (executionClass == null) {
            LOG.warnf("Skipping step '%s': invalid execution class name '%s'", name, executionClassName);
            return null;
        }

        return new StepDefinition(name, kind, executionClass, externalMapper, inputType, outputType);
    }

    private ClassName parseOptionalClassName(String typeName, String stepName, String fieldName) {
        if (isBlank(typeName)) {
            return null;
        }
        ClassName parsed = parseClassName(typeName);
        if (parsed == null) {
            LOG.warnf("Skipping step '%s': invalid %s class name '%s'", stepName, fieldName, typeName);
        }
        return parsed;
    }

    /**
     * Safely extracts a string value from a map, handling null and non-string values.
     *
     * @param map the map to extract from
     * @param key the key to look up
     * @return the string value, or null if not present or not a string
     */
    private String getStringValue(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value == null) {
            return null;
        }
        if (value instanceof String) {
            return (String) value;
        }
        // Log a warning when value is non-null and not a String
        LOG.warnf("Non-String value for key '%s' in step definition: %s (actual type: %s)", 
            key, value, value.getClass().getName());
        return String.valueOf(value);
    }

    /**
     * Checks if a string is null, empty, or blank.
     *
     * @param value the string to check
     * @return true if the string is null, empty, or blank
     */
    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    /**
     * Parses a fully qualified class name string into a ClassName object.
     *
     * @param className the fully qualified class name string
     * @return a ClassName object, or null if the className is invalid
     */
    private ClassName parseClassName(String className) {
        if (className == null || className.isBlank()) {
            return null;
        }

        int lastDot = className.lastIndexOf('.');
        if (lastDot < 0) {
            // No package, just a simple name
            return ClassName.get("", className);
        }
        if (lastDot == 0) {
            // Starts with a dot - invalid package
            LOG.warnf("Invalid class name format: '%s' (starts with dot)", className);
            return null;
        }

        String packageName = className.substring(0, lastDot);
        String simpleName = className.substring(lastDot + 1);

        // Handle nested classes
        String[] parts = simpleName.split("\\$");
        if (parts.length == 1) {
            return ClassName.get(packageName, simpleName);
        } else {
            String outerName = parts[0];
            String[] nestedNames = new String[parts.length - 1];
            System.arraycopy(parts, 1, nestedNames, 0, parts.length - 1);
            return ClassName.get(packageName, outerName, nestedNames);
        }
    }
}
