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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import javax.tools.Diagnostic;

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
    private static final Set<String> SUPPORTED_STEP_KEYS = Set.of(
        "name",
        "service",
        "operator",
        "delegate",
        "input",
        "output",
        "operatorMapper",
        "externalMapper",
        "cardinality",
        "inputTypeName",
        "inputFields",
        "outputTypeName",
        "outputFields");
    private final BiConsumer<Diagnostic.Kind, String> diagnosticReporter;

    /**
     * Creates a new StepDefinitionParser.
     */
    public StepDefinitionParser() {
        this((kind, message) -> {
        });
    }

    /**
     * Creates a StepDefinitionParser with a diagnostic reporter.
     *
     * @param diagnosticReporter reporter used to surface parse diagnostics (e.g. via annotation processing Messager)
     */
    public StepDefinitionParser(BiConsumer<Diagnostic.Kind, String> diagnosticReporter) {
        this.diagnosticReporter = diagnosticReporter == null ? (kind, message) -> {
        } : diagnosticReporter;
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
        @SuppressWarnings("unchecked")
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
        reportUnknownStepKeys(name, stepData);

        // Check if it's an operator step (has 'operator'/'delegate' field) or internal step (has 'service' field)
        String operatorClassName = getStringValue(stepData, "operator");
        String delegateClassName = getStringValue(stepData, "delegate");
        String serviceClassName = getStringValue(stepData, "service");
        String delegatedClassName = null;

        if (!isBlank(operatorClassName) && !isBlank(delegateClassName)) {
            String message = "Skipping step '" + name + "': 'operator' and 'delegate' are aliases and are mutually exclusive";
            LOG.warn(message);
            report(Diagnostic.Kind.ERROR, message);
            return null;
        }
        if (!isBlank(operatorClassName)) {
            delegatedClassName = operatorClassName;
        } else if (!isBlank(delegateClassName)) {
            delegatedClassName = delegateClassName;
        }

        if (!isBlank(delegatedClassName) && !isBlank(serviceClassName)) {
            String message = "Skipping step '" + name + "': 'service' and delegated execution ('operator'/'delegate') are mutually exclusive";
            LOG.warn(message);
            report(Diagnostic.Kind.ERROR, message);
            return null;
        }

        StepKind kind;
        String executionClassName;

        if (!isBlank(delegatedClassName)) {
            kind = StepKind.DELEGATED;
            executionClassName = delegatedClassName;
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
        if (!isBlank(inputTypeName) && inputType == null) {
            return null;
        }
        if (!isBlank(outputTypeName) && outputType == null) {
            return null;
        }

        // Parse operator mapper / legacy external mapper if present
        String operatorMapperName = getStringValue(stepData, "operatorMapper");
        String externalMapperName = getStringValue(stepData, "externalMapper");
        String effectiveMapperName = null;
        if (!isBlank(operatorMapperName) && !isBlank(externalMapperName)) {
            String message = "Skipping step '" + name + "': 'operatorMapper' and 'externalMapper' are aliases and are mutually exclusive";
            LOG.warn(message);
            report(Diagnostic.Kind.ERROR, message);
            return null;
        }
        if (!isBlank(operatorMapperName)) {
            effectiveMapperName = operatorMapperName;
        } else if (!isBlank(externalMapperName)) {
            effectiveMapperName = externalMapperName;
        }

        ClassName externalMapper = null;
        if (!isBlank(effectiveMapperName)) {
            externalMapper = parseClassName(effectiveMapperName);
            if (externalMapper == null) {
                LOG.warnf("Skipping step '%s': invalid operator mapper class name '%s'", name, effectiveMapperName);
                return null;
            }
        }

        if (kind == StepKind.INTERNAL) {
            if (externalMapper != null) {
                String message = "Ignoring 'operatorMapper'/'externalMapper' on internal step '" + name
                    + "'; mapper override is only used for delegated steps";
                LOG.warn(message);
                report(Diagnostic.Kind.WARNING, message);
                externalMapper = null;
            }
            if (!isBlank(inputTypeName) || !isBlank(outputTypeName)) {
                String message = "Ignoring 'input'/'output' on internal step '" + name
                    + "'; internal types are derived from the @PipelineStep service signature";
                LOG.warn(message);
                report(Diagnostic.Kind.WARNING, message);
                inputType = null;
                outputType = null;
            }
        }

        if (kind == StepKind.DELEGATED) {
            boolean hasInput = !isBlank(inputTypeName);
            boolean hasOutput = !isBlank(outputTypeName);
            if (hasInput != hasOutput) {
                String message = "Skipping step '" + name
                    + "': delegated steps must provide both 'input' and 'output' together";
                LOG.warn(message);
                report(Diagnostic.Kind.ERROR, message);
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

    private void reportUnknownStepKeys(String stepName, Map<String, Object> stepData) {
        Set<String> unknownKeys = new HashSet<>();
        for (String key : stepData.keySet()) {
            if (!SUPPORTED_STEP_KEYS.contains(key)) {
                unknownKeys.add(key);
            }
        }
        if (unknownKeys.isEmpty()) {
            return;
        }
        String message = "Step '" + stepName + "' contains unsupported keys that will be ignored: "
            + String.join(", ", unknownKeys);
        LOG.warn(message);
        report(Diagnostic.Kind.WARNING, message);
    }

    private void report(Diagnostic.Kind kind, String message) {
        diagnosticReporter.accept(kind, message);
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
            if (!isValidIdentifier(className)) {
                LOG.warnf("Invalid class name format: '%s' (invalid simple name)", className);
                return null;
            }
            return ClassName.get("", className);
        }
        if (lastDot == 0) {
            // Starts with a dot - invalid package
            LOG.warnf("Invalid class name format: '%s' (starts with dot)", className);
            return null;
        }

        String packageName = className.substring(0, lastDot);
        String simpleName = className.substring(lastDot + 1);
        if (packageName.endsWith(".") || packageName.contains("..")) {
            LOG.warnf("Invalid class name format: '%s' (invalid package segments)", className);
            return null;
        }
        String[] pkgSegments = packageName.split("\\.");
        for (String segment : pkgSegments) {
            if (!isValidIdentifier(segment)) {
                LOG.warnf("Invalid class name format: '%s' (invalid package segment '%s')", className, segment);
                return null;
            }
        }
        if (simpleName.isBlank()) {
            LOG.warnf("Invalid class name format: '%s' (missing class name)", className);
            return null;
        }

        // Handle nested classes
        String[] parts = simpleName.split("\\$");
        for (String part : parts) {
            if (!isValidIdentifier(part)) {
                LOG.warnf("Invalid class name format: '%s' (invalid class segment '%s')", className, part);
                return null;
            }
        }
        if (parts.length == 1) {
            return ClassName.get(packageName, simpleName);
        } else {
            String outerName = parts[0];
            String[] nestedNames = new String[parts.length - 1];
            System.arraycopy(parts, 1, nestedNames, 0, parts.length - 1);
            return ClassName.get(packageName, outerName, nestedNames);
        }
    }

    private boolean isValidIdentifier(String segment) {
        if (segment == null || segment.isEmpty()) {
            return false;
        }
        if (!Character.isJavaIdentifierStart(segment.charAt(0))) {
            return false;
        }
        for (int i = 1; i < segment.length(); i++) {
            if (!Character.isJavaIdentifierPart(segment.charAt(i))) {
                return false;
            }
        }
        return true;
    }
}
