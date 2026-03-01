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
import org.pipelineframework.processor.ir.MapperFallbackMode;
import org.pipelineframework.processor.ir.StepDefinition;
import org.pipelineframework.processor.ir.StepKind;
import org.pipelineframework.processor.ir.StreamingShape;

/**
 * Parser for extracting StepDefinition objects from pipeline template YAML files.
 */
public class StepDefinitionParser {

    private static final Logger LOG = Logger.getLogger(StepDefinitionParser.class);
    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());
    /**
     * Legacy suffix used to resolve short-form internal step types.
     * For legacy internal steps, {@code input/output: Foo} resolves to
     * {@code <basePackage> + LEGACY_INTERNAL_PACKAGE_SUFFIX + Foo}.
     */
    public static final String DEFAULT_LEGACY_INTERNAL_PACKAGE_SUFFIX = ".common.domain.";
    private static final Set<String> SUPPORTED_STEP_KEYS = Set.of(
        "name",
        "service",
        "operator",
        "delegate",
        "input",
        "output",
        "operatorMapper",
        "externalMapper",
        "mapperFallback",
        "cardinality",
        "inputTypeName",
        "inputFields",
        "outputTypeName",
        "outputFields");
    private final BiConsumer<Diagnostic.Kind, String> diagnosticReporter;
    private final String legacyInternalPackageSuffix;

    /**
     * Creates a StepDefinitionParser that uses a no-op diagnostic reporter.
     *
     * Initializes the parser with a default diagnostic reporter which ignores all diagnostics.
     */
    public StepDefinitionParser() {
        this((kind, message) -> {
        }, DEFAULT_LEGACY_INTERNAL_PACKAGE_SUFFIX);
    }

    /**
     * Creates a StepDefinitionParser with a diagnostic reporter.
     *
     * @param diagnosticReporter reporter used to surface parse diagnostics (e.g. via annotation processing Messager)
     */
    public StepDefinitionParser(BiConsumer<Diagnostic.Kind, String> diagnosticReporter) {
        this(diagnosticReporter, DEFAULT_LEGACY_INTERNAL_PACKAGE_SUFFIX);
    }

    /**
     * Creates a StepDefinitionParser with a diagnostic reporter and configurable legacy internal package suffix.
     *
     * @param diagnosticReporter reporter used to surface parse diagnostics (e.g. via annotation processing Messager)
     * @param legacyInternalPackageSuffix suffix used when resolving short-form legacy internal input/output type names
     */
    public StepDefinitionParser(
        BiConsumer<Diagnostic.Kind, String> diagnosticReporter,
        String legacyInternalPackageSuffix) {
        this.diagnosticReporter = diagnosticReporter == null ? (kind, message) -> {
        } : diagnosticReporter;
        this.legacyInternalPackageSuffix = isBlank(legacyInternalPackageSuffix)
            ? DEFAULT_LEGACY_INTERNAL_PACKAGE_SUFFIX
            : legacyInternalPackageSuffix;
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
        String basePackage = getStringValue(templateData, "basePackage");

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
            StepDefinition stepDef = parseStepDefinition(stepData, basePackage);
            if (stepDef != null) {
                stepDefinitions.add(stepDef);
            }
        }

        return stepDefinitions;
    }
    
    /**
     * Parse a single step definition from a YAML-derived configuration map.
     *
     * The map is expected to come from a parsed "step" entry and may contain keys such as
     * "name", "operator", "delegate", "service", "input", "output", "inputTypeName",
     * "outputTypeName", "operatorMapper", and "externalMapper". The method validates
     * mutual exclusivity, resolves step kind (INTERNAL or DELEGATED), parses class names,
     * and enforces rules about input/output and mapper usage.
     *
     * @param stepData the map containing step configuration data (YAML-derived keys described above)
     * @return a StepDefinition for the parsed step, or null if the step is invalid, unsupported, or should be skipped
     */
    private StepDefinition parseStepDefinition(Map<String, Object> stepData, String basePackage) {
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
        boolean inferredLegacyInternal = isBlank(delegatedClassName) && isBlank(serviceClassName);

        StepKind kind;
        String executionClassName;

        if (!isBlank(delegatedClassName)) {
            kind = StepKind.DELEGATED;
            executionClassName = delegatedClassName;
        } else if (!isBlank(serviceClassName)) {
            kind = StepKind.INTERNAL;
            executionClassName = serviceClassName;
        } else {
            String inferredService = deriveLegacyServiceClassName(basePackage, name);
            if (isBlank(inferredService)) {
                // Legacy template-format steps without basePackage cannot be mapped to an internal service class.
                LOG.debugf("Skipping legacy step '%s' from YAML-driven StepDefinition parsing", name);
                return null;
            }
            kind = StepKind.INTERNAL;
            executionClassName = inferredService;
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
        ClassName inputType = parseOptionalClassName(inputTypeName, name, "input", basePackage, inferredLegacyInternal);
        ClassName outputType = parseOptionalClassName(outputTypeName, name, "output", basePackage, inferredLegacyInternal);
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

        MapperFallbackMode mapperFallback = parseMapperFallback(stepData, name);
        if (mapperFallback == null) {
            return null;
        }

        if (kind == StepKind.INTERNAL && !inferredLegacyInternal) {
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
            if (mapperFallback != MapperFallbackMode.NONE) {
                String message = "Ignoring 'mapperFallback' on internal step '" + name
                    + "'; mapper fallback is only used for delegated steps";
                LOG.warn(message);
                report(Diagnostic.Kind.WARNING, message);
                mapperFallback = MapperFallbackMode.NONE;
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

        return new StepDefinition(
            name,
            kind,
            executionClass,
            externalMapper,
            mapperFallback,
            inputType,
            outputType,
            parseStreamingShapeHint(stepData, name));
    }

    private String deriveLegacyServiceClassName(String basePackage, String stepName) {
        if (isBlank(basePackage) || isBlank(stepName)) {
            return null;
        }
        StringBuilder simpleName = new StringBuilder();
        boolean capitalizeNext = true;
        for (int i = 0; i < stepName.length(); i++) {
            char c = stepName.charAt(i);
            if (Character.isLetterOrDigit(c)) {
                simpleName.append(capitalizeNext ? Character.toUpperCase(c) : c);
                capitalizeNext = false;
            } else {
                capitalizeNext = true;
            }
        }
        if (simpleName.isEmpty()) {
            return null;
        }
        String candidate = simpleName.toString();
        if (!candidate.endsWith("Service")) {
            candidate = candidate + "Service";
        }
        return basePackage + ".service." + candidate;
    }

    /**
     * Reports any unsupported keys present in a step definition and emits a warning.
     *
     * <p>If the step contains keys that are not in the supported set, a warning message
     * listing those keys is logged and forwarded to the diagnostic reporter as
     * Diagnostic.Kind.WARNING. Unsupported keys are ignored by the parser.
     *
     * @param stepName human-readable name of the step (used in the warning message)
     * @param stepData map of key/value pairs from the step definition to inspect
     */
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

    /**
     * Forwards a diagnostic message to the configured diagnostic reporter.
     *
     * @param kind    the diagnostic severity kind
     * @param message the diagnostic message text
     */
    private void report(Diagnostic.Kind kind, String message) {
        diagnosticReporter.accept(kind, message);
    }

    /**
     * Parses a candidate class-name string for a step field.
     *
     * @param typeName   the candidate class-name string (may be fully-qualified or nested)
     * @param stepName   the step's name used for diagnostic messages
     * @param fieldName  the field name (e.g., "input", "output", "operatorMapper") used for diagnostics
     * @return           the parsed ClassName, or `null` if `typeName` is blank or not a valid class name
     */
    private ClassName parseOptionalClassName(
            String typeName,
            String stepName,
            String fieldName,
            String basePackage,
            boolean legacyInternalStep) {
        if (isBlank(typeName)) {
            return null;
        }
        String candidate = typeName;
        if (legacyInternalStep && !typeName.contains(".") && !isBlank(basePackage)) {
            candidate = basePackage + legacyInternalPackageSuffix + typeName;
        }
        ClassName parsed = parseClassName(candidate);
        if (parsed == null) {
            LOG.warnf("Skipping step '%s': invalid %s class name '%s'", stepName, fieldName, typeName);
        }
        return parsed;
    }

    private StreamingShape parseStreamingShapeHint(Map<String, Object> stepData, String stepName) {
        String raw = getStringValue(stepData, "cardinality");
        if (isBlank(raw)) {
            return null;
        }
        String normalized = raw.trim().toUpperCase(java.util.Locale.ROOT);
        return switch (normalized) {
            case "ONE_TO_ONE" -> StreamingShape.UNARY_UNARY;
            case "EXPANSION", "ONE_TO_MANY" -> StreamingShape.UNARY_STREAMING;
            case "MANY_TO_ONE" -> StreamingShape.STREAMING_UNARY;
            case "MANY_TO_MANY" -> StreamingShape.STREAMING_STREAMING;
            default -> {
                LOG.warnf(
                    "Unrecognized cardinality '%s' for step '%s'; default streaming shape inference may apply",
                    raw,
                    stepName);
                yield null;
            }
        };
    }

    private MapperFallbackMode parseMapperFallback(Map<String, Object> stepData, String stepName) {
        String raw = getStringValue(stepData, "mapperFallback");
        if (isBlank(raw)) {
            return MapperFallbackMode.NONE;
        }
        String normalized = raw.trim().toUpperCase(java.util.Locale.ROOT);
        try {
            return MapperFallbackMode.valueOf(normalized);
        } catch (IllegalArgumentException ex) {
            String message = "Skipping step '" + stepName + "': invalid mapperFallback '" + raw
                + "'. Allowed values: NONE, JACKSON";
            LOG.warn(message);
            report(Diagnostic.Kind.ERROR, message);
            return null;
        }
    }

    /**
     * Retrieve a string representation for the given map entry.
     *
     * If the entry is a `String` it is returned; if the entry is non-null and not a `String` its
     * string representation is returned via `String.valueOf(value)`; if the key is absent or maps
     * to `null` this method returns `null`.
     *
     * @param map the map to extract from
     * @param key the key to look up
     * @return the string value for the key, or `null` if the key is absent or maps to `null`; when the
     *         value is non-String, returns `String.valueOf(value)`
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
     * Parse a fully qualified Java class name string into a ClassName representation.
     *
     * Accepts top-level and nested class names (nested segments separated by '$'), validates package and identifier segments,
     * and rejects null, blank, or malformed inputs.
     *
     * @param className the fully qualified class name (e.g. "com.example.Outer$Inner"); may include '$' for nested classes
     * @return the corresponding ClassName, or `null` if the input is null, blank, or not a valid Java class name
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

    /**
     * Determines whether the given string is a valid Java identifier segment.
     *
     * @param segment the string to validate
     * @return `true` if the string is non-empty and satisfies Java identifier start and part character rules, `false` otherwise
     */
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
