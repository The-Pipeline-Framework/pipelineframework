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
import org.pipelineframework.config.template.PipelineTemplateRemoteTarget;
import org.pipelineframework.config.template.PipelineTemplateStepExecution;
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
        "inboundMapper",
        "outboundMapper",
        "operatorMapper",
        "externalMapper",
        "mapperFallback",
        "cardinality",
        "inputTypeName",
        "inputFields",
        "outputTypeName",
        "outputFields",
        "execution");
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
        int version = parseVersion(templateData);

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
            StepDefinition stepDef = parseStepDefinition(stepData, basePackage, version);
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
    private StepDefinition parseStepDefinition(Map<String, Object> stepData, String basePackage, int version) {
        String name = getStringValue(stepData, "name");
        if (isBlank(name)) {
            LOG.warnf("Skipping step with null or blank name: %s", stepData);
            return null;
        }
        reportUnknownStepKeys(name, stepData);

        PipelineTemplateStepExecution remoteExecution;
        try {
            remoteExecution = parseRemoteExecution(stepData, name, version);
        } catch (IllegalArgumentException ex) {
            return null;
        }

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
        if (remoteExecution != null && (!isBlank(delegatedClassName) || !isBlank(serviceClassName))) {
            String message = "Skipping step '" + name
                + "': remote execution is mutually exclusive with 'service', 'operator', and 'delegate'";
            LOG.warn(message);
            report(Diagnostic.Kind.ERROR, message);
            return null;
        }
        boolean inferredLegacyInternal = isBlank(delegatedClassName) && isBlank(serviceClassName);

        StepKind kind;
        String executionClassName;

        if (remoteExecution != null) {
            kind = StepKind.REMOTE;
            executionClassName = null;
        } else if (!isBlank(delegatedClassName)) {
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

        String inboundMapperName = getStringValue(stepData, "inboundMapper");
        String outboundMapperName = getStringValue(stepData, "outboundMapper");
        ClassName inboundMapper = parseOptionalStepMapper(inboundMapperName, name, "inboundMapper");
        if (!isBlank(inboundMapperName) && inboundMapper == null) {
            return null;
        }
        ClassName outboundMapper = parseOptionalStepMapper(outboundMapperName, name, "outboundMapper");
        if (!isBlank(outboundMapperName) && outboundMapper == null) {
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
                String message = "Skipping step '" + name + "': invalid "
                    + (!isBlank(operatorMapperName) ? "operatorMapper" : "externalMapper")
                    + " class name '" + effectiveMapperName + "'";
                LOG.warn(message);
                report(Diagnostic.Kind.ERROR, message);
                return null;
            }
        }

        MapperFallbackMode mapperFallback = parseMapperFallback(stepData, name);
        if (mapperFallback == null) {
            return null;
        }

        if (kind == StepKind.INTERNAL && !inferredLegacyInternal) {
            if (externalMapper != null) {
                String message = "Skipping step '" + name
                    + "': 'operatorMapper'/'externalMapper' are only valid for delegated steps; use 'inboundMapper'/'outboundMapper' for internal service steps";
                LOG.warn(message);
                report(Diagnostic.Kind.ERROR, message);
                return null;
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
            if (inboundMapper != null || outboundMapper != null) {
                String message = "Skipping step '" + name
                    + "': delegated steps cannot declare 'inboundMapper'/'outboundMapper'; use 'operatorMapper' for delegated mapping";
                LOG.warn(message);
                report(Diagnostic.Kind.ERROR, message);
                return null;
            }
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

        if (kind == StepKind.REMOTE) {
            if (inboundMapper != null || outboundMapper != null) {
                String message = "Skipping step '" + name
                    + "': remote execution cannot be combined with inboundMapper/outboundMapper";
                LOG.warn(message);
                report(Diagnostic.Kind.ERROR, message);
                return null;
            }
            if (inputType == null || outputType == null) {
                String message = "Skipping step '" + name
                    + "': remote steps must provide inputTypeName and outputTypeName";
                LOG.warn(message);
                report(Diagnostic.Kind.ERROR, message);
                return null;
            }
            StreamingShape shape = parseStreamingShapeHint(stepData, name);
            if (shape != null && shape != StreamingShape.UNARY_UNARY) {
                String message = "Skipping step '" + name
                    + "': remote execution currently supports only ONE_TO_ONE cardinality";
                LOG.warn(message);
                report(Diagnostic.Kind.ERROR, message);
                return null;
            }
            if (!isBlank(operatorMapperName) || !isBlank(externalMapperName) || mapperFallback != MapperFallbackMode.NONE) {
                String message = "Skipping step '" + name
                    + "': remote execution cannot be combined with operatorMapper/externalMapper/mapperFallback";
                LOG.warn(message);
                report(Diagnostic.Kind.ERROR, message);
                return null;
            }
            return new StepDefinition(
                name,
                kind,
                null,
                remoteExecution,
                null,
                null,
                null,
                MapperFallbackMode.NONE,
                inputType,
                outputType,
                StreamingShape.UNARY_UNARY);
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
            inboundMapper,
            outboundMapper,
            externalMapper,
            mapperFallback,
            inputType,
            outputType,
            parseStreamingShapeHint(stepData, name));
    }

    private ClassName parseOptionalStepMapper(String mapperName, String stepName, String fieldName) {
        if (isBlank(mapperName)) {
            return null;
        }
        ClassName mapper = parseClassName(mapperName);
        if (mapper == null) {
            String message = "Invalid " + fieldName + " class name for step '" + stepName + "': " + fieldName + " = '" + mapperName + "'";
            LOG.warnf("Skipping step '%s': invalid %s class name '%s'", stepName, fieldName, mapperName);
            report(Diagnostic.Kind.ERROR, message);
        }
        return mapper;
    }

    private PipelineTemplateStepExecution parseRemoteExecution(Map<String, Object> stepData, String stepName, int version) {
        Object executionObj = stepData.get("execution");
        if (executionObj == null) {
            return null;
        }
        if (version < 2) {
            String message = "Skipping step '" + stepName + "': execution blocks require version: 2";
            LOG.warn(message);
            report(Diagnostic.Kind.ERROR, message);
            throw new IllegalArgumentException(message);
        }
        if (!(executionObj instanceof Map<?, ?> rawExecutionMap)) {
            String message = "Skipping step '" + stepName + "': execution block must be a map";
            LOG.warn(message);
            report(Diagnostic.Kind.ERROR, message);
            throw new IllegalArgumentException(message);
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> executionMap = (Map<String, Object>) rawExecutionMap;
        PipelineTemplateRemoteTarget target = parseRemoteTarget(executionMap.get("target"));
        PipelineTemplateStepExecution execution = new PipelineTemplateStepExecution(
            getStringValue(executionMap, "mode"),
            getStringValue(executionMap, "operatorId"),
            getStringValue(executionMap, "protocol"),
            parseOptionalPositiveInteger(executionMap.get("timeoutMs"), stepName, "execution.timeoutMs"),
            target);
        validateRemoteExecution(stepName, execution);
        return execution;
    }

    private PipelineTemplateRemoteTarget parseRemoteTarget(Object targetObj) {
        if (targetObj != null && !(targetObj instanceof Map<?, ?>)) {
            LOG.warnf("Ignoring invalid remote target block of type %s: %s",
                targetObj.getClass().getName(), targetObj);
            return null;
        }
        if (!(targetObj instanceof Map<?, ?> rawTargetMap)) {
            return null;
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> targetMap = (Map<String, Object>) rawTargetMap;
        return new PipelineTemplateRemoteTarget(
            getStringValue(targetMap, "url"),
            getStringValue(targetMap, "urlConfigKey"));
    }

    private void validateRemoteExecution(String stepName, PipelineTemplateStepExecution execution) {
        if (execution == null) {
            return;
        }
        if (!execution.isRemote()) {
            String message = "Skipping step '" + stepName
                + "': invalid execution.mode '" + execution.mode() + "'; expected REMOTE";
            LOG.warn(message);
            report(Diagnostic.Kind.ERROR, message);
            throw new IllegalArgumentException(message);
        }
        if (isBlank(execution.operatorId())) {
            String message = "Skipping step '" + stepName + "': remote execution requires execution.operatorId";
            LOG.warn(message);
            report(Diagnostic.Kind.ERROR, message);
            throw new IllegalArgumentException(message);
        }
        if (!"PROTOBUF_HTTP_V1".equalsIgnoreCase(execution.protocol())) {
            String message = "Skipping step '" + stepName
                + "': remote execution requires execution.protocol=PROTOBUF_HTTP_V1";
            LOG.warn(message);
            report(Diagnostic.Kind.ERROR, message);
            throw new IllegalArgumentException(message);
        }
        PipelineTemplateRemoteTarget target = execution.target();
        boolean hasUrl = target != null && !isBlank(target.url());
        boolean hasConfigKey = target != null && !isBlank(target.urlConfigKey());
        if (hasUrl == hasConfigKey) {
            String message = "Skipping step '" + stepName
                + "': remote execution requires exactly one of execution.target.url or execution.target.urlConfigKey";
            LOG.warn(message);
            report(Diagnostic.Kind.ERROR, message);
            throw new IllegalArgumentException(message);
        }
    }

    private Integer parseOptionalPositiveInteger(Object rawValue, String stepName, String fieldName) {
        if (rawValue == null) {
            return null;
        }
        try {
            long parsed;
            if (rawValue instanceof Number number) {
                double value = number.doubleValue();
                if (value != Math.rint(value)) {
                    String message = "Skipping step '" + stepName + "': " + fieldName
                        + " must be a whole integer value, got '" + rawValue + "'";
                    LOG.warn(message);
                    report(Diagnostic.Kind.ERROR, message);
                    throw new IllegalArgumentException(message);
                }
                parsed = number.longValue();
            } else {
                parsed = Long.parseLong(String.valueOf(rawValue).trim());
            }
            if (parsed > Integer.MAX_VALUE) {
                String message = "Skipping step '" + stepName + "': invalid integer value for " + fieldName
                    + " -> '" + rawValue + "'";
                LOG.warn(message);
                report(Diagnostic.Kind.ERROR, message);
                throw new IllegalArgumentException(message);
            }
            if (parsed <= 0) {
                String message = "Skipping step '" + stepName + "': " + fieldName + " must be > 0";
                LOG.warn(message);
                report(Diagnostic.Kind.ERROR, message);
                throw new IllegalArgumentException(message);
            }
            return (int) parsed;
        } catch (NumberFormatException ex) {
            String message = "Skipping step '" + stepName + "': invalid integer value for " + fieldName
                + " -> '" + rawValue + "'";
            LOG.warn(message);
            report(Diagnostic.Kind.ERROR, message);
            throw new IllegalArgumentException(message, ex);
        }
    }

    private int parseVersion(Map<String, Object> templateData) {
        Object rawVersion = templateData.get("version");
        if (rawVersion == null) {
            return 1;
        }
        if (rawVersion instanceof Number number) {
            double value = number.doubleValue();
            if (value != Math.rint(value)) {
                String message = "Invalid template version: '" + rawVersion + "'";
                LOG.warn(message);
                report(Diagnostic.Kind.ERROR, message);
                throw new IllegalArgumentException(message);
            }
            return (int) value;
        }
        if (rawVersion instanceof String text) {
            try {
                return Integer.parseInt(text.trim());
            } catch (NumberFormatException ignored) {
                // fall through
            }
        }
        String message = "Invalid template version: '" + rawVersion + "'";
        LOG.warn(message);
        report(Diagnostic.Kind.ERROR, message);
        throw new IllegalArgumentException(message);
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
            case "MANY_TO_ONE", "REDUCTION" -> StreamingShape.STREAMING_UNARY;
            case "SIDE_EFFECT" -> StreamingShape.UNARY_UNARY;
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