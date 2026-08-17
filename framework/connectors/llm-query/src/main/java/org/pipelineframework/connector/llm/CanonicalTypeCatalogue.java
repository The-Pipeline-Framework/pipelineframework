package org.pipelineframework.connector.llm;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptorLoader;

/**
 * Runtime view of compiler-emitted canonical v3 type metadata.
 *
 * <p>JSON Schema is only a model-facing projection. Validation is performed against the same
 * canonical metadata, so the projection does not become a second type system.</p>
 */
final class CanonicalTypeCatalogue {
    private static final ObjectMapper JSON = PipelineJson.mapper();
    private final Map<String, TypeBinding> types;

    private CanonicalTypeCatalogue(Map<String, TypeBinding> types) {
        this.types = Map.copyOf(types);
    }

    static CanonicalTypeCatalogue load(ClassLoader classLoader) {
        PipelineContractDescriptor contract = new PipelineContractDescriptorLoader().load(classLoader)
            .orElseThrow(() -> new IllegalStateException(
                "LLM Query requires " + PipelineContractDescriptor.RESOURCE_PATH));
        if (contract.canonicalTypes().isEmpty()) {
            throw new IllegalStateException("LLM Query requires compiler-emitted canonical v3 type metadata");
        }
        Map<String, TypeBinding> bindings = new LinkedHashMap<>();
        contract.canonicalTypes().entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry -> {
            JsonNode binding = JSON.valueToTree(entry.getValue());
            JsonNode definition = binding.path("definition");
            if (!definition.isObject()) {
                throw new IllegalStateException("Canonical type '" + entry.getKey() + "' has no definition");
            }
            bindings.put(entry.getKey(), new TypeBinding(
                entry.getKey(), definition, optionalText(binding, "contributedIdentity")));
        });
        return new CanonicalTypeCatalogue(bindings);
    }

    String schema(String typeName) {
        ObjectNode root = schemaDefinition(requireType(typeName));
        if (!"object".equals(root.path("type").asText())) {
            throw new IllegalStateException("LLM tool argument contract must project as a JSON object: " + typeName);
        }
        ObjectNode definitions = root.putObject("$defs");
        types.keySet().stream().sorted().forEach(name -> definitions.set(name, schemaDefinition(requireType(name))));
        try {
            return JSON.writeValueAsString(root);
        } catch (IOException failure) {
            throw new IllegalStateException("Failed to serialize canonical schema for '" + typeName + "'", failure);
        }
    }

    String validateAndCanonicalize(String typeName, String payload) {
        JsonNode value;
        try {
            value = JSON.readTree(Objects.requireNonNull(payload, "model arguments JSON must not be null"));
        } catch (IOException failure) {
            throw new InvalidModelDecisionException("arguments are not valid JSON", failure);
        }
        validateNamed(typeName, value, "$", new ArrayList<>());
        try {
            return JSON.writeValueAsString(sorted(value));
        } catch (IOException failure) {
            throw new IllegalStateException("Failed to canonicalize model arguments", failure);
        }
    }

    Map<String, String> unionVariants(String unionType) {
        TypeBinding binding = requireType(unionType);
        if (!"union".equals(text(binding.definition(), "kind"))) {
            throw new IllegalStateException("LLM Query output must be a canonical union: " + unionType);
        }
        Map<String, String> variants = new LinkedHashMap<>();
        for (JsonNode variant : binding.definition().path("variants")) {
            variants.put(text(variant, "discriminator"), namedType(variant.path("payload")));
        }
        return Collections.unmodifiableMap(variants);
    }

    Optional<String> contributedIdentity(String typeName) {
        return requireType(typeName).contributedIdentity();
    }

    private ObjectNode schemaDefinition(TypeBinding binding) {
        JsonNode definition = binding.definition();
        return switch (text(definition, "kind")) {
            case "record" -> recordSchema(definition);
            case "wrapper" -> wrapperSchema(definition);
            case "alias" -> referenceSchema(definition.path("target"));
            case "union" -> unionSchema(definition);
            default -> throw new IllegalStateException("Unsupported canonical type kind for '" + binding.name() + "'");
        };
    }

    private ObjectNode recordSchema(JsonNode definition) {
        ObjectNode schema = JSON.createObjectNode();
        schema.put("type", "object");
        ObjectNode properties = schema.putObject("properties");
        ArrayNode required = schema.putArray("required");
        List<JsonNode> fields = new ArrayList<>();
        definition.path("fields").forEach(fields::add);
        fields.stream().sorted(Comparator.comparing(field -> text(field, "name"))).forEach(field -> {
            String name = text(field, "name");
            properties.set(name, referenceSchema(field.path("type")));
            required.add(name);
        });
        schema.put("additionalProperties", false);
        return schema;
    }

    private ObjectNode wrapperSchema(JsonNode definition) {
        ObjectNode schema = referenceSchema(definition.path("wraps"));
        copyConstraint(definition, schema, "minLength");
        copyConstraint(definition, schema, "maxLength");
        copyConstraint(definition, schema, "pattern");
        copyConstraint(definition, schema, "format");
        copyConstraint(definition, schema, "minimum");
        if (definition.has("minimumExclusive")) {
            schema.set("exclusiveMinimum", definition.get("minimumExclusive"));
        }
        copyConstraint(definition, schema, "maximum");
        if (definition.has("maximumExclusive")) {
            schema.set("exclusiveMaximum", definition.get("maximumExclusive"));
        }
        return schema;
    }

    private ObjectNode unionSchema(JsonNode definition) {
        ObjectNode schema = JSON.createObjectNode();
        ArrayNode oneOf = schema.putArray("oneOf");
        for (JsonNode variant : definition.path("variants")) {
            ObjectNode alternative = oneOf.addObject();
            alternative.put("type", "object");
            ObjectNode properties = alternative.putObject("properties");
            properties.putObject("discriminator").put("const", text(variant, "discriminator"));
            properties.set("value", referenceSchema(variant.path("payload")));
            alternative.putArray("required").add("discriminator").add("value");
            alternative.put("additionalProperties", false);
        }
        return schema;
    }

    private ObjectNode referenceSchema(JsonNode expression) {
        return switch (text(expression, "kind")) {
            case "named" -> JSON.createObjectNode().put("$ref", "#/$defs/" + text(expression, "id"));
            case "scalar" -> scalarSchema(text(expression, "id"));
            case "map" -> {
                ObjectNode map = JSON.createObjectNode();
                map.put("type", "object");
                map.set("additionalProperties", referenceSchema(expression.path("value")));
                yield map;
            }
            default -> throw new IllegalStateException("Unsupported canonical type expression: " + expression);
        };
    }

    private ObjectNode scalarSchema(String scalar) {
        ObjectNode schema = JSON.createObjectNode();
        switch (scalar) {
            case "bool" -> schema.put("type", "boolean");
            case "int32", "int64" -> schema.put("type", "integer");
            case "float32", "float64", "decimal" -> schema.put("type", "number");
            case "uuid" -> schema.put("type", "string").put("format", "uuid");
            case "timestamp", "datetime" -> schema.put("type", "string").put("format", "date-time");
            case "date" -> schema.put("type", "string").put("format", "date");
            case "uri" -> schema.put("type", "string").put("format", "uri");
            case "bytes" -> schema.put("type", "string").put("contentEncoding", "base64");
            case "payload_ref" -> schema.put("type", "object");
            default -> schema.put("type", "string");
        }
        return schema;
    }

    private void validateNamed(String typeName, JsonNode value, String path, List<String> stack) {
        if (stack.contains(typeName)) {
            throw invalid(path, "recursive canonical values are not supported by the v1 LLM catalogue");
        }
        List<String> nested = new ArrayList<>(stack);
        nested.add(typeName);
        JsonNode definition = requireType(typeName).definition();
        switch (text(definition, "kind")) {
            case "record" -> validateRecord(definition, value, path, nested);
            case "wrapper" -> validateReference(definition.path("wraps"), value, path, nested);
            case "alias" -> validateReference(definition.path("target"), value, path, nested);
            case "union" -> validateUnion(definition, value, path, nested);
            default -> throw new IllegalStateException("Unsupported canonical type kind: " + definition);
        }
        validateConstraints(definition, value, path);
    }

    private void validateRecord(JsonNode definition, JsonNode value, String path, List<String> stack) {
        if (!value.isObject()) {
            throw invalid(path, "expected object");
        }
        Map<String, JsonNode> fields = new TreeMap<>();
        definition.path("fields").forEach(field -> fields.put(text(field, "name"), field.path("type")));
        Iterator<String> names = value.fieldNames();
        while (names.hasNext()) {
            String name = names.next();
            if (!fields.containsKey(name)) {
                throw invalid(path + "." + name, "unknown field");
            }
        }
        fields.forEach((name, expression) -> {
            JsonNode field = value.get(name);
            if (field == null || field.isNull()) {
                throw invalid(path + "." + name, "missing required field");
            }
            validateReference(expression, field, path + "." + name, stack);
        });
    }

    private void validateUnion(JsonNode definition, JsonNode value, String path, List<String> stack) {
        if (!value.isObject() || !value.path("discriminator").isTextual() || !value.hasNonNull("value")) {
            throw invalid(path, "expected discriminator/value union object");
        }
        String discriminator = value.path("discriminator").textValue();
        for (JsonNode variant : definition.path("variants")) {
            if (discriminator.equals(text(variant, "discriminator"))) {
                validateReference(variant.path("payload"), value.get("value"), path + ".value", stack);
                return;
            }
        }
        throw invalid(path + ".discriminator", "unknown union discriminator '" + discriminator + "'");
    }

    private void validateReference(JsonNode expression, JsonNode value, String path, List<String> stack) {
        switch (text(expression, "kind")) {
            case "named" -> validateNamed(text(expression, "id"), value, path, stack);
            case "scalar" -> validateScalar(text(expression, "id"), value, path);
            case "map" -> {
                if (!value.isObject()) {
                    throw invalid(path, "expected map object");
                }
                value.fields().forEachRemaining(entry ->
                    validateReference(expression.path("value"), entry.getValue(), path + "." + entry.getKey(), stack));
            }
            default -> throw new IllegalStateException("Unsupported canonical type expression: " + expression);
        }
    }

    private void validateScalar(String scalar, JsonNode value, String path) {
        boolean valid = switch (scalar) {
            case "bool" -> value.isBoolean();
            case "int32", "int64" -> value.isIntegralNumber();
            case "float32", "float64", "decimal" -> value.isNumber();
            case "payload_ref" -> value.isObject();
            default -> value.isTextual();
        };
        if (!valid) {
            throw invalid(path, "invalid " + scalar + " value");
        }
        if (!value.isTextual()) {
            return;
        }
        String text = value.textValue();
        try {
            switch (scalar) {
                case "uuid" -> UUID.fromString(text);
                case "timestamp" -> Instant.parse(text);
                case "datetime" -> OffsetDateTime.parse(text);
                case "date" -> LocalDate.parse(text);
                case "duration" -> Duration.parse(text);
                case "uri" -> URI.create(text);
                default -> { }
            }
        } catch (RuntimeException failure) {
            throw invalid(path, "invalid " + scalar + " value");
        }
    }

    private void validateConstraints(JsonNode definition, JsonNode value, String path) {
        if (value.isTextual()) {
            String text = value.textValue();
            if (definition.has("minLength") && text.length() < definition.path("minLength").intValue()) {
                throw invalid(path, "value is shorter than minLength");
            }
            if (definition.has("maxLength") && text.length() > definition.path("maxLength").intValue()) {
                throw invalid(path, "value is longer than maxLength");
            }
            if (definition.has("pattern") && !Pattern.compile(definition.path("pattern").textValue()).matcher(text).matches()) {
                throw invalid(path, "value does not match pattern");
            }
        }
        if (value.isNumber()) {
            BigDecimal number = value.decimalValue();
            compareBound(definition, "minimum", number, path, false, true);
            compareBound(definition, "minimumExclusive", number, path, true, true);
            compareBound(definition, "maximum", number, path, false, false);
            compareBound(definition, "maximumExclusive", number, path, true, false);
        }
    }

    private void compareBound(JsonNode definition, String field, BigDecimal value, String path, boolean exclusive, boolean lower) {
        if (!definition.has(field)) {
            return;
        }
        int comparison = value.compareTo(definition.path(field).decimalValue());
        boolean invalid = lower ? comparison < 0 || exclusive && comparison == 0 : comparison > 0 || exclusive && comparison == 0;
        if (invalid) {
            throw invalid(path, "value violates " + field);
        }
    }

    private static JsonNode sorted(JsonNode value) {
        if (value.isObject()) {
            ObjectNode sorted = JSON.createObjectNode();
            List<String> names = new ArrayList<>();
            value.fieldNames().forEachRemaining(names::add);
            names.stream().sorted().forEach(name -> sorted.set(name, sorted(value.get(name))));
            return sorted;
        }
        if (value.isArray()) {
            ArrayNode sorted = JSON.createArrayNode();
            value.forEach(item -> sorted.add(sorted(item)));
            return sorted;
        }
        return value;
    }

    private TypeBinding requireType(String name) {
        TypeBinding binding = types.get(name);
        if (binding == null) {
            throw new IllegalStateException("Unknown canonical type '" + name + "'");
        }
        return binding;
    }

    private static String namedType(JsonNode expression) {
        if (!"named".equals(text(expression, "kind"))) {
            throw new IllegalStateException("LLM decision variants must reference named payload types");
        }
        return text(expression, "id");
    }

    private static void copyConstraint(JsonNode source, ObjectNode target, String name) {
        if (source.has(name)) {
            target.set(name, source.get(name));
        }
    }

    private static Optional<String> optionalText(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value == null || !value.isTextual() ? Optional.empty() : Optional.of(value.textValue());
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || !value.isTextual() || value.textValue().isBlank()) {
            throw new IllegalStateException("Canonical metadata field '" + field + "' must be non-blank text");
        }
        return value.textValue();
    }

    private static InvalidModelDecisionException invalid(String path, String reason) {
        return new InvalidModelDecisionException("invalid model decision at " + path + ": " + reason);
    }

    private record TypeBinding(String name, JsonNode definition, Optional<String> contributedIdentity) {
        private TypeBinding {
            Objects.requireNonNull(name, "canonical type name must not be null");
            Objects.requireNonNull(definition, "canonical type definition must not be null");
            Objects.requireNonNull(contributedIdentity, "contributed identity must not be null");
        }
    }
}
