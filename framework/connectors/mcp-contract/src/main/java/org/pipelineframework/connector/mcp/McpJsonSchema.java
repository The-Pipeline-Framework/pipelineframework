package org.pipelineframework.connector.mcp;

import java.math.BigDecimal;
import java.util.Set;
import com.google.re2j.Pattern;

import com.fasterxml.jackson.databind.JsonNode;

/** Immutable external schema pin and bounded validator for the importer-v1 schema dialect. */
public record McpJsonSchema(String json, java.util.List<String> includeFields) {
    private static final Set<String> KEYWORDS = Set.of(
        "$schema", "title", "description", "default", "examples", "deprecated", "readOnly", "writeOnly",
        "type", "properties", "required", "additionalProperties", "items", "minItems", "maxItems",
        "minLength", "maxLength", "pattern", "format", "minimum", "maximum", "exclusiveMinimum",
        "exclusiveMaximum", "enum", "const");
    private static final Set<String> TYPES = Set.of("object", "array", "string", "integer", "number", "boolean", "null");
    private static final Set<String> FORMATS = Set.of(
        "email", "uuid", "date-time", "date", "duration", "uri", "int32", "int64", "float", "double", "decimal");

    public McpJsonSchema {
        JsonNode schema = McpPinnedJson.parse(json);
        includeFields = new McpInputSelection(includeFields).includeFields();
        checkSchema(project(schema, includeFields), "$", true);
        json = McpPinnedJson.canonicalize(schema);
    }

    public McpJsonSchema(String json) {
        this(json, java.util.List.of());
    }

    private static JsonNode project(JsonNode schema, java.util.List<String> includeFields) {
        if (includeFields.isEmpty()) {
            return schema;
        }
        var mapper = new com.fasterxml.jackson.databind.ObjectMapper()
            .enable(com.fasterxml.jackson.databind.DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .enable(com.fasterxml.jackson.databind.DeserializationFeature.USE_BIG_INTEGER_FOR_INTS);
        java.util.Map<String, Object> original = mapper.convertValue(schema,
            new com.fasterxml.jackson.core.type.TypeReference<java.util.Map<String, Object>>() {});
        return mapper.valueToTree(new McpInputSelection(includeFields).project(original));
    }

    public String sha256() {
        return McpPinnedJson.sha256(json);
    }

    public JsonNode node() {
        return McpPinnedJson.parse(json);
    }

    public void validateArguments(JsonNode value) {
        JsonNode bounded = McpPinnedJson.parse(McpPinnedJson.canonicalize(value));
        validate(project(node(), includeFields), bounded, "$", new int[] {0});
    }

    private static void checkSchema(JsonNode schema, String path, boolean root) {
        require(schema.isObject(), path, "schema must be an object");
        schema.fieldNames().forEachRemaining(key -> require(KEYWORDS.contains(key), path + "." + key,
            "unsupported pinned schema keyword; references and composition are not supported by importer v1"));
        JsonNode type = schema.path("type");
        require(type.isTextual() && TYPES.contains(type.textValue())
            || type.isArray() && type.size() == 2 && type.get(0).isTextual() && type.get(1).isTextual()
                && !type.get(0).equals(type.get(1)) && (type.get(0).asText().equals("null")
                    || type.get(1).asText().equals("null"))
                && TYPES.contains(type.get(0).asText()) && TYPES.contains(type.get(1).asText()),
            path, "unsupported type");
        if (root) {
            require(type.isTextual() && type.asText().equals("object"), path, "root must be a non-null object");
        }
        if (schema.has("$schema")) {
            require(Set.of("https://json-schema.org/draft/2020-12/schema", "https://json-schema.org/draft/2019-09/schema",
                "http://json-schema.org/draft-07/schema#").contains(schema.path("$schema").asText()), path,
                "unsupported schema dialect");
        }
        if (schema.has("properties")) {
            require(schema.get("properties").isObject(), path, "properties must be an object");
            schema.get("properties").fields().forEachRemaining(entry ->
                checkSchema(entry.getValue(), path + ".properties." + entry.getKey(), false));
        }
        if (schema.has("additionalProperties")) {
            require(schema.get("additionalProperties").isBoolean(), path, "additionalProperties must be boolean");
        }
        if (schema.has("required")) {
            require(schema.get("required").isArray(), path, "required must be an array");
            var names = new java.util.HashSet<String>();
            schema.get("required").forEach(name -> require(name.isTextual() && names.add(name.textValue()), path,
                "required must contain unique strings"));
        }
        if (schema.has("items")) {
            checkSchema(schema.get("items"), path + ".items", false);
        }
        for (String bound : Set.of("minItems", "maxItems", "minLength", "maxLength")) {
            if (schema.has(bound)) {
                require(schema.get(bound).isIntegralNumber() && schema.get(bound).canConvertToInt()
                    && schema.get(bound).intValue() >= 0, path + "." + bound, "must be a non-negative integer");
            }
        }
        for (String bound : Set.of("minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum")) {
            if (schema.has(bound)) {
                require(schema.get(bound).isNumber(), path + "." + bound, "must be numeric");
            }
        }
        if (schema.has("pattern")) {
            require(schema.get("pattern").isTextual(), path, "pattern must be a string");
            require(schema.get("pattern").textValue().length() <= 4096, path, "pattern exceeds size limit");
            try {
                require(Pattern.compile(schema.get("pattern").textValue()).programSize() <= 4096, path,
                    "pattern exceeds program limit");
            } catch (com.google.re2j.PatternSyntaxException failure) {
                throw new IllegalArgumentException("pinned MCP schema " + path + ": unsupported pattern", failure);
            }
        }
        if (schema.has("format")) {
            require(schema.get("format").isTextual() && FORMATS.contains(schema.get("format").textValue()), path,
                "unsupported format");
        }
        if (schema.has("enum")) {
            require(schema.get("enum").isArray() && !schema.get("enum").isEmpty(), path, "enum must be nonempty");
        }
    }

    private static void validate(JsonNode schema, JsonNode value, String path, int[] work) {
        require(++work[0] <= 20_000, path, "validation work limit exceeded");
        JsonNode type = schema.path("type");
        boolean matches = type.isArray()
            ? java.util.stream.StreamSupport.stream(type.spliterator(), false).anyMatch(t -> matches(t.asText(), value))
            : matches(type.asText(), value);
        require(matches, path, "type mismatch");
        if (schema.has("const")) {
            require(schema.get("const").equals(value), path, "const mismatch");
        }
        if (schema.has("enum")) {
            require(java.util.stream.StreamSupport.stream(schema.get("enum").spliterator(), false)
                .anyMatch(value::equals), path, "enum mismatch");
        }
        if (value.isObject()) {
            schema.path("required").forEach(name -> require(value.has(name.textValue()), path, "required field missing"));
            value.fields().forEachRemaining(entry -> {
                JsonNode property = schema.path("properties").path(entry.getKey());
                if (property.isMissingNode()) {
                    require(!schema.path("additionalProperties").isBoolean()
                        || schema.path("additionalProperties").booleanValue(), path, "unknown property");
                } else {
                    validate(property, entry.getValue(), path + "." + entry.getKey(), work);
                }
            });
        } else if (value.isArray()) {
            size(schema, value.size(), "minItems", "maxItems", path);
            if (schema.has("items")) {
                for (int i = 0; i < value.size(); i++) {
                    validate(schema.get("items"), value.get(i), path + "[" + i + "]", work);
                }
            }
        } else if (value.isTextual()) {
            String text = value.textValue();
            size(schema, text.codePointCount(0, text.length()), "minLength", "maxLength", path);
            if (schema.has("pattern")) {
                require(Pattern.compile(schema.get("pattern").textValue()).matcher(text).find(), path, "pattern mismatch");
            }
            if (schema.has("format")) {
                format(schema.get("format").textValue(), text, path);
            }
        } else if (value.isNumber()) {
            BigDecimal number = value.decimalValue();
            for (String bound : Set.of("minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum")) {
                if (schema.has(bound)) {
                    int comparison = number.compareTo(schema.get(bound).decimalValue());
                    boolean valid = switch (bound) {
                        case "minimum" -> comparison >= 0;
                        case "maximum" -> comparison <= 0;
                        case "exclusiveMinimum" -> comparison > 0;
                        default -> comparison < 0;
                    };
                    require(valid, path, "numeric bound violated");
                }
            }
        }
    }

    private static boolean matches(String type, JsonNode value) {
        return switch (type) {
            case "object" -> value.isObject();
            case "array" -> value.isArray();
            case "string" -> value.isTextual();
            case "boolean" -> value.isBoolean();
            case "number" -> value.isNumber();
            case "integer" -> value.isNumber() && value.decimalValue().stripTrailingZeros().scale() <= 0;
            case "null" -> value.isNull();
            default -> false;
        };
    }

    private static void size(JsonNode schema, int size, String min, String max, String path) {
        require(!schema.has(min) || size >= schema.get(min).intValue(), path, "minimum size violated");
        require(!schema.has(max) || size <= schema.get(max).intValue(), path, "maximum size violated");
    }

    private static void format(String format, String value, String path) {
        try {
            switch (format) {
                case "uuid" -> java.util.UUID.fromString(value);
                case "date-time" -> java.time.OffsetDateTime.parse(value);
                case "date" -> java.time.LocalDate.parse(value);
                case "duration" -> java.time.Duration.parse(value);
                case "uri" -> require(java.net.URI.create(value).isAbsolute(), path, "URI must be absolute");
                case "email" -> require(Pattern.compile("^[^\\s@]+@[^\\s@]+\\.[^\\s@]+$")
                    .matcher(value).matches(), path, "email mismatch");
                default -> { /* Numeric formats are annotations; canonical validation owns scalar ranges. */ }
            }
        } catch (RuntimeException failure) {
            throw new IllegalArgumentException("pinned MCP input " + path + " format mismatch");
        }
    }

    private static void require(boolean condition, String path, String reason) {
        if (!condition) {
            throw new IllegalArgumentException("pinned MCP schema " + path + ": " + reason);
        }
    }
}
