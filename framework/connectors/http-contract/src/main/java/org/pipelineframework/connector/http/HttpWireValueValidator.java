package org.pipelineframework.connector.http;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

/** Validates runtime values against the bounded JSON Schema subset retained in an HTTP pin. */
public final class HttpWireValueValidator {
    private static final int MAX_DEPTH = 64;

    private HttpWireValueValidator() {
    }

    public static void validate(JsonNode value, HttpWireSchema schema) {
        validate(value, schema.node(), schema, "$", 0);
    }

    private static void validate(JsonNode value, JsonNode schema, HttpWireSchema owner, String path, int depth) {
        if (depth > MAX_DEPTH) throw new IllegalArgumentException("HTTP wire value exceeds schema depth at " + path);
        if (schema.isBoolean()) {
            if (!schema.booleanValue()) throw new IllegalArgumentException("HTTP wire value is rejected at " + path);
            return;
        }
        alternatives(value, schema, owner, path, depth);
        if (schema.has("const") && !schema.get("const").equals(value)) {
            throw new IllegalArgumentException("HTTP wire value disagrees with const at " + path);
        }
        if (schema.path("enum").isArray()) {
            boolean allowed = false;
            for (JsonNode candidate : schema.path("enum")) allowed |= candidate.equals(value);
            if (!allowed) throw new IllegalArgumentException("HTTP wire value is outside enum at " + path);
        }
        if (!matchesType(value, schema.path("type"))) {
            throw new IllegalArgumentException("HTTP wire value has the wrong type at " + path);
        }
        if (value.isObject()) object(value, schema, owner, path, depth);
        if (value.isArray()) array(value, schema, owner, path, depth);
        if (value.isTextual()) string(value, schema, owner, path);
        if (value.isNumber()) number(value, schema, path);
    }

    private static void alternatives(
        JsonNode value,
        JsonNode schema,
        HttpWireSchema owner,
        String path,
        int depth
    ) {
        if (schema.path("allOf").isArray()) {
            for (JsonNode item : schema.path("allOf")) validate(value, item, owner, path, depth + 1);
        }
        if (schema.path("anyOf").isArray() && matches(value, schema.path("anyOf"), owner, path, depth) < 1) {
            throw new IllegalArgumentException("HTTP wire value matches no anyOf branch at " + path);
        }
        if (schema.path("oneOf").isArray() && matches(value, schema.path("oneOf"), owner, path, depth) != 1) {
            throw new IllegalArgumentException("HTTP wire value must match exactly one oneOf branch at " + path);
        }
    }

    private static int matches(
        JsonNode value,
        JsonNode alternatives,
        HttpWireSchema owner,
        String path,
        int depth
    ) {
        int matches = 0;
        for (JsonNode candidate : alternatives) {
            try {
                validate(value, candidate, owner, path, depth + 1);
                matches++;
            } catch (IllegalArgumentException ignored) {
                // A branch mismatch is expected while evaluating a bounded union.
            }
        }
        return matches;
    }

    private static boolean matchesType(JsonNode value, JsonNode type) {
        if (type.isMissingNode()) return true;
        if (type.isArray()) {
            for (JsonNode candidate : type) {
                if (candidate.isTextual() && matchesType(value, candidate.textValue())) return true;
            }
            return false;
        }
        return type.isTextual() && matchesType(value, type.textValue());
    }

    private static boolean matchesType(JsonNode value, String type) {
        return switch (type) {
            case "null" -> value.isNull();
            case "boolean" -> value.isBoolean();
            case "integer" -> value.isIntegralNumber();
            case "number" -> value.isNumber();
            case "string" -> value.isTextual();
            case "array" -> value.isArray();
            case "object" -> value.isObject();
            default -> false;
        };
    }

    private static void object(JsonNode value, JsonNode schema, HttpWireSchema owner, String path, int depth) {
        JsonNode required = schema.path("required");
        if (required.isArray()) {
            required.forEach(field -> {
                if (!field.isTextual() || !value.has(field.textValue())) {
                    throw new IllegalArgumentException("HTTP wire value is missing required field at "
                        + path + "." + field.asText());
                }
            });
        }
        JsonNode properties = schema.path("properties");
        List<Map.Entry<String, JsonNode>> fields = new ArrayList<>();
        value.fields().forEachRemaining(fields::add);
        for (Map.Entry<String, JsonNode> field : fields) {
            if (properties.isObject() && properties.has(field.getKey())) {
                validate(field.getValue(), properties.get(field.getKey()), owner,
                    path + "." + field.getKey(), depth + 1);
            } else if (schema.path("additionalProperties").isBoolean()
                && !schema.path("additionalProperties").booleanValue()) {
                throw new IllegalArgumentException("HTTP wire value has an undeclared field at "
                    + path + "." + field.getKey());
            } else if (schema.path("additionalProperties").isObject()) {
                validate(field.getValue(), schema.path("additionalProperties"), owner,
                    path + "." + field.getKey(), depth + 1);
            }
        }
    }

    private static void array(JsonNode value, JsonNode schema, HttpWireSchema owner, String path, int depth) {
        int size = value.size();
        if (schema.path("minItems").canConvertToInt() && size < schema.path("minItems").intValue()) {
            throw new IllegalArgumentException("HTTP wire array is shorter than minItems at " + path);
        }
        if (schema.path("maxItems").canConvertToInt() && size > schema.path("maxItems").intValue()) {
            throw new IllegalArgumentException("HTTP wire array exceeds maxItems at " + path);
        }
        JsonNode items = schema.path("items");
        if (items.isObject() || items.isBoolean()) {
            for (int index = 0; index < size; index++) {
                validate(value.get(index), items, owner, path + "[" + index + "]", depth + 1);
            }
        }
    }

    private static void string(JsonNode value, JsonNode schema, HttpWireSchema owner, String path) {
        int length = value.textValue().codePointCount(0, value.textValue().length());
        if (schema.path("minLength").canConvertToInt() && length < schema.path("minLength").intValue()) {
            throw new IllegalArgumentException("HTTP wire string is shorter than minLength at " + path);
        }
        if (schema.path("maxLength").canConvertToInt() && length > schema.path("maxLength").intValue()) {
            throw new IllegalArgumentException("HTTP wire string exceeds maxLength at " + path);
        }
        if (schema.path("pattern").isTextual()) {
            if (!owner.matchesPattern(schema.path("pattern").textValue(), value.textValue())) {
                throw new IllegalArgumentException("HTTP wire string does not match pattern at " + path);
            }
        }
    }

    private static void number(JsonNode value, JsonNode schema, String path) {
        BigDecimal actual = value.decimalValue();
        compare(actual, schema, "minimum", path, comparison -> comparison < 0);
        compare(actual, schema, "exclusiveMinimum", path, comparison -> comparison <= 0);
        compare(actual, schema, "maximum", path, comparison -> comparison > 0);
        compare(actual, schema, "exclusiveMaximum", path, comparison -> comparison >= 0);
    }

    private static void compare(
        BigDecimal actual,
        JsonNode schema,
        String bound,
        String path,
        java.util.function.IntPredicate invalid
    ) {
        if (schema.path(bound).isNumber()
            && invalid.test(actual.compareTo(schema.path(bound).decimalValue()))) {
            throw new IllegalArgumentException("HTTP wire number violates " + bound + " at " + path);
        }
    }
}
