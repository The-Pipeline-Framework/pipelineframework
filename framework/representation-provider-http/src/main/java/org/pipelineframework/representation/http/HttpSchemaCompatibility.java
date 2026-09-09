package org.pipelineframework.representation.http;

import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import org.pipelineframework.connector.http.HttpPinnedJson;
import org.pipelineframework.connector.http.HttpRepresentationMappingOptions;

/** Deterministic structural validation for the bounded HTTP mapping option language. */
final class HttpSchemaCompatibility {
    private HttpSchemaCompatibility() {
    }

    static boolean equivalent(String canonicalSchema, String wireSchema) {
        return normalized(canonicalSchema).equals(normalized(wireSchema));
    }

    static void validate(
        String canonicalSchema,
        String wireSchema,
        HttpRepresentationMappingOptions options
    ) {
        JsonNode canonical = HttpPinnedJson.parse(canonicalSchema);
        JsonNode wire = HttpPinnedJson.parse(wireSchema);
        options.fields().forEach((canonicalPath, wirePath) -> compatible(
            at(canonical, canonicalPath), at(wire, wirePath), canonicalPath + " -> " + wirePath));
        options.constants().forEach((wirePath, value) -> scalarCompatible(at(wire, wirePath), value, wirePath));
        options.enums().forEach((canonicalPath, values) -> {
            String wirePath = options.fields().getOrDefault(canonicalPath, canonicalPath);
            requireType(at(canonical, canonicalPath), "string", canonicalPath);
            requireType(at(wire, wirePath), "string", wirePath);
        });
        options.jsonObjects().forEach((canonicalPath, wirePath) -> {
            requireType(at(canonical, canonicalPath), "string", canonicalPath);
            requireType(at(wire, wirePath), "object", wirePath);
        });
        options.collections().forEach(collection -> {
            JsonNode canonicalItems = items(at(canonical, collection.canonicalPath()), collection.canonicalPath());
            JsonNode wireItems = items(at(wire, collection.wirePath()), collection.wirePath());
            collection.fields().forEach((canonicalPath, wirePath) -> compatible(
                at(canonicalItems, canonicalPath), at(wireItems, wirePath),
                collection.canonicalPath() + "." + canonicalPath + " -> "
                    + collection.wirePath() + "." + wirePath));
        });
        options.discriminator().ifPresent(discriminator -> {
            requireType(at(canonical, discriminator.canonicalPath()), "string", discriminator.canonicalPath());
            requireType(at(wire, discriminator.wirePath()), "string", discriminator.wirePath());
        });
    }

    private static JsonNode normalized(String value) {
        JsonNode node = HttpPinnedJson.parse(value).deepCopy();
        if (node.isObject()) {
            ((com.fasterxml.jackson.databind.node.ObjectNode) node).remove(
                java.util.List.of("$schema", "title", "description", "examples"));
        }
        return node;
    }

    private static JsonNode at(JsonNode schema, String path) {
        JsonNode current = schema;
        for (String field : path.split("\\.")) {
            JsonNode properties = current.path("properties");
            if (!properties.isObject() || !properties.has(field)) {
                throw new IllegalArgumentException("HTTP mapping path is absent from normalized schema: " + path);
            }
            current = properties.get(field);
        }
        return current;
    }

    private static JsonNode items(JsonNode schema, String path) {
        requireType(schema, "array", path);
        JsonNode items = schema.path("items");
        if (!items.isObject()) throw new IllegalArgumentException("HTTP collection path has no bounded item schema: " + path);
        return items;
    }

    private static void compatible(JsonNode canonical, JsonNode wire, String subject) {
        String canonicalType = canonical.path("type").asText();
        String wireType = wire.path("type").asText();
        if (canonicalType.isBlank() || !canonicalType.equals(wireType)) {
            throw new IllegalArgumentException("HTTP mapping path types disagree for " + subject
                + ": " + canonicalType + " vs " + wireType);
        }
    }

    private static void scalarCompatible(JsonNode schema, JsonNode value, String path) {
        String expected = schema.path("type").asText();
        boolean matches = switch (expected) {
            case "string" -> value.isTextual();
            case "integer" -> value.isIntegralNumber();
            case "number" -> value.isNumber();
            case "boolean" -> value.isBoolean();
            case "null" -> value.isNull();
            default -> false;
        };
        if (!matches) throw new IllegalArgumentException("HTTP mapping constant disagrees with wire schema at " + path);
    }

    private static void requireType(JsonNode schema, String expected, String path) {
        if (!expected.equals(schema.path("type").asText())) {
            throw new IllegalArgumentException("HTTP mapping path '" + path + "' must have type " + expected);
        }
    }
}
