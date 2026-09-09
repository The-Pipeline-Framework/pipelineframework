package org.pipelineframework.connector.http;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** Deterministic tree mapping used by generated HTTP representation mappers. */
public final class HttpOptionMappingSupport {
    private HttpOptionMappingSupport() {
    }

    public static JsonNode toWire(JsonNode canonical, HttpRepresentationMappingOptions options) {
        ObjectNode target = JsonNodeFactory.instance.objectNode();
        copy(canonical, target, options.fields(), false);
        if (options.fields().isEmpty()) merge(target, canonical);
        options.collections().forEach(mapping -> collection(canonical, target, mapping, false));
        options.constants().forEach((path, value) -> set(target, path, value.deepCopy()));
        options.enums().forEach((path, values) -> translate(canonical, target, path,
            options.fields().getOrDefault(path, path), values));
        options.jsonObjects().forEach((canonicalPath, wirePath) -> toJsonObject(canonical, target,
            canonicalPath, wirePath));
        options.discriminator().ifPresent(mapping -> translate(canonical, target, mapping.canonicalPath(),
            mapping.wirePath(), mapping.values()));
        return target;
    }

    public static JsonNode fromWire(JsonNode wire, HttpRepresentationMappingOptions options) {
        ObjectNode target = JsonNodeFactory.instance.objectNode();
        copy(wire, target, options.fields(), true);
        if (options.fields().isEmpty()) merge(target, wire);
        options.collections().forEach(mapping -> collection(wire, target, mapping, true));
        options.enums().forEach((canonicalPath, values) -> translate(wire, target,
            options.fields().getOrDefault(canonicalPath, canonicalPath), canonicalPath, inverse(values)));
        options.jsonObjects().forEach((canonicalPath, wirePath) -> fromJsonObject(wire, target,
            wirePath, canonicalPath));
        options.discriminator().ifPresent(mapping -> translate(wire, target, mapping.wirePath(),
            mapping.canonicalPath(), inverse(mapping.values())));
        return target;
    }

    private static void copy(JsonNode source, ObjectNode target, Map<String, String> fields, boolean reverse) {
        fields.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry -> {
            String from = reverse ? entry.getValue() : entry.getKey();
            String to = reverse ? entry.getKey() : entry.getValue();
            select(source, from).ifPresent(value -> set(target, to, value.deepCopy()));
        });
    }

    private static void collection(
        JsonNode source,
        ObjectNode target,
        HttpRepresentationMappingOptions.CollectionMapping mapping,
        boolean reverse
    ) {
        String from = reverse ? mapping.wirePath() : mapping.canonicalPath();
        String to = reverse ? mapping.canonicalPath() : mapping.wirePath();
        Optional<JsonNode> sourceValue = select(source, from);
        if (sourceValue.isEmpty()) return;
        JsonNode selected = sourceValue.orElseThrow();
        if (!selected.isArray()) throw new IllegalArgumentException("HTTP collection mapping source is not an array: " + from);
        ArrayNode result = JsonNodeFactory.instance.arrayNode();
        selected.forEach(item -> {
            if (!item.isObject()) throw new IllegalArgumentException("HTTP collection mapping elements must be objects");
            ObjectNode mapped = JsonNodeFactory.instance.objectNode();
            if (mapping.fields().isEmpty()) merge(mapped, item);
            else copy(item, mapped, mapping.fields(), reverse);
            result.add(mapped);
        });
        set(target, to, result);
    }

    private static void translate(
        JsonNode source,
        ObjectNode target,
        String sourcePath,
        String targetPath,
        Map<String, String> values
    ) {
        select(source, sourcePath).ifPresent(value -> {
            if (!value.isTextual()) throw new IllegalArgumentException("HTTP enum mapping source must be textual: " + sourcePath);
            String translated = values.get(value.textValue());
            if (translated == null) throw new IllegalArgumentException("HTTP enum mapping has no exact value for: " + value.textValue());
            set(target, targetPath, JsonNodeFactory.instance.textNode(translated));
        });
    }

    private static Map<String, String> inverse(Map<String, String> values) {
        return values.entrySet().stream().collect(java.util.stream.Collectors.toUnmodifiableMap(
            Map.Entry::getValue, Map.Entry::getKey));
    }

    private static void toJsonObject(JsonNode source, ObjectNode target, String sourcePath, String targetPath) {
        select(source, sourcePath).ifPresent(value -> {
            if (!value.isTextual()) {
                throw new IllegalArgumentException("nominal HTTP JSON-object source must be textual: " + sourcePath);
            }
            JsonNode parsed = HttpPinnedJson.parse(value.textValue());
            if (!parsed.isObject()) {
                throw new IllegalArgumentException("nominal HTTP JSON-object source must contain an object: " + sourcePath);
            }
            set(target, targetPath, parsed);
        });
    }

    private static void fromJsonObject(JsonNode source, ObjectNode target, String sourcePath, String targetPath) {
        select(source, sourcePath).ifPresent(value -> {
            if (!value.isObject()) {
                throw new IllegalArgumentException("HTTP JSON-object wire value must be an object: " + sourcePath);
            }
            set(target, targetPath, JsonNodeFactory.instance.textNode(HttpPinnedJson.canonicalize(value)));
        });
    }

    private static Optional<JsonNode> select(JsonNode root, String path) {
        JsonNode current = root;
        for (String field : path.split("\\.")) {
            if (!current.isObject() || !current.has(field)) return Optional.empty();
            current = current.get(field);
        }
        return Optional.of(current);
    }

    private static void set(ObjectNode root, String path, JsonNode value) {
        String[] fields = path.split("\\.");
        ObjectNode current = root;
        for (int index = 0; index < fields.length - 1; index++) {
            JsonNode child = current.get(fields[index]);
            if (child == null) {
                current = current.putObject(fields[index]);
            } else if (child instanceof ObjectNode object) {
                current = object;
            } else {
                throw new IllegalArgumentException("HTTP mapping target path collides with scalar: " + path);
            }
        }
        current.set(fields[fields.length - 1], value);
    }

    private static void merge(ObjectNode target, JsonNode source) {
        if (!source.isObject()) throw new IllegalArgumentException("HTTP mapping root must be an object");
        List<Map.Entry<String, JsonNode>> fields = new ArrayList<>();
        source.fields().forEachRemaining(fields::add);
        fields.stream().sorted(Comparator.comparing(Map.Entry::getKey))
            .forEach(entry -> target.set(entry.getKey(), entry.getValue().deepCopy()));
    }
}
