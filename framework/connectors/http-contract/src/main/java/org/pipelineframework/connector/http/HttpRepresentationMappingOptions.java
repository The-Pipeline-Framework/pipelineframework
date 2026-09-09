package org.pipelineframework.connector.http;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Bounded deterministic option language shared by assisted and curated HTTP mappings. */
public record HttpRepresentationMappingOptions(
    Map<String, String> fields,
    Map<String, JsonNode> constants,
    Map<String, Map<String, String>> enums,
    Map<String, String> jsonObjects,
    List<CollectionMapping> collections,
    Optional<DiscriminatorMapping> discriminator
) {
    private static final ObjectMapper JSON = new ObjectMapper();

    public HttpRepresentationMappingOptions {
        fields = paths(fields, "HTTP field mapping");
        constants = constants(constants);
        enums = enums(enums);
        jsonObjects = paths(jsonObjects, "HTTP nominal JSON-object mapping");
        collections = List.copyOf(Objects.requireNonNull(collections,
            "HTTP collection mappings must not be null"));
        if (collections.size() > 32) throw new IllegalArgumentException("HTTP collection mappings exceed limit");
        discriminator = Objects.requireNonNull(discriminator, "HTTP discriminator mapping must not be null");
        if (fields.isEmpty() && hasRenamedSupplementalPath(jsonObjects, collections, discriminator)) {
            throw new IllegalArgumentException(
                "renamed HTTP collection, JSON-object, or discriminator mappings require explicit field mappings");
        }
    }

    public static HttpRepresentationMappingOptions empty() {
        return new HttpRepresentationMappingOptions(Map.of(), Map.of(), Map.of(), Map.of(), List.of(), Optional.empty());
    }

    public static HttpRepresentationMappingOptions from(Map<String, Object> source) {
        Objects.requireNonNull(source, "HTTP representation options must not be null");
        JsonNode root = JSON.valueToTree(source);
        if (!root.isObject()) throw new IllegalArgumentException("HTTP representation options must be an object");
        root.fieldNames().forEachRemaining(field -> {
            if (!List.of("fields", "constants", "enums", "jsonObjects", "collections", "discriminator")
                .contains(field)) {
                throw new IllegalArgumentException("unsupported HTTP representation option '" + field + "'");
            }
        });
        Map<String, String> fields = stringMap(root.path("fields"), "HTTP field mappings");
        Map<String, JsonNode> constants = new LinkedHashMap<>();
        if (!root.path("constants").isMissingNode()) {
            requireObject(root.path("constants"), "HTTP constants").fields().forEachRemaining(entry -> {
                if (!entry.getValue().isValueNode()) {
                    throw new IllegalArgumentException("HTTP mapping constants must be JSON scalar values");
                }
                constants.put(path(entry.getKey(), "HTTP constant target"), entry.getValue().deepCopy());
            });
        }
        Map<String, Map<String, String>> enums = new LinkedHashMap<>();
        if (!root.path("enums").isMissingNode()) {
            requireObject(root.path("enums"), "HTTP enum mappings").fields().forEachRemaining(entry ->
                enums.put(path(entry.getKey(), "HTTP enum path"),
                    stringMap(entry.getValue(), "HTTP enum translation")));
        }
        Map<String, String> jsonObjects = stringMap(root.path("jsonObjects"), "HTTP nominal JSON-object mappings");
        List<CollectionMapping> collections = new java.util.ArrayList<>();
        if (!root.path("collections").isMissingNode()) {
            JsonNode array = root.path("collections");
            if (!array.isArray()) throw new IllegalArgumentException("HTTP collection mappings must be an array");
            array.forEach(node -> {
                requireObject(node, "HTTP collection mapping");
                node.fieldNames().forEachRemaining(field -> {
                    if (!List.of("canonicalPath", "wirePath", "fields").contains(field)) {
                        throw new IllegalArgumentException("unsupported HTTP collection mapping field '" + field + "'");
                    }
                });
                collections.add(new CollectionMapping(required(node, "canonicalPath"), required(node, "wirePath"),
                    stringMap(node.path("fields"), "HTTP collection field mappings")));
            });
        }
        Optional<DiscriminatorMapping> discriminator = Optional.empty();
        if (!root.path("discriminator").isMissingNode()) {
            JsonNode node = requireObject(root.path("discriminator"), "HTTP discriminator mapping");
            node.fieldNames().forEachRemaining(field -> {
                if (!List.of("canonicalPath", "wirePath", "values").contains(field)) {
                    throw new IllegalArgumentException("unsupported HTTP discriminator mapping field '" + field + "'");
                }
            });
            discriminator = Optional.of(new DiscriminatorMapping(required(node, "canonicalPath"),
                required(node, "wirePath"), stringMap(node.path("values"), "HTTP discriminator values")));
        }
        return new HttpRepresentationMappingOptions(fields, constants, enums, jsonObjects, collections, discriminator);
    }

    public record CollectionMapping(String canonicalPath, String wirePath, Map<String, String> fields) {
        public CollectionMapping {
            canonicalPath = path(canonicalPath, "HTTP collection canonical path");
            wirePath = path(wirePath, "HTTP collection wire path");
            fields = paths(fields, "HTTP collection field mapping");
        }
    }

    public record DiscriminatorMapping(String canonicalPath, String wirePath, Map<String, String> values) {
        public DiscriminatorMapping {
            canonicalPath = path(canonicalPath, "HTTP discriminator canonical path");
            wirePath = path(wirePath, "HTTP discriminator wire path");
            values = translation(values, "HTTP discriminator values");
        }
    }

    private static Map<String, String> paths(Map<String, String> source, String subject) {
        Objects.requireNonNull(source, subject + " must not be null");
        if (source.size() > 256) throw new IllegalArgumentException(subject + " exceeds limit");
        Map<String, String> result = new LinkedHashMap<>();
        source.forEach((canonical, wire) -> putOneToOne(result,
            path(canonical, subject + " canonical path"), path(wire, subject + " wire path"), subject));
        return Collections.unmodifiableMap(result);
    }

    private static boolean hasRenamedSupplementalPath(
        Map<String, String> jsonObjects,
        List<CollectionMapping> collections,
        Optional<DiscriminatorMapping> discriminator
    ) {
        return jsonObjects.entrySet().stream().anyMatch(entry -> !entry.getKey().equals(entry.getValue()))
            || collections.stream().anyMatch(mapping -> !mapping.canonicalPath().equals(mapping.wirePath()))
            || discriminator.filter(mapping -> !mapping.canonicalPath().equals(mapping.wirePath())).isPresent();
    }

    private static Map<String, JsonNode> constants(Map<String, JsonNode> source) {
        Objects.requireNonNull(source, "HTTP mapping constants must not be null");
        if (source.size() > 128) throw new IllegalArgumentException("HTTP mapping constants exceed limit");
        Map<String, JsonNode> result = new LinkedHashMap<>();
        source.forEach((target, value) -> {
            JsonNode scalar = Objects.requireNonNull(value, "HTTP mapping constant must not be null");
            if (!scalar.isValueNode()) throw new IllegalArgumentException("HTTP mapping constants must be scalar");
            result.put(path(target, "HTTP constant target"), scalar.deepCopy());
        });
        return Collections.unmodifiableMap(result);
    }

    private static Map<String, Map<String, String>> enums(Map<String, Map<String, String>> source) {
        Objects.requireNonNull(source, "HTTP enum mappings must not be null");
        Map<String, Map<String, String>> result = new LinkedHashMap<>();
        source.forEach((path, values) -> result.put(
            HttpRepresentationMappingOptions.path(path, "HTTP enum path"), translation(values, "HTTP enum values")));
        return Collections.unmodifiableMap(result);
    }

    private static Map<String, String> translation(Map<String, String> source, String subject) {
        Objects.requireNonNull(source, subject + " must not be null");
        if (source.isEmpty() || source.size() > 128 || source.values().stream().distinct().count() != source.size()) {
            throw new IllegalArgumentException(subject + " must be a bounded one-to-one translation");
        }
        Map<String, String> result = new LinkedHashMap<>();
        source.forEach((left, right) -> putOneToOne(result,
            text(left, subject + " source"), text(right, subject + " target"), subject));
        return Collections.unmodifiableMap(result);
    }

    private static void putOneToOne(Map<String, String> result, String source, String target, String subject) {
        if (result.containsKey(source) || result.containsValue(target)) {
            throw new IllegalArgumentException(subject + " must remain one-to-one after normalisation");
        }
        result.put(source, target);
    }

    private static Map<String, String> stringMap(JsonNode value, String subject) {
        if (value.isMissingNode()) return Map.of();
        requireObject(value, subject);
        Map<String, String> result = new LinkedHashMap<>();
        value.fields().forEachRemaining(entry -> {
            if (!entry.getValue().isTextual()) throw new IllegalArgumentException(subject + " values must be strings");
            result.put(entry.getKey(), entry.getValue().textValue());
        });
        return result;
    }

    private static JsonNode requireObject(JsonNode value, String subject) {
        if (!value.isObject()) throw new IllegalArgumentException(subject + " must be an object");
        return value;
    }

    private static String required(JsonNode value, String field) {
        JsonNode item = value.path(field);
        if (!item.isTextual() || item.textValue().isBlank()) {
            throw new IllegalArgumentException("HTTP representation option '" + field + "' is required");
        }
        return item.textValue();
    }

    static String path(String value, String subject) {
        String result = text(value, subject);
        if (!result.matches("[A-Za-z][A-Za-z0-9_]*(?:\\.[A-Za-z][A-Za-z0-9_]*)*")) {
            throw new IllegalArgumentException(subject + " must traverse named record fields only: " + result);
        }
        return result;
    }

    private static String text(String value, String subject) {
        String result = Objects.requireNonNull(value, subject + " must not be null").trim();
        if (result.isEmpty()) throw new IllegalArgumentException(subject + " must not be blank");
        return result;
    }
}
