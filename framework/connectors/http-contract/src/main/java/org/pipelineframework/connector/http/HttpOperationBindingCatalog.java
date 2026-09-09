package org.pipelineframework.connector.http;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

/** Strict reader/writer for compiler-resolved HTTP operation representation bindings. */
public final class HttpOperationBindingCatalog {
    public static final String RESOURCE_PATH = "META-INF/pipeline/http-operation-bindings.json";
    public static final int SCHEMA_VERSION = 1;
    private final List<HttpOperationRepresentationBinding> bindings;

    public HttpOperationBindingCatalog(List<HttpOperationRepresentationBinding> bindings) {
        Set<String> keys = new HashSet<>();
        this.bindings = Objects.requireNonNull(bindings, "HTTP representation bindings must not be null").stream()
            .map(binding -> Objects.requireNonNull(binding, "HTTP representation binding must not be null"))
            .sorted(Comparator.comparing(HttpOperationRepresentationBinding::mappingKey)).peek(binding -> {
                if (!keys.add(binding.mappingKey())) {
                    throw new IllegalArgumentException("duplicate HTTP representation mapping key: " + binding.mappingKey());
                }
            }).toList();
    }

    public List<HttpOperationRepresentationBinding> bindings() {
        return bindings;
    }

    public Optional<HttpOperationRepresentationBinding> find(String mappingKey) {
        return bindings.stream().filter(binding -> binding.mappingKey().equals(mappingKey)).findFirst();
    }

    public static HttpOperationBindingCatalog load(ClassLoader classLoader) {
        Objects.requireNonNull(classLoader, "HTTP metadata classloader must not be null");
        try {
            Enumeration<URL> resources = classLoader.getResources(RESOURCE_PATH);
            List<URL> ordered = new ArrayList<>();
            while (resources.hasMoreElements()) ordered.add(resources.nextElement());
            ordered.sort(Comparator.comparing(URL::toExternalForm));
            List<HttpOperationRepresentationBinding> values = new ArrayList<>();
            for (URL resource : ordered) {
                try (var stream = resource.openStream()) {
                    byte[] bytes = stream.readNBytes(HttpPinnedJson.MAX_RESOURCE_BYTES + 1);
                    if (bytes.length > HttpPinnedJson.MAX_RESOURCE_BYTES) {
                        throw new IllegalArgumentException("HTTP operation binding resource exceeds size limit");
                    }
                    values.addAll(read(new String(bytes, StandardCharsets.UTF_8)).bindings());
                }
            }
            return new HttpOperationBindingCatalog(values);
        } catch (IOException failure) {
            throw new IllegalStateException("unable to load HTTP operation representation bindings", failure);
        }
    }

    public String json() {
        var root = JsonNodeFactory.instance.objectNode();
        root.put("schemaVersion", SCHEMA_VERSION);
        var array = root.putArray("bindings");
        bindings.forEach(binding -> {
            var node = array.addObject();
            node.put("mappingKey", binding.mappingKey());
            node.put("mode", binding.mode().name());
            binding.representationType().ifPresent(value -> node.put("representationType", value));
            binding.mapperType().ifPresent(value -> node.put("mapperType", value));
            node.put("mappingFingerprint", binding.mappingFingerprint());
        });
        return HttpPinnedJson.canonicalize(root) + "\n";
    }

    public static HttpOperationBindingCatalog read(String json) {
        JsonNode root = HttpPinnedJson.parse(json);
        if (!root.isObject() || !root.path("schemaVersion").isInt()
            || root.path("schemaVersion").intValue() != SCHEMA_VERSION || !root.path("bindings").isArray()) {
            throw new IllegalArgumentException("unsupported HTTP operation binding catalogue");
        }
        Set<String> rootFields = Set.of("schemaVersion", "bindings");
        root.fieldNames().forEachRemaining(field -> {
            if (!rootFields.contains(field)) throw new IllegalArgumentException("unknown HTTP binding field '" + field + "'");
        });
        List<HttpOperationRepresentationBinding> values = new ArrayList<>();
        root.path("bindings").forEach(node -> {
            Set<String> allowed = Set.of("mappingKey", "mode", "representationType", "mapperType", "mappingFingerprint");
            if (!node.isObject()) throw new IllegalArgumentException("HTTP representation binding must be an object");
            node.fieldNames().forEachRemaining(field -> {
                if (!allowed.contains(field)) throw new IllegalArgumentException("unknown HTTP representation binding field '" + field + "'");
            });
            values.add(new HttpOperationRepresentationBinding(required(node, "mappingKey"),
                HttpRepresentationMode.valueOf(required(node, "mode")), optional(node, "representationType"),
                optional(node, "mapperType"), required(node, "mappingFingerprint")));
        });
        return new HttpOperationBindingCatalog(values);
    }

    private static String required(JsonNode node, String field) {
        JsonNode value = node.path(field);
        if (!value.isTextual() || value.textValue().isBlank()) {
            throw new IllegalArgumentException("HTTP representation binding field '" + field + "' must be text");
        }
        return value.textValue();
    }

    private static Optional<String> optional(JsonNode node, String field) {
        return node.has(field) ? Optional.of(required(node, field)) : Optional.empty();
    }
}
