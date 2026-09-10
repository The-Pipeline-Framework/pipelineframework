package org.pipelineframework.processor.util;

import java.io.IOException;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.gson.Gson;
import org.pipelineframework.config.pipeline.PipelineYamlCallable;
import org.pipelineframework.config.pipeline.PipelineYamlConfig;
import org.pipelineframework.config.pipeline.PipelineYamlConnectorBinding;
import org.pipelineframework.representation.spi.ResolvedOperationRepresentation;

/** Projects sanitized release-time operation provenance for capabilities referenced by the effective pipeline. */
final class ConnectorOperationProvenanceProjector {
    static final String RESOURCE_PATH = "META-INF/pipeline/connector-operation-provenance.json";
    private static final Gson GSON = new Gson();

    List<Map<String, Object>> project(
        PipelineYamlConfig config,
        Path moduleDir,
        ClassLoader classLoader,
        List<ResolvedOperationRepresentation> resolvedRepresentations
    ) {
        if (config == null || config.connectors().isEmpty()) return List.of();
        Set<Reference> references = references(config);
        if (references.isEmpty()) return List.of();
        Map<String, ResolvedOperationRepresentation> mappings = new LinkedHashMap<>();
        Objects.requireNonNull(resolvedRepresentations, "resolved operation representations must not be null")
            .forEach(value -> mappings.put(value.mappingKey(), value));
        Map<String, Map<String, Object>> imports = new LinkedHashMap<>();
        for (Document document : documents(moduleDir, classLoader)) {
            int schemaVersion = integer(document.root().get("schemaVersion"), "schemaVersion", document.source());
            if (schemaVersion != 1) {
                throw new IllegalStateException("Connector operation provenance schemaVersion must be 1: "
                    + document.source());
            }
            for (Map<String, Object> imported : maps(document.root().get("imports"), "imports", document.source())) {
                String provider = string(imported.get("provider"), "provider", document.source());
                List<Map<String, Object>> selected = maps(imported.get("operations"), "operations", document.source())
                    .stream().filter(operation -> references.contains(reference(provider, operation, document.source())))
                    .map(operation -> withMappings(operation, mappings))
                    .sorted(Comparator.comparing(ConnectorOperationProvenanceProjector::operationKey))
                    .toList();
                if (selected.isEmpty()) continue;
                Map<String, Object> projected = new LinkedHashMap<>(imported);
                projected.put("operations", selected);
                Map<String, Object> normalized = immutableSortedMap(projected);
                String key = provider + ":" + string(imported.get("importId"), "importId", document.source());
                Map<String, Object> previous = imports.putIfAbsent(key, normalized);
                if (previous != null && !previous.equals(normalized)) {
                    throw new IllegalStateException("Conflicting Connector operation provenance for " + key);
                }
            }
        }
        return imports.entrySet().stream().sorted(Map.Entry.comparingByKey()).map(Map.Entry::getValue).toList();
    }

    private static Map<String, Object> withMappings(
        Map<String, Object> source,
        Map<String, ResolvedOperationRepresentation> mappings
    ) {
        Map<String, Object> operation = new LinkedHashMap<>(source);
        operation.put("majorVersion", integer(
            source.get("majorVersion"), "majorVersion", "operation provenance"));
        Object requestKey = operation.get("requestMappingKey");
        if (requestKey instanceof String key && mappings.containsKey(key)) {
            operation.put("requestMapping", mapping(mappings.get(key)));
        }
        Map<String, Object> responses = new LinkedHashMap<>();
        List<Map<String, Object>> responseValues = operation.containsKey("responses")
            ? maps(operation.get("responses"), "responses", "operation provenance") : List.of();
        for (Map<String, Object> response : responseValues) {
            Object key = response.get("mappingKey");
            if (key instanceof String mappingKey && mappings.containsKey(mappingKey)) {
                responses.put(mappingKey, mapping(mappings.get(mappingKey)));
            }
        }
        if (!responses.isEmpty()) operation.put("responseMappings", immutableSortedMap(responses));
        return immutableSortedMap(operation);
    }

    private static Map<String, Object> mapping(ResolvedOperationRepresentation value) {
        Map<String, Object> mapping = new LinkedHashMap<>();
        mapping.put("mode", value.mode());
        mapping.put("mappingFingerprint", value.mappingFingerprint());
        value.representationType().ifPresent(type -> mapping.put("representationType", type));
        value.mapperType().ifPresent(type -> mapping.put("mapperType", type));
        return immutableSortedMap(mapping);
    }

    private static Set<Reference> references(PipelineYamlConfig config) {
        Set<Reference> references = new LinkedHashSet<>();
        for (var steps : config.stepDefinitions().values()) {
            for (var step : steps) {
                step.operationSelection().ifPresent(selection -> {
                    PipelineYamlConnectorBinding binding = config.connectors().get(selection.using());
                    if (binding != null) references.add(new Reference(binding.provider(), step.kind().toLowerCase(),
                        selection.operation(), selection.operationVersion()));
                });
                for (PipelineYamlCallable callable : step.callables().values()) {
                    PipelineYamlConnectorBinding binding = config.connectors().get(callable.using());
                    if (binding != null) references.add(new Reference(binding.provider(), callable.kindToken(),
                        callable.operation(), callable.operationVersion()));
                }
            }
        }
        return Set.copyOf(references);
    }

    private static Reference reference(String provider, Map<String, Object> operation, String source) {
        return new Reference(provider, normalizeKind(string(operation.get("kind"), "operation kind", source)),
            string(operation.get("operation"), "operation", source),
            integer(operation.get("majorVersion"), "majorVersion", source));
    }

    private static String normalizeKind(String value) {
        return switch (value.toLowerCase(java.util.Locale.ROOT)) {
            case "query", "tpf:query" -> "query";
            case "command", "tpf:command" -> "command";
            default -> value.toLowerCase(java.util.Locale.ROOT);
        };
    }

    private static List<Document> documents(Path moduleDir, ClassLoader classLoader) {
        try {
            Map<String, Document> result = new LinkedHashMap<>();
            if (moduleDir != null) {
                Path local = moduleDir.resolve("src/main/resources").resolve(RESOURCE_PATH).normalize();
                if (Files.isRegularFile(local)) {
                    try (Reader reader = Files.newBufferedReader(local, StandardCharsets.UTF_8)) {
                        result.put(local.toUri().toString(), new Document(local.toString(), object(reader, local.toString())));
                    }
                }
            }
            Enumeration<URL> resources = classLoader.getResources(RESOURCE_PATH);
            while (resources.hasMoreElements()) {
                URL resource = resources.nextElement();
                if (result.containsKey(resource.toExternalForm())) continue;
                try (Reader reader = new java.io.InputStreamReader(resource.openStream(), StandardCharsets.UTF_8)) {
                    result.put(resource.toExternalForm(), new Document(resource.toExternalForm(),
                        object(reader, resource.toExternalForm())));
                }
            }
            return List.copyOf(result.values());
        } catch (IOException failure) {
            throw new IllegalStateException("Unable to read Connector operation provenance", failure);
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> object(Reader reader, String source) {
        Object value = GSON.fromJson(reader, Object.class);
        if (!(value instanceof Map<?, ?> map)) {
            throw new IllegalStateException("Connector operation provenance must be an object: " + source);
        }
        return (Map<String, Object>) map;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> maps(Object value, String field, String source) {
        if (!(value instanceof List<?> values)) {
            throw new IllegalStateException("Connector operation provenance '" + field + "' must be an array: " + source);
        }
        List<Map<String, Object>> result = new ArrayList<>();
        for (Object item : values) {
            if (!(item instanceof Map<?, ?> map)) {
                throw new IllegalStateException("Connector operation provenance '" + field
                    + "' entries must be objects: " + source);
            }
            result.add((Map<String, Object>) map);
        }
        return List.copyOf(result);
    }

    private static String string(Object value, String field, String source) {
        if (!(value instanceof String text) || text.isBlank()) {
            throw new IllegalStateException("Connector operation provenance '" + field + "' is required: " + source);
        }
        return text;
    }

    private static int integer(Object value, String field, String source) {
        if (!(value instanceof Number number)) {
            throw new IllegalStateException("Connector operation provenance '" + field + "' must be positive: " + source);
        }
        double numeric = number.doubleValue();
        if (!Double.isFinite(numeric) || numeric < 1 || numeric > Integer.MAX_VALUE || numeric != Math.rint(numeric)) {
            throw new IllegalStateException("Connector operation provenance '" + field
                + "' must be a positive integer within the int range: " + source);
        }
        return (int) numeric;
    }

    private static String operationKey(Map<String, Object> value) {
        return value.get("kind") + ":" + value.get("operation") + ":" + value.get("majorVersion");
    }

    private static Map<String, Object> immutableSortedMap(Map<String, ?> source) {
        Map<String, Object> result = new LinkedHashMap<>();
        source.entrySet().stream().sorted(Map.Entry.comparingByKey())
            .forEach(entry -> result.put(entry.getKey(), immutable(entry.getValue())));
        return Collections.unmodifiableMap(result);
    }

    private static Object immutable(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> converted = new LinkedHashMap<>();
            map.forEach((key, item) -> converted.put(Objects.toString(key), immutable(item)));
            return immutableSortedMap(converted);
        }
        if (value instanceof List<?> list) return list.stream().map(ConnectorOperationProvenanceProjector::immutable).toList();
        return value;
    }

    private record Reference(String provider, String kind, String operation, int version) {
    }

    private record Document(String source, Map<String, Object> root) {
    }
}
