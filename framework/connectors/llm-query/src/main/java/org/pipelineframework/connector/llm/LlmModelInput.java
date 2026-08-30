package org.pipelineframework.connector.llm;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.RecordComponent;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.pipelineframework.repository.PayloadReference;

/** Produces the model-visible JSON and media references from one typed Query input. */
final class LlmModelInput {
    private final ObjectMapper json;

    LlmModelInput(ObjectMapper json) {
        this.json = java.util.Objects.requireNonNull(json, "JSON mapper must not be null");
    }

    String applicationStateJson(Object input, List<String> excludedPaths) throws Exception {
        JsonNode root = json.valueToTree(input);
        for (String path : excludedPaths) {
            removeJsonPath(root, path);
        }
        return json.writeValueAsString(root);
    }

    List<PayloadReference> payloadReferences(Object input, List<String> excludedPaths) {
        LinkedHashSet<PayloadReference> references = new LinkedHashSet<>();
        collectPayloadReferences(input, "", Set.copyOf(excludedPaths), references,
            Collections.newSetFromMap(new IdentityHashMap<>()));
        return List.copyOf(references);
    }

    private static void removeJsonPath(JsonNode root, String path) {
        String[] components = path.split("\\.");
        JsonNode parent = root;
        for (int index = 0; index < components.length - 1; index++) {
            parent = parent.path(components[index]);
            if (parent.isMissingNode()) {
                return;
            }
        }
        if (parent instanceof ObjectNode object) {
            object.remove(components[components.length - 1]);
        }
    }

    private static void collectPayloadReferences(
        Object value,
        String path,
        Set<String> excludedPaths,
        LinkedHashSet<PayloadReference> references,
        Set<Object> visited
    ) {
        if (!path.isEmpty() && excludedPaths.contains(path)) {
            return;
        }
        if (value == null || value instanceof String || value instanceof Number
            || value instanceof Boolean || value instanceof Character || value.getClass().isEnum()) {
            return;
        }
        if (value instanceof PayloadReference reference) {
            references.add(reference);
            return;
        }
        if (!visited.add(value)) {
            return;
        }
        if (value instanceof Optional<?> optional) {
            optional.ifPresent(item -> collectPayloadReferences(item, path, excludedPaths, references, visited));
            return;
        }
        if (value instanceof Iterable<?> iterable) {
            iterable.forEach(item -> collectPayloadReferences(item, path, excludedPaths, references, visited));
            return;
        }
        if (value instanceof Map<?, ?> map) {
            map.forEach((key, item) -> collectPayloadReferences(
                item, path.isEmpty() ? String.valueOf(key) : path + "." + key, excludedPaths, references, visited));
            return;
        }
        if (!value.getClass().isRecord()) {
            return;
        }
        for (RecordComponent component : value.getClass().getRecordComponents()) {
            try {
                String componentPath = path.isEmpty() ? component.getName() : path + "." + component.getName();
                collectPayloadReferences(component.getAccessor().invoke(value), componentPath,
                    excludedPaths, references, visited);
            } catch (IllegalAccessException | InvocationTargetException failure) {
                throw new IllegalStateException(
                    "Failed inspecting payload reference field '" + component.getName() + "'", failure);
            }
        }
    }
}
