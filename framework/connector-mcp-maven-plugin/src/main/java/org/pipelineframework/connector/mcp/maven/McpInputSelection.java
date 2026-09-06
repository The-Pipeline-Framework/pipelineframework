package org.pipelineframework.connector.mcp.maven;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;

/** Explicit input allowlist. A selected parent retains its complete external subtree. */
public record McpInputSelection(List<String> includeFields) {
    public McpInputSelection {
        Objects.requireNonNull(includeFields, "includeFields");
        if (includeFields.size() > 1024) {
            throw new IllegalArgumentException("includeFields exceeds 1024-path limit");
        }
        TreeSet<String> paths = new TreeSet<>();
        for (String field : includeFields) {
            if (field == null || field.length() > 1024
                || !field.matches("[A-Za-z_][A-Za-z_0-9]*(\\.[A-Za-z_][A-Za-z_0-9]*)*")) {
                throw new IllegalArgumentException("includeFields contains an invalid property path");
            }
            if (!paths.add(field)) {
                throw new IllegalArgumentException("includeFields duplicate path: " + field);
            }
        }
        for (String path : paths) {
            for (int dot = path.indexOf('.'); dot >= 0; dot = path.indexOf('.', dot + 1)) {
                if (paths.contains(path.substring(0, dot))) {
                    throw new IllegalArgumentException("includeFields conflicting parent and child: " + path);
                }
            }
        }
        includeFields = List.copyOf(paths);
    }

    public Map<String, Object> project(Map<String, Object> original) {
        Objects.requireNonNull(original, "MCP input schema");
        return includeFields.isEmpty() ? original : select(original, includeFields, "$", 0);
    }

    private Map<String, Object> select(Map<String, Object> schema, List<String> paths, String location, int depth) {
        if (depth > 64) {
            throw failure(location, "includeFields exceeds nested selection limit");
        }
        Object type = schema.get("type");
        boolean object = "object".equals(type) || (type instanceof List<?> types && types.size() == 2
            && types.contains("object") && types.contains("null"));
        if (!object || !Boolean.FALSE.equals(schema.get("additionalProperties"))) {
            throw failure(location, "selected parents must be closed objects");
        }
        Map<String, Object> properties = map(schema.get("properties"), location + ".properties");
        TreeSet<String> names = new TreeSet<>();
        for (String path : paths) {
            String name = path.split("\\.", 2)[0];
            if (!properties.containsKey(name)) {
                throw failure(location + ".properties." + name, "unknown includeFields path");
            }
            names.add(name);
        }
        Object requiredValue = schema.getOrDefault("required", List.of());
        if (!(requiredValue instanceof List<?> required)) {
            throw failure(location + ".required", "must be an array");
        }
        for (Object name : required) {
            if (!(name instanceof String text) || !names.contains(text)) {
                throw failure(location + ".properties." + name, "includeFields omits required property");
            }
        }
        Map<String, Object> selected = new LinkedHashMap<>();
        for (String name : names) {
            Object child = properties.get(name);
            if (paths.contains(name)) {
                selected.put(name, child);
            } else {
                List<String> nested = paths.stream().filter(path -> path.startsWith(name + "."))
                    .map(path -> path.substring(name.length() + 1)).toList();
                selected.put(name, select(map(child, location + ".properties." + name), nested,
                    location + ".properties." + name, depth + 1));
            }
        }
        Map<String, Object> result = new LinkedHashMap<>(schema);
        result.put("properties", selected);
        return result;
    }

    private Map<String, Object> map(Object value, String path) {
        if (!(value instanceof Map<?, ?> raw)) {
            throw failure(path, "must be an object for includeFields selection");
        }
        Map<String, Object> result = new LinkedHashMap<>();
        raw.forEach((key, child) -> {
            if (!(key instanceof String name)) {
                throw failure(path, "must use string property names");
            }
            result.put(name, child);
        });
        return result;
    }

    private IllegalArgumentException failure(String path, String reason) {
        return new IllegalArgumentException("includeFields schema " + path + " " + reason);
    }
}
