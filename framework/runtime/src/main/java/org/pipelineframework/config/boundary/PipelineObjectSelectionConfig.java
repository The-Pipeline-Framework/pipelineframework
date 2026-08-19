package org.pipelineframework.config.boundary;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/** One Object Ingest admission assembled from a selected set of source objects. */
public record PipelineObjectSelectionConfig(
    String mode,
    Map<String, String> keys,
    Optional<String> into
) {
    public PipelineObjectSelectionConfig {
        mode = normalize(mode);
        if (!"together".equals(mode)) {
            throw new IllegalArgumentException("input.object.selection.mode must be 'together'");
        }
        LinkedHashMap<String, String> copied = new LinkedHashMap<>();
        if (keys != null) {
            keys.forEach((field, key) -> {
                String normalizedField = normalize(field);
                String normalizedKey = normalize(key);
                if (normalizedField == null || normalizedKey == null) {
                    throw new IllegalArgumentException("input.object.selection.keys must not contain blank fields or keys");
                }
                if (copied.containsKey(normalizedField)) {
                    throw new IllegalArgumentException("input.object.selection.keys contains duplicate field '" + normalizedField + "'");
                }
                copied.put(normalizedField, normalizedKey);
            });
        }
        keys = java.util.Collections.unmodifiableMap(copied);
        into = normalize(into);
        if (keys.isEmpty() == into.isEmpty()) {
            throw new IllegalArgumentException("input.object.selection must declare exactly one of keys or into");
        }
    }

    private static String normalize(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }

    private static Optional<String> normalize(Optional<String> value) {
        if (value == null || value.isEmpty()) {
            return Optional.empty();
        }
        String normalized = normalize(value.orElseThrow());
        return normalized == null ? Optional.empty() : Optional.of(normalized);
    }
}
