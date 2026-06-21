package org.pipelineframework.config.pipeline;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Declarative JPA read specification for a first-party captured query.
 */
public record PipelineYamlJpaQuery(
    String entity,
    Map<String, String> where,
    Map<String, String> projection,
    String result
) {
    public PipelineYamlJpaQuery {
        if (entity == null || entity.isBlank()) {
            throw new IllegalArgumentException("query jpa.entity must not be blank");
        }
        where = validateMap(where, "where", true);
        projection = validateMap(projection, "projection", false);
        result = result == null || result.isBlank() ? "single" : result.trim();
        if (!"single".equals(result)) {
            throw new IllegalArgumentException("query jpa.result supports only single in v1");
        }
    }

    private static Map<String, String> validateMap(Map<String, String> values, String field, boolean required) {
        if (values == null || values.isEmpty()) {
            if (required) {
                throw new IllegalArgumentException("query jpa." + field + " must not be empty");
            }
            return Map.of();
        }
        Map<String, String> normalized = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : values.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key == null || key.isBlank()) {
                throw new IllegalArgumentException("query jpa." + field + " must not contain blank keys");
            }
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException("query jpa." + field + "." + key + " must not be blank");
            }
            normalized.put(key.trim(), value.trim());
        }
        return Map.copyOf(normalized);
    }
}
