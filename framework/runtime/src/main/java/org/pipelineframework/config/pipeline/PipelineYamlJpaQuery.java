package org.pipelineframework.config.pipeline;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Declarative JPA read specification for a first-party captured query.
 */
public record PipelineYamlJpaQuery(
    String entity,
    Map<String, PipelineYamlJpaPredicate> where,
    Map<String, String> projection,
    Map<String, String> orderBy,
    Integer limit,
    String result
) {
    public PipelineYamlJpaQuery(String entity, Map<String, String> where, Map<String, String> projection, String result) {
        this(entity, legacyWhere(where), projection, Map.of(), null, result);
    }

    public PipelineYamlJpaQuery {
        if (entity == null || entity.isBlank()) {
            throw new IllegalArgumentException("query jpa.entity must not be blank");
        }
        where = validateWhere(where);
        projection = validateMap(projection, "projection", false);
        orderBy = validateOrderBy(orderBy);
        if (limit != null && limit != 1) {
            throw new IllegalArgumentException("query jpa.limit supports only 1 in v2");
        }
        if (limit != null && orderBy.isEmpty()) {
            throw new IllegalArgumentException("query jpa.limit requires orderBy");
        }
        result = result == null || result.isBlank() ? "single" : result.trim();
        if (!"single".equals(result)) {
            throw new IllegalArgumentException("query jpa.result supports only single in v1");
        }
    }

    private static Map<String, PipelineYamlJpaPredicate> legacyWhere(Map<String, String> values) {
        if (values == null || values.isEmpty()) {
            return Map.of();
        }
        Map<String, PipelineYamlJpaPredicate> normalized = new LinkedHashMap<>();
        values.forEach((key, value) -> normalized.put(key, PipelineYamlJpaPredicate.equalTo(value)));
        return Collections.unmodifiableMap(new LinkedHashMap<>(normalized));
    }

    private static Map<String, PipelineYamlJpaPredicate> validateWhere(Map<String, PipelineYamlJpaPredicate> values) {
        if (values == null || values.isEmpty()) {
            throw new IllegalArgumentException("query jpa.where must not be empty");
        }
        Map<String, PipelineYamlJpaPredicate> normalized = new LinkedHashMap<>();
        for (Map.Entry<String, PipelineYamlJpaPredicate> entry : values.entrySet()) {
            String key = entry.getKey();
            PipelineYamlJpaPredicate value = entry.getValue();
            if (key == null || key.isBlank()) {
                throw new IllegalArgumentException("query jpa.where must not contain blank keys");
            }
            if (value == null) {
                throw new IllegalArgumentException("query jpa.where." + key + " must not be null");
            }
            normalized.put(key.trim(), value);
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(normalized));
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
        return Collections.unmodifiableMap(new LinkedHashMap<>(normalized));
    }

    private static Map<String, String> validateOrderBy(Map<String, String> values) {
        Map<String, String> normalized = validateMap(values, "orderBy", false);
        Map<String, String> directions = new LinkedHashMap<>();
        normalized.forEach((path, direction) -> {
            String lower = direction.toLowerCase(java.util.Locale.ROOT);
            if (!"asc".equals(lower) && !"desc".equals(lower)) {
                throw new IllegalArgumentException("query jpa.orderBy." + path + " must be asc or desc");
            }
            directions.put(path, lower);
        });
        return Collections.unmodifiableMap(directions);
    }
}
