package org.pipelineframework.config.pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Declarative predicate for a first-party JPA captured query.
 */
public record PipelineYamlJpaPredicate(String operator, List<Object> values) {
    private static final Set<String> SUPPORTED_OPERATORS = Set.of(
        "eq",
        "in",
        "gt",
        "gte",
        "lt",
        "lte",
        "between",
        "like",
        "isNull");

    public PipelineYamlJpaPredicate {
        if (operator == null || operator.isBlank()) {
            throw new IllegalArgumentException("query jpa.where operator must not be blank");
        }
        operator = operator.trim();
        if (!SUPPORTED_OPERATORS.contains(operator)) {
            throw new IllegalArgumentException("query jpa.where operator is not supported: " + operator);
        }
        values = normalizeValues(operator, values);
    }

    public static PipelineYamlJpaPredicate equalTo(Object value) {
        List<Object> values = new ArrayList<>();
        values.add(value);
        return new PipelineYamlJpaPredicate("eq", values);
    }

    private static List<Object> normalizeValues(String operator, List<Object> rawValues) {
        List<Object> normalized = rawValues == null ? List.of() : new ArrayList<>(rawValues);
        return switch (operator) {
            case "isNull" -> normalizeIsNull(normalized);
            case "between" -> {
                if (normalized.size() != 2) {
                    throw new IllegalArgumentException("query jpa.where between requires exactly two values");
                }
                yield normalizeNonBlankValues(normalized, operator);
            }
            case "in" -> {
                if (normalized.isEmpty()) {
                    throw new IllegalArgumentException("query jpa.where in requires at least one value");
                }
                yield normalizeNonBlankValues(normalized, operator);
            }
            default -> {
                if (normalized.size() != 1) {
                    throw new IllegalArgumentException("query jpa.where " + operator + " requires exactly one value");
                }
                yield normalizeNonBlankValues(normalized, operator);
            }
        };
    }

    private static List<Object> normalizeIsNull(List<Object> values) {
        if (values.size() != 1) {
            throw new IllegalArgumentException("query jpa.where isNull requires exactly one boolean value");
        }
        Object value = values.get(0);
        if (value instanceof Boolean) {
            return List.of(value);
        }
        if (value instanceof String text) {
            String normalized = text.trim().toLowerCase(Locale.ROOT);
            if ("true".equals(normalized)) {
                return List.of(Boolean.TRUE);
            }
            if ("false".equals(normalized)) {
                return List.of(Boolean.FALSE);
            }
        }
        throw new IllegalArgumentException("query jpa.where isNull requires a boolean value");
    }

    private static List<Object> normalizeNonBlankValues(List<Object> values, String operator) {
        List<Object> normalized = new ArrayList<>();
        for (Object value : values) {
            if (value == null) {
                throw new IllegalArgumentException("query jpa.where " + operator + " values must not be null");
            }
            if (value instanceof String text) {
                if (text.isBlank()) {
                    throw new IllegalArgumentException("query jpa.where " + operator + " values must not be blank");
                }
                normalized.add(text.trim());
            } else {
                normalized.add(value);
            }
        }
        return List.copyOf(normalized);
    }
}
