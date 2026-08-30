package org.pipelineframework.connector.query.jpa;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/** Connector-domain predicate shared by Hibernate Query providers. */
public record JpaPredicate(String operator, List<Object> values) {
    private static final Set<String> SUPPORTED_OPERATORS = Set.of(
        "eq", "in", "gt", "gte", "lt", "lte", "between", "like", "isNull");

    public JpaPredicate {
        if (operator == null || operator.isBlank()) {
            throw new IllegalArgumentException("JPA predicate operator must not be blank");
        }
        operator = operator.trim();
        if (!SUPPORTED_OPERATORS.contains(operator)) {
            throw new IllegalArgumentException("JPA predicate operator is not supported: " + operator);
        }
        values = normalizeValues(operator, values);
    }

    private static List<Object> normalizeValues(String operator, List<Object> rawValues) {
        List<Object> normalized = rawValues == null ? List.of() : new ArrayList<>(rawValues);
        return switch (operator) {
            case "isNull" -> normalizeIsNull(normalized);
            case "between" -> {
                if (normalized.size() != 2) {
                    throw new IllegalArgumentException("JPA predicate between requires exactly two values");
                }
                yield normalizeNonBlankValues(normalized, operator);
            }
            case "in" -> {
                if (normalized.isEmpty()) {
                    throw new IllegalArgumentException("JPA predicate in requires at least one value");
                }
                yield normalizeNonBlankValues(normalized, operator);
            }
            default -> {
                if (normalized.size() != 1) {
                    throw new IllegalArgumentException("JPA predicate " + operator + " requires exactly one value");
                }
                yield normalizeNonBlankValues(normalized, operator);
            }
        };
    }

    private static List<Object> normalizeIsNull(List<Object> values) {
        if (values.size() != 1) {
            throw new IllegalArgumentException("JPA predicate isNull requires exactly one boolean value");
        }
        Object value = values.getFirst();
        if (value instanceof Boolean) {
            return List.of(value);
        }
        if (value instanceof String text) {
            return switch (text.trim().toLowerCase(Locale.ROOT)) {
                case "true" -> List.of(Boolean.TRUE);
                case "false" -> List.of(Boolean.FALSE);
                default -> throw new IllegalArgumentException("JPA predicate isNull requires a boolean value");
            };
        }
        throw new IllegalArgumentException("JPA predicate isNull requires a boolean value");
    }

    private static List<Object> normalizeNonBlankValues(List<Object> values, String operator) {
        List<Object> normalized = new ArrayList<>();
        for (Object value : values) {
            if (value == null) {
                throw new IllegalArgumentException("JPA predicate " + operator + " values must not be null");
            }
            if (value instanceof String text) {
                if (text.isBlank()) {
                    throw new IllegalArgumentException("JPA predicate " + operator + " values must not be blank");
                }
                normalized.add(text.trim());
            } else {
                normalized.add(value);
            }
        }
        return List.copyOf(normalized);
    }
}
