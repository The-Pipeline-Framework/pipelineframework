package org.pipelineframework.connector.graphql;

import java.util.Locale;
import java.util.Objects;

final class GraphQlValues {
    private GraphQlValues() {
    }

    static String text(String value, String label, int maximumLength) {
        String normalized = Objects.requireNonNull(value, label + " must not be null").trim();
        if (normalized.isEmpty()) throw new IllegalArgumentException(label + " must not be blank");
        if (normalized.length() > maximumLength) {
            throw new IllegalArgumentException(label + " must not exceed " + maximumLength + " characters");
        }
        return normalized;
    }

    static String message(String value) {
        String sanitized = Objects.requireNonNull(value, "GraphQL error message must not be null")
            .replaceAll("[\\p{Cntrl}&&[^\\n\\t]]", " ")
            .replaceAll("\\s+", " ")
            .trim();
        if (sanitized.isEmpty()) sanitized = "GraphQL operation failed";
        return sanitized.length() <= GraphQlError.MAX_MESSAGE_LENGTH
            ? sanitized
            : sanitized.substring(0, GraphQlError.MAX_MESSAGE_LENGTH);
    }

    static String code(String value) {
        if (value == null || value.isBlank()) return "graphql-error";
        String normalized = value.trim().toLowerCase(Locale.ROOT)
            .replaceAll("[^a-z0-9]+", "-")
            .replaceAll("^-+|-+$", "");
        if (normalized.isEmpty()) return "graphql-error";
        return normalized.length() <= GraphQlError.MAX_CODE_LENGTH
            ? normalized
            : normalized.substring(0, GraphQlError.MAX_CODE_LENGTH);
    }

}
