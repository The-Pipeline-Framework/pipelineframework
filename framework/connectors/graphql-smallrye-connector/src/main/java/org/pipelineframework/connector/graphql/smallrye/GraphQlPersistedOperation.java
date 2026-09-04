package org.pipelineframework.connector.graphql.smallrye;

import java.util.Locale;
import java.util.Objects;

/** One digest-pinned application resource in a GraphQL binding's operation catalogue. */
public record GraphQlPersistedOperation(
    GraphQlOperationKind kind,
    String operationName,
    String resource,
    String sha256
) {
    public GraphQlPersistedOperation {
        kind = Objects.requireNonNull(kind, "GraphQL operation kind must not be null");
        operationName = text(operationName, "GraphQL operation name", 256);
        resource = text(resource, "GraphQL operation resource", 1_024);
        if (resource.startsWith("/") || resource.contains("\\") || resource.contains("..")) {
            throw new IllegalArgumentException("GraphQL operation resource must be a relative classpath resource");
        }
        sha256 = text(sha256, "GraphQL operation SHA-256", 64).toLowerCase(Locale.ROOT);
        if (!sha256.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("GraphQL operation SHA-256 must contain 64 hexadecimal characters");
        }
    }

    private static String text(String value, String label, int maximumLength) {
        String normalized = Objects.requireNonNull(value, label + " must not be null").trim();
        if (normalized.isEmpty()) throw new IllegalArgumentException(label + " must not be blank");
        if (normalized.length() > maximumLength) {
            throw new IllegalArgumentException(label + " must not exceed " + maximumLength + " characters");
        }
        return normalized;
    }
}
