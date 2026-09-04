package org.pipelineframework.connector.graphql.smallrye;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import org.pipelineframework.connector.ConnectionRef;

/** Application-owned connection reference and persisted-operation allowlist. */
public record GraphQlProviderConfiguration(
    ConnectionRef connection,
    Map<String, GraphQlPersistedOperation> operations
) {
    public GraphQlProviderConfiguration {
        connection = Objects.requireNonNull(connection, "GraphQL connection must not be null");
        Objects.requireNonNull(operations, "GraphQL operations must not be null");
        if (operations.isEmpty()) throw new IllegalArgumentException("GraphQL operations must not be empty");
        Map<String, GraphQlPersistedOperation> normalized = new TreeMap<>();
        operations.forEach((key, value) -> {
            String operationKey = Objects.requireNonNull(key, "GraphQL operation key must not be null").trim();
            if (!operationKey.matches("[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*")) {
                throw new IllegalArgumentException(
                    "GraphQL operation key must be a stable lowercase dotted identity: " + operationKey);
            }
            if (normalized.put(operationKey,
                Objects.requireNonNull(value, "GraphQL persisted operation must not be null")) != null) {
                throw new IllegalArgumentException("Duplicate GraphQL operation key: " + operationKey);
            }
        });
        operations = Collections.unmodifiableMap(normalized);
    }
}
