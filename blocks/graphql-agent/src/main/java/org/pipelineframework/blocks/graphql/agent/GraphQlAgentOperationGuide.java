package org.pipelineframework.blocks.graphql.agent;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/** Typed model guidance; the application connector catalogue remains the authority. */
public record GraphQlAgentOperationGuide(List<GraphQlAgentOperation> operations) {
    public static final int MAX_OPERATIONS = 64;

    public GraphQlAgentOperationGuide {
        operations = List.copyOf(Objects.requireNonNull(operations, "GraphQL agent operations must not be null"));
        if (operations.isEmpty() || operations.size() > MAX_OPERATIONS) {
            throw new IllegalArgumentException("GraphQL agent operation guide must contain between 1 and "
                + MAX_OPERATIONS + " operations");
        }
        var keys = new HashSet<String>();
        operations.forEach(operation -> {
            Objects.requireNonNull(operation, "GraphQL agent operation must not be null");
            if (!keys.add(operation.operationKey())) {
                throw new IllegalArgumentException("Duplicate GraphQL agent operation key: " + operation.operationKey());
            }
        });
    }
}
