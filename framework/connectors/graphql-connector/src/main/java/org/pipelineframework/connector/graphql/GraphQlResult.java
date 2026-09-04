package org.pipelineframework.connector.graphql;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Stable Block boundary after deterministic response normalization. */
public record GraphQlResult(Optional<GraphQlDataJson> data, List<GraphQlError> errors) {
    public GraphQlResult {
        data = Objects.requireNonNull(data, "GraphQL result data must not be null");
        errors = List.copyOf(Objects.requireNonNull(errors, "GraphQL result errors must not be null"));
        if (errors.size() > GraphQlResponse.MAX_ERRORS) {
            throw new IllegalArgumentException(
                "GraphQL result must not contain more than " + GraphQlResponse.MAX_ERRORS + " errors");
        }
        if (data.isEmpty() && errors.isEmpty()) {
            throw new IllegalArgumentException("GraphQL result must contain data or errors");
        }
    }

    public static GraphQlResult from(GraphQlResponse response) {
        Objects.requireNonNull(response, "GraphQL response must not be null");
        return new GraphQlResult(response.data(), response.errors());
    }
}
