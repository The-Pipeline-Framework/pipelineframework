package org.pipelineframework.connector.graphql;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Normalized GraphQL response, including valid responses containing application errors. */
public record GraphQlResponse(Optional<GraphQlDataJson> data, List<GraphQlError> errors) {
    public static final int MAX_ERRORS = 32;

    public GraphQlResponse {
        data = Objects.requireNonNull(data, "GraphQL response data must not be null");
        errors = List.copyOf(Objects.requireNonNull(errors, "GraphQL errors must not be null"));
        if (errors.size() > MAX_ERRORS) {
            throw new IllegalArgumentException("GraphQL response must not contain more than " + MAX_ERRORS + " errors");
        }
        if (data.isEmpty() && errors.isEmpty()) {
            throw new IllegalArgumentException("GraphQL response must contain data or errors");
        }
    }
}
