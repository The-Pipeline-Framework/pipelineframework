package org.pipelineframework.connector.graphql;

import java.util.List;
import java.util.Objects;

/** Bounded and sanitized provider-neutral GraphQL application error. */
public record GraphQlError(String code, List<String> path, String message) {
    public static final int MAX_CODE_LENGTH = 128;
    public static final int MAX_PATH_SEGMENTS = 32;
    public static final int MAX_MESSAGE_LENGTH = 1_024;

    public GraphQlError {
        code = GraphQlValues.code(code);
        path = List.copyOf(Objects.requireNonNull(path, "GraphQL error path must not be null"));
        if (path.size() > MAX_PATH_SEGMENTS) {
            throw new IllegalArgumentException("GraphQL error path must not exceed " + MAX_PATH_SEGMENTS + " segments");
        }
        path = path.stream()
            .map(segment -> GraphQlValues.text(segment, "GraphQL error path segment", 128))
            .toList();
        message = GraphQlValues.message(message);
    }
}
