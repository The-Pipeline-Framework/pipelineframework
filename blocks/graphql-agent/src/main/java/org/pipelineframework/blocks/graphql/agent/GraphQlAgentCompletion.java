package org.pipelineframework.blocks.graphql.agent;

import java.util.Objects;

/** Typed completion selected by the model or produced when the turn bound is exhausted. */
public record GraphQlAgentCompletion(String summary) {
    public GraphQlAgentCompletion {
        summary = Objects.requireNonNull(summary, "GraphQL agent completion summary must not be null").trim();
        if (summary.isEmpty() || summary.length() > 4_096) {
            throw new IllegalArgumentException("GraphQL agent completion summary must contain 1 to 4096 characters");
        }
    }
}
