package org.pipelineframework.blocks.graphql.agent;

import java.util.Objects;

/** One decision input, with trusted mutation identity derived before model projection. */
public record GraphQlAgentTurn(GraphQlAgentState state, String nextEffectKey) {
    public GraphQlAgentTurn {
        state = Objects.requireNonNull(state, "GraphQL agent state must not be null");
        nextEffectKey = Objects.requireNonNull(nextEffectKey, "GraphQL agent effect key must not be null").trim();
        if (nextEffectKey.isEmpty()) {
            throw new IllegalArgumentException("GraphQL agent effect key must not be blank");
        }
    }
}
