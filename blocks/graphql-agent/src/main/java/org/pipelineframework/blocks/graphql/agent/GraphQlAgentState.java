package org.pipelineframework.blocks.graphql.agent;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Trusted bounded state carried by the packaged GraphQL agent topology. */
public record GraphQlAgentState(
    String objective,
    GraphQlAgentOperationGuide operationGuide,
    String effectScope,
    int maxTurns,
    int turn,
    List<GraphQlAgentObservation> history
) {
    public static final int MAX_TURNS = 16;

    public GraphQlAgentState {
        objective = require(objective, "objective");
        operationGuide = Objects.requireNonNull(operationGuide, "GraphQL agent operation guide must not be null");
        effectScope = require(effectScope, "effectScope");
        if (maxTurns < 1 || maxTurns > MAX_TURNS) {
            throw new IllegalArgumentException("GraphQL agent maxTurns must be between 1 and " + MAX_TURNS);
        }
        if (turn < 0 || turn > maxTurns) {
            throw new IllegalArgumentException("GraphQL agent turn must be between 0 and maxTurns");
        }
        history = List.copyOf(Objects.requireNonNull(history, "GraphQL agent history must not be null"));
        if (history.size() != turn) {
            throw new IllegalArgumentException("GraphQL agent history must contain exactly one observation per completed turn");
        }
    }

    public static GraphQlAgentState start(
        String objective,
        GraphQlAgentOperationGuide operationGuide,
        String effectScope,
        int maxTurns
    ) {
        return new GraphQlAgentState(objective, operationGuide, effectScope, maxTurns, 0, List.of());
    }

    GraphQlAgentState append(GraphQlAgentObservation observation) {
        Objects.requireNonNull(observation, "GraphQL agent observation must not be null");
        if (turn >= maxTurns || observation.turn() != turn) {
            throw new IllegalStateException("GraphQL agent observation does not match the current turn");
        }
        var nextHistory = new ArrayList<>(history);
        nextHistory.add(observation);
        return new GraphQlAgentState(objective, operationGuide, effectScope, maxTurns, turn + 1, nextHistory);
    }

    private static String require(String value, String field) {
        String checked = Objects.requireNonNull(value, "GraphQL agent " + field + " must not be null").trim();
        if (checked.isEmpty()) {
            throw new IllegalArgumentException("GraphQL agent " + field + " must not be blank");
        }
        return checked;
    }
}
