package org.pipelineframework.blocks.graphql.agent;

import java.util.List;
import java.util.Objects;

import org.pipelineframework.connector.graphql.GraphQlError;
import org.pipelineframework.connector.graphql.GraphQlVariablesJson;

/** Bounded provider-neutral observation retained in trusted GraphQL agent state. */
public record GraphQlAgentObservation(
    int turn,
    String operationKey,
    String kind,
    GraphQlVariablesJson variablesJson,
    String outcome,
    String code,
    String dataJson,
    List<GraphQlError> errors
) {
    public GraphQlAgentObservation {
        if (turn < 0 || turn >= GraphQlAgentState.MAX_TURNS) {
            throw new IllegalArgumentException("GraphQL agent observation turn is outside the supported range");
        }
        operationKey = require(operationKey, "operationKey");
        kind = require(kind, "kind");
        variablesJson = Objects.requireNonNull(variablesJson, "GraphQL agent observation variables must not be null");
        outcome = require(outcome, "outcome");
        code = Objects.requireNonNull(code, "GraphQL agent observation code must not be null");
        dataJson = Objects.requireNonNull(dataJson, "GraphQL agent observation dataJson must not be null");
        errors = List.copyOf(Objects.requireNonNull(errors, "GraphQL agent observation errors must not be null"));
    }

    private static String require(String value, String field) {
        String checked = Objects.requireNonNull(value, "GraphQL agent observation " + field + " must not be null").trim();
        if (checked.isEmpty()) {
            throw new IllegalArgumentException("GraphQL agent observation " + field + " must not be blank");
        }
        return checked;
    }
}
