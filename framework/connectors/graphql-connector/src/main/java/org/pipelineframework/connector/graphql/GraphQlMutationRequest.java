package org.pipelineframework.connector.graphql;

import java.util.Objects;

/** Portable mutation request with an application-supplied semantic effect key. */
public record GraphQlMutationRequest(
    String operationKey,
    String effectKey,
    GraphQlVariablesJson variablesJson
) {
    public GraphQlMutationRequest {
        operationKey = GraphQlValues.text(operationKey, "GraphQL operation key", 256);
        effectKey = GraphQlValues.text(effectKey, "GraphQL effect key", 512);
        variablesJson = Objects.requireNonNull(variablesJson, "GraphQL variables must not be null");
    }
}
