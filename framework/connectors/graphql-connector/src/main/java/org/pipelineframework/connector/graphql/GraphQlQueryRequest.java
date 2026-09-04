package org.pipelineframework.connector.graphql;

import java.util.Objects;

/** Portable Query request. The GraphQL document is selected by an application-owned operation key. */
public record GraphQlQueryRequest(String operationKey, GraphQlVariablesJson variablesJson) {
    public GraphQlQueryRequest {
        operationKey = GraphQlValues.text(operationKey, "GraphQL operation key", 256);
        variablesJson = Objects.requireNonNull(variablesJson, "GraphQL variables must not be null");
    }
}
