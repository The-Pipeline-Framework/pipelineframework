package org.pipelineframework.connector.graphql;

/** Nominal, validated and deterministically encoded GraphQL response data object. */
public record GraphQlDataJson(String value) {
    public GraphQlDataJson {
        value = GraphQlJsonObjects.normalize(value, "GraphQL response data");
    }
}
