package org.pipelineframework.connector.graphql;

/** Nominal, validated and deterministically encoded GraphQL variables object. */
public record GraphQlVariablesJson(String value) {
    public GraphQlVariablesJson {
        value = GraphQlJsonObjects.normalize(value, "GraphQL variables");
    }

    public static GraphQlVariablesJson empty() {
        return new GraphQlVariablesJson("{}");
    }
}
