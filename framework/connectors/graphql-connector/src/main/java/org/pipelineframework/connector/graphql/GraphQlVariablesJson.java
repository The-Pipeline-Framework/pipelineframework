package org.pipelineframework.connector.graphql;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/** Nominal, validated and deterministically encoded GraphQL variables object. */
public record GraphQlVariablesJson(@JsonValue String value) {
    @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
    public GraphQlVariablesJson {
        value = GraphQlJsonObjects.normalize(value, "GraphQL variables");
    }

    public static GraphQlVariablesJson empty() {
        return new GraphQlVariablesJson("{}");
    }
}
