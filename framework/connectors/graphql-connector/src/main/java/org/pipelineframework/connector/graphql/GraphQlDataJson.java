package org.pipelineframework.connector.graphql;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/** Nominal, validated and deterministically encoded GraphQL response data object. */
public record GraphQlDataJson(@JsonValue String value) {
    @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
    public GraphQlDataJson {
        value = GraphQlJsonObjects.normalize(value, "GraphQL response data");
    }
}
