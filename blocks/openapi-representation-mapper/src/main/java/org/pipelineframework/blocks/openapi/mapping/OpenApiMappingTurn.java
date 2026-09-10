package org.pipelineframework.blocks.openapi.mapping;

import java.util.List;
import java.util.Objects;

/** Separates trusted carried state from the schemas and diagnostics visible to the model. */
public record OpenApiMappingTurn(
    OpenApiMappingState state,
    String sourceSchemaJson,
    String targetSchemaJson,
    List<String> diagnostics
) {
    public OpenApiMappingTurn {
        state = Objects.requireNonNull(state, "OpenAPI mapping state must not be null");
        sourceSchemaJson = OpenApiMappingJson.require(sourceSchemaJson, "OpenAPI source schema");
        targetSchemaJson = OpenApiMappingJson.require(targetSchemaJson, "OpenAPI target schema");
        diagnostics = List.copyOf(Objects.requireNonNull(
            diagnostics, "OpenAPI mapping diagnostics must not be null"));
    }

    static OpenApiMappingTurn from(OpenApiMappingState state) {
        return new OpenApiMappingTurn(state, state.sourceSchemaJson(), state.targetSchemaJson(), state.diagnostics());
    }
}
