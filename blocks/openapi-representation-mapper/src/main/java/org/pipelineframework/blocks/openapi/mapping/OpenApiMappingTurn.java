package org.pipelineframework.blocks.openapi.mapping;

import java.util.Objects;

public record OpenApiMappingTurn(OpenApiMappingState state) {
    public OpenApiMappingTurn {
        state = Objects.requireNonNull(state, "OpenAPI mapping state must not be null");
    }
}
