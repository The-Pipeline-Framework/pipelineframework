package org.pipelineframework.blocks.openapi.mapping;

import java.util.Objects;

/** Model proposal envelope. It grants no authority and is deterministically validated before it is returned. */
public record OpenApiMappingCandidate(OpenApiMappingState state, String mappingOptionsJson) {
    public OpenApiMappingCandidate {
        state = Objects.requireNonNull(state, "OpenAPI mapping candidate state must not be null");
        mappingOptionsJson = OpenApiMappingJson.require(mappingOptionsJson, "OpenAPI mapping options");
    }
}
