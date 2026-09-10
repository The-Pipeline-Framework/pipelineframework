package org.pipelineframework.blocks.openapi.mapping;

import java.util.List;

/** Reviewable authoring result; compilation remains responsible for accepting and generating the mapping. */
public record OpenApiRepresentationMappingProposal(
    String operationIdentity,
    String direction,
    String sourceSchemaFingerprint,
    String targetSchemaFingerprint,
    String mappingOptionsJson,
    boolean valid,
    List<String> diagnostics,
    int turns
) {
    public OpenApiRepresentationMappingProposal {
        operationIdentity = OpenApiMappingJson.require(operationIdentity, "OpenAPI proposal operation identity");
        direction = OpenApiMappingJson.require(direction, "OpenAPI proposal direction");
        sourceSchemaFingerprint = OpenApiMappingJson.require(sourceSchemaFingerprint, "OpenAPI source fingerprint");
        targetSchemaFingerprint = OpenApiMappingJson.require(targetSchemaFingerprint, "OpenAPI target fingerprint");
        mappingOptionsJson = OpenApiMappingJson.canonicalObject(mappingOptionsJson, "OpenAPI proposal options");
        diagnostics = List.copyOf(java.util.Objects.requireNonNull(
            diagnostics, "OpenAPI proposal diagnostics must not be null"));
        if (turns < 0 || turns > OpenApiMappingState.MAX_TURNS) {
            throw new IllegalArgumentException("OpenAPI proposal turns are invalid");
        }
    }

    static OpenApiRepresentationMappingProposal exhausted(OpenApiMappingState state) {
        return new OpenApiRepresentationMappingProposal(state.operationIdentity(), state.direction(),
            state.sourceSchemaFingerprint(), state.targetSchemaFingerprint(), "{}", false,
            state.diagnostics().isEmpty() ? List.of("No valid mapping was proposed within the turn bound")
                : state.diagnostics(), state.turn());
    }
}
