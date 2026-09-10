package org.pipelineframework.blocks.openapi.mapping;

public sealed interface OpenApiMappingProgress {
    String discriminator();

    record Recur(OpenApiMappingState value) implements OpenApiMappingProgress {
        @Override public String discriminator() { return "recur"; }
    }

    record Complete(OpenApiRepresentationMappingProposal value) implements OpenApiMappingProgress {
        @Override public String discriminator() { return "complete"; }
    }
}
