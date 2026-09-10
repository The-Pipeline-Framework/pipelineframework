package org.pipelineframework.blocks.openapi.mapping;

public sealed interface OpenApiMappingTurnGate {
    String discriminator();

    record Ready(OpenApiMappingTurn value) implements OpenApiMappingTurnGate {
        @Override public String discriminator() { return "ready"; }
    }

    record Complete(OpenApiRepresentationMappingProposal value) implements OpenApiMappingTurnGate {
        @Override public String discriminator() { return "complete"; }
    }
}
