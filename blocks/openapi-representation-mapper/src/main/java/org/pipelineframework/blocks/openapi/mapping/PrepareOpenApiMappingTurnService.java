package org.pipelineframework.blocks.openapi.mapping;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.service.ReactiveService;

/** Enforces the authoring turn bound before another model Query is captured or dispatched. */
@ApplicationScoped
public final class PrepareOpenApiMappingTurnService
    implements ReactiveService<OpenApiMappingState, OpenApiMappingTurnGate> {
    @Override
    public Uni<OpenApiMappingTurnGate> process(OpenApiMappingState state) {
        return Uni.createFrom().item(() -> prepare(state));
    }

    static OpenApiMappingTurnGate prepare(OpenApiMappingState state) {
        return state.turn() >= state.maxTurns()
            ? new OpenApiMappingTurnGate.Complete(OpenApiRepresentationMappingProposal.exhausted(state))
            : new OpenApiMappingTurnGate.Ready(OpenApiMappingTurn.from(state));
    }
}
