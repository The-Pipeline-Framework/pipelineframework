package org.pipelineframework.blocks.graphql.agent;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.service.ReactiveService;

/** Enforces the bound before inference and derives model-invisible mutation identity. */
@ApplicationScoped
public final class PrepareGraphQlAgentTurnService
    implements ReactiveService<GraphQlAgentState, GraphQlAgentTurnGate> {
    @Override
    public Uni<GraphQlAgentTurnGate> process(GraphQlAgentState state) {
        return Uni.createFrom().item(() -> prepare(state));
    }

    static GraphQlAgentTurnGate prepare(GraphQlAgentState state) {
        if (state.turn() >= state.maxTurns()) {
            return new GraphQlAgentTurnGate.Complete(new GraphQlAgentCompletion(
                "GraphQL agent stopped after reaching its configured turn bound."));
        }
        return new GraphQlAgentTurnGate.Ready(new GraphQlAgentTurn(state, effectKey(state.effectScope(), state.turn())));
    }

    static String effectKey(String effectScope, int logicalTurn) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(
                (effectScope + '\0' + logicalTurn).getBytes(StandardCharsets.UTF_8));
            return java.util.HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }
}
