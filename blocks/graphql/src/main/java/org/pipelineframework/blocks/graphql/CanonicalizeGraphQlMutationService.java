package org.pipelineframework.blocks.graphql;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.connector.graphql.GraphQlMutationRequest;
import org.pipelineframework.connector.graphql.GraphQlVariablesJson;
import org.pipelineframework.service.ReactiveService;

/** Revalidates and deterministically canonicalizes the portable GraphQL mutation request. */
@ApplicationScoped
public final class CanonicalizeGraphQlMutationService
    implements ReactiveService<GraphQlMutationRequest, GraphQlMutationRequest> {
    @Override
    public Uni<GraphQlMutationRequest> process(GraphQlMutationRequest request) {
        return Uni.createFrom().item(() -> canonicalize(request));
    }

    static GraphQlMutationRequest canonicalize(GraphQlMutationRequest request) {
        return new GraphQlMutationRequest(request.operationKey(), request.effectKey(),
            new GraphQlVariablesJson(request.variablesJson().value()));
    }
}
