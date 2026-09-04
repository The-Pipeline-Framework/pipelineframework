package org.pipelineframework.blocks.graphql;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.connector.graphql.GraphQlQueryRequest;
import org.pipelineframework.connector.graphql.GraphQlVariablesJson;
import org.pipelineframework.service.ReactiveService;

/** Revalidates and deterministically canonicalizes the portable GraphQL Query request. */
@ApplicationScoped
public final class CanonicalizeGraphQlQueryService
    implements ReactiveService<GraphQlQueryRequest, GraphQlQueryRequest> {
    @Override
    public Uni<GraphQlQueryRequest> process(GraphQlQueryRequest request) {
        return Uni.createFrom().item(() -> canonicalize(request));
    }

    static GraphQlQueryRequest canonicalize(GraphQlQueryRequest request) {
        return new GraphQlQueryRequest(request.operationKey(), new GraphQlVariablesJson(request.variablesJson().value()));
    }
}
