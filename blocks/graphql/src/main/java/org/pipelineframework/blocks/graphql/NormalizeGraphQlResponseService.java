package org.pipelineframework.blocks.graphql;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.connector.graphql.GraphQlResponse;
import org.pipelineframework.connector.graphql.GraphQlResult;
import org.pipelineframework.service.ReactiveService;

/** Closes the reusable Block on its stable provider-neutral result boundary. */
@ApplicationScoped
public final class NormalizeGraphQlResponseService implements ReactiveService<GraphQlResponse, GraphQlResult> {
    @Override
    public Uni<GraphQlResult> process(GraphQlResponse response) {
        return Uni.createFrom().item(() -> GraphQlResult.from(response));
    }
}
