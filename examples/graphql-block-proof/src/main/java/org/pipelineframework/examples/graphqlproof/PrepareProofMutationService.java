package org.pipelineframework.examples.graphqlproof;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.connector.graphql.GraphQlMutationRequest;
import org.pipelineframework.connector.graphql.GraphQlResult;
import org.pipelineframework.connector.graphql.GraphQlVariablesJson;
import org.pipelineframework.service.ReactiveService;

/** Application-owned choice of persisted mutation and semantic effect identity for the proof. */
@ApplicationScoped
public final class PrepareProofMutationService implements ReactiveService<GraphQlResult, GraphQlMutationRequest> {
    @Override
    public Uni<GraphQlMutationRequest> process(GraphQlResult queryResult) {
        return Uni.createFrom().item(new GraphQlMutationRequest(
            "customer.update",
            "proof/customer-7/name-ada",
            new GraphQlVariablesJson("{\"id\":\"7\",\"name\":\"Ada\"}")));
    }
}
