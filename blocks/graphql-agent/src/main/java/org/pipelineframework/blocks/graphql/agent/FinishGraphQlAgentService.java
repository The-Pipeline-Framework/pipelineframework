package org.pipelineframework.blocks.graphql.agent;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.service.ReactiveService;

@ApplicationScoped
public final class FinishGraphQlAgentService
    implements ReactiveService<GraphQlAgentCompletion, GraphQlAgentCompletion> {
    @Override
    public Uni<GraphQlAgentCompletion> process(GraphQlAgentCompletion completion) {
        return Uni.createFrom().item(completion);
    }
}
