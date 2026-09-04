package org.pipelineframework.blocks.graphql;

import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.command.CommandDescriptor;
import org.pipelineframework.command.CommandIdGenerator;
import org.pipelineframework.connector.graphql.GraphQlMutationRequest;

/** Reusable deterministic generator that preserves the application's semantic effect key. */
@ApplicationScoped
public final class GraphQlEffectKeyCommandIdGenerator implements CommandIdGenerator<GraphQlMutationRequest> {
    @Override
    public String commandId(CommandDescriptor descriptor, GraphQlMutationRequest input) {
        return "graphql:" + input.operationKey() + ":" + input.effectKey();
    }
}
