package org.pipelineframework.connector.graphql;

import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.command.CommandDescriptor;
import org.pipelineframework.command.CommandIdGenerator;

/** Reusable deterministic generator over the application-supplied GraphQL effect key. */
@ApplicationScoped
public final class GraphQlEffectKeyCommandIdGenerator implements CommandIdGenerator<GraphQlMutationRequest> {
    @Override
    public String commandId(CommandDescriptor descriptor, GraphQlMutationRequest input) {
        return "graphql:" + component(input.operationKey()) + ":" + component(input.effectKey());
    }

    private static String component(String value) {
        return value.replace("%", "%25").replace(":", "%3A");
    }
}
