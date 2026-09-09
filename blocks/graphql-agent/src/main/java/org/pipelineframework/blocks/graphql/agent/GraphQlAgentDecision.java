package org.pipelineframework.blocks.graphql.agent;

public sealed interface GraphQlAgentDecision {
    String discriminator();

    record Call(AgentCall value) implements GraphQlAgentDecision {
        @Override public String discriminator() { return "call"; }
    }

    record Complete(GraphQlAgentCompletion value) implements GraphQlAgentDecision {
        @Override public String discriminator() { return "complete"; }
    }
}
