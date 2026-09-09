package org.pipelineframework.blocks.graphql.agent;

public sealed interface GraphQlAgentTurnGate {
    String discriminator();

    record Ready(GraphQlAgentTurn value) implements GraphQlAgentTurnGate {
        @Override public String discriminator() { return "ready"; }
    }

    record Complete(GraphQlAgentCompletion value) implements GraphQlAgentTurnGate {
        @Override public String discriminator() { return "complete"; }
    }
}
