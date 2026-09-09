package org.pipelineframework.blocks.graphql.agent;

public sealed interface GraphQlAgentProgress {
    String discriminator();

    record Recur(GraphQlAgentState value) implements GraphQlAgentProgress {
        @Override public String discriminator() { return "recur"; }
    }

    record Complete(GraphQlAgentCompletion value) implements GraphQlAgentProgress {
        @Override public String discriminator() { return "complete"; }
    }
}
