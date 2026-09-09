package org.pipelineframework.examples.agentproof.domain;

public sealed interface AgentDecision {
    String discriminator();

    record Call(AgentCall value) implements AgentDecision {
        @Override
        public String discriminator() {
            return "call";
        }
    }

    record Complete(ApplicationResult value) implements AgentDecision {
        @Override
        public String discriminator() {
            return "complete";
        }
    }
}
