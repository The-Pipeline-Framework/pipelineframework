package org.pipelineframework.connector.llm;

public sealed interface Decision permits Decision.Call, Decision.Complete {
    String discriminator();

    record Call(AgentCall value) implements Decision {
        @Override
        public String discriminator() {
            return "call";
        }
    }

    record Complete(CompleteResult value) implements Decision {
        @Override
        public String discriminator() {
            return "complete";
        }
    }
}
