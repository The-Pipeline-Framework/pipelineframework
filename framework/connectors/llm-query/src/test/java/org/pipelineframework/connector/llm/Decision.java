package org.pipelineframework.connector.llm;

public sealed interface Decision permits Decision.AskUser, Decision.Call, Decision.Complete {
    String discriminator();

    record Call(AgentCall value) implements Decision {
        @Override
        public String discriminator() {
            return "call";
        }
    }

    record AskUser(org.pipelineframework.connector.llm.AskUser value) implements Decision {
        @Override
        public String discriminator() {
            return "askUser";
        }
    }

    record Complete(CompleteResult value) implements Decision {
        @Override
        public String discriminator() {
            return "complete";
        }
    }
}
