package org.pipelineframework.connector.llm;

import java.util.concurrent.CompletionStage;

/** Low-level adapter seam: one request produces one model decision and never executes a tool. */
public interface LlmDecisionClient {
    CompletionStage<LlmToolProposal> decide(LlmTurnRequest request);
}
