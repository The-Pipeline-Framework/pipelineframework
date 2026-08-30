package org.pipelineframework.connector.llm;

import java.util.List;
import java.util.concurrent.CompletionStage;

/** Low-level adapter seam: one request produces one model decision and never executes a tool. */
public interface LlmDecisionClient {
    /** Whether this adapter can enforce the supplied decision schemas natively. */
    default boolean supportsNativeStructuredOutput() {
        return false;
    }

    /** Whether this adapter can enforce the supplied decision alternatives natively. */
    default boolean supportsNativeStructuredOutput(List<LlmToolDefinition> tools) {
        return supportsNativeStructuredOutput();
    }

    CompletionStage<LlmDecision> decide(LlmTurnRequest request);
}
