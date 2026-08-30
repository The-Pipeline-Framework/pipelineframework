package org.pipelineframework.connector.llm;

import java.util.concurrent.CompletionStage;

import org.pipelineframework.connector.ConnectorExecutionContext;

/** Resolves the low-level LLM client for one live connector invocation. */
@FunctionalInterface
public interface LlmDecisionClientResolver {
    CompletionStage<LlmDecisionClient> resolve(ConnectorExecutionContext executionContext);
}
