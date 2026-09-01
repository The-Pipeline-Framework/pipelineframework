package org.pipelineframework.connector.llm.langchain4j;

import java.time.Duration;
import java.util.concurrent.CompletionStage;
import org.pipelineframework.connector.ConnectionRef;
import org.pipelineframework.connector.ConnectionResolver;
import org.pipelineframework.connector.ConnectorExecutionContext;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.llm.LlmDecisionClient;
import org.pipelineframework.connector.llm.LlmProviderConfiguration;

/** One host-stack-specific implementation behind the OpenAI-compatible connector. */
interface OpenAiCompatibleClientImplementation {
    String id();

    CompletionStage<LlmDecisionClient> resolve(
        ConnectionResolver resolver,
        ConnectionRef reference,
        LlmProviderConfiguration configuration,
        ConnectorRuntimeContext runtimeContext,
        ConnectorExecutionContext executionContext,
        Duration requestTimeout);
}
