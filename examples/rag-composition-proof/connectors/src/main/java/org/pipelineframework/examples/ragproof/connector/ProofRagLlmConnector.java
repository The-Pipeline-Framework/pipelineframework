package org.pipelineframework.examples.ragproof.connector;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.llm.LlmDecision;
import org.pipelineframework.connector.llm.LlmDecisionClient;
import org.pipelineframework.connector.llm.LlmDecisionClientResolver;
import org.pipelineframework.connector.llm.LlmProviderConfiguration;
import org.pipelineframework.connector.llm.LlmQueryConnectorProvider;
import org.pipelineframework.connector.llm.LlmToolDefinition;
import org.pipelineframework.connector.llm.LlmToolProposal;
import org.pipelineframework.connector.llm.LlmTurnRequest;

/** Offline direct-completion LLM provider for the RAG proof. */
@ApplicationScoped
public class ProofRagLlmConnector extends LlmQueryConnectorProvider {
    public static final ConnectorProviderId PROVIDER_ID = ConnectorProviderId.of("proof.rag.llm");

    @Inject
    RagProofInvocationRecorder recorder;

    public ProofRagLlmConnector() { super(PROVIDER_ID); }

    @Override
    protected LlmDecisionClientResolver createClientResolver(
        LlmProviderConfiguration configuration,
        ConnectorRuntimeContext context
    ) {
        LlmDecisionClient client = new Client(Objects.requireNonNull(recorder, "proof recorder must be injected"));
        return ignored -> CompletableFuture.completedStage(client);
    }

    static final class Client implements LlmDecisionClient {
        private static final ObjectMapper JSON = PipelineJson.mapper();
        private final RagProofInvocationRecorder recorder;

        Client(RagProofInvocationRecorder recorder) { this.recorder = recorder; }
        @Override public boolean supportsNativeStructuredOutput(List<LlmToolDefinition> tools) { return true; }

        @Override
        public CompletionStage<LlmDecision> decide(LlmTurnRequest request) {
            try {
                JsonNode context = JSON.readTree(request.applicationStateJson());
                String questionId = context.path("questionId").asText();
                String question = context.path("question").asText();
                String passages = java.util.stream.StreamSupport.stream(context.path("passages").spliterator(), false)
                    .map(JsonNode::asText).collect(Collectors.joining(" | "));
                String answer = passages.isBlank() ? "No matching context for: " + question : passages;
                recorder.answer();
                String arguments = JSON.writeValueAsString(java.util.Map.of(
                    "questionId", questionId, "text", answer));
                return CompletableFuture.completedStage(new LlmDecision(new LlmToolProposal("complete", arguments)));
            } catch (Exception failure) {
                return CompletableFuture.failedStage(failure);
            }
        }
    }
}
