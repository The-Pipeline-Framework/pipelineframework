package org.pipelineframework.examples.agentproof.connector;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.llm.LlmDecisionClient;
import org.pipelineframework.connector.llm.LlmProviderConfiguration;
import org.pipelineframework.connector.llm.LlmQueryConnectorProvider;
import org.pipelineframework.connector.llm.LlmToolDefinition;
import org.pipelineframework.connector.llm.LlmToolProposal;
import org.pipelineframework.connector.llm.LlmTurnRequest;

/** Offline adapter whose decision is a pure function of the canonical AgentState input. */
@ApplicationScoped
public class ScriptedLlmQueryConnector extends LlmQueryConnectorProvider {
    public static final ConnectorProviderId PROVIDER_ID = ConnectorProviderId.of("proof.llm");

    @Inject
    ProofInvocationRecorder recorder;

    public ScriptedLlmQueryConnector() {
        super(PROVIDER_ID);
    }

    @Override
    protected LlmDecisionClient createClient(
        LlmProviderConfiguration configuration,
        ConnectorRuntimeContext context
    ) {
        return new StatelessDecisionClient(Objects.requireNonNull(
            recorder, "proof invocation recorder must be injected"));
    }

    public static final class StatelessDecisionClient implements LlmDecisionClient {
        private static final ObjectMapper JSON = PipelineJson.mapper();
        private final ProofInvocationRecorder recorder;

        public StatelessDecisionClient(ProofInvocationRecorder recorder) {
            this.recorder = Objects.requireNonNull(recorder, "proof invocation recorder must not be null");
        }

        @Override
        public boolean supportsNativeStructuredOutput(List<LlmToolDefinition> tools) {
            return true;
        }

        @Override
        public CompletionStage<LlmToolProposal> decide(LlmTurnRequest request) {
            try {
                JsonNode state = JSON.readTree(request.applicationStateJson());
                String phase = state.path("phase").asText();
                recorder.recordInference(phase, request.structuredOutputSchema());
                return CompletableFuture.completedFuture(decision(phase));
            } catch (Exception failure) {
                return CompletableFuture.failedFuture(failure);
            }
        }

        private static LlmToolProposal decision(String phase) {
            return switch (phase) {
                case "lookup" -> new LlmToolProposal("lookup", "{\"subject\":\"missing-proof\"}");
                case "action" -> new LlmToolProposal("record", "{\"action\":\"record-proof\"}");
                case "complete" -> new LlmToolProposal(
                    "complete", "{\"summary\":\"query-not-found then command-succeeded\",\"turns\":3}");
                default -> throw new IllegalArgumentException("unsupported proof phase: " + phase);
            };
        }
    }
}
