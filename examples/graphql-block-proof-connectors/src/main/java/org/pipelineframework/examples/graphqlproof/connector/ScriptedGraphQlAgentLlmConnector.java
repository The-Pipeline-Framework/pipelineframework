package org.pipelineframework.examples.graphqlproof.connector;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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

/** Offline decision provider proving the packaged GraphQL agent topology deterministically. */
@ApplicationScoped
public final class ScriptedGraphQlAgentLlmConnector extends LlmQueryConnectorProvider {
    public static final ConnectorProviderId PROVIDER_ID = ConnectorProviderId.of("proof.graphql.llm");

    @Inject
    GraphQlAgentProofRecorder recorder;

    public ScriptedGraphQlAgentLlmConnector() {
        super(PROVIDER_ID);
    }

    @Override
    protected LlmDecisionClientResolver createClientResolver(
        LlmProviderConfiguration configuration,
        ConnectorRuntimeContext context
    ) {
        LlmDecisionClient client = new StatelessDecisionClient(
            Objects.requireNonNull(recorder, "GraphQL proof recorder must be injected"));
        return ignored -> CompletableFuture.completedStage(client);
    }

    static final class StatelessDecisionClient implements LlmDecisionClient {
        private static final ObjectMapper JSON = PipelineJson.mapper();
        private final GraphQlAgentProofRecorder recorder;

        StatelessDecisionClient(GraphQlAgentProofRecorder recorder) {
            this.recorder = Objects.requireNonNull(recorder, "GraphQL proof recorder must not be null");
        }

        @Override
        public boolean supportsNativeStructuredOutput(List<LlmToolDefinition> tools) {
            return true;
        }

        @Override
        public CompletionStage<LlmDecision> decide(LlmTurnRequest request) {
            try {
                JsonNode input = JSON.readTree(request.applicationStateJson());
                int turn = input.path("state").path("turn").asInt(-1);
                recorder.record(turn, request.applicationStateJson(), request.tools());
                return CompletableFuture.completedStage(new LlmDecision(proposal(turn)));
            } catch (Exception failure) {
                return CompletableFuture.failedStage(failure);
            }
        }

        private static LlmToolProposal proposal(int turn) throws Exception {
            return switch (turn) {
                case 0 -> new LlmToolProposal("graphql_query", arguments(
                    "customer.lookup", "{\"id\":\"customer-7\"}"));
                case 1 -> new LlmToolProposal("graphql_mutation", arguments(
                    "customer.update", "{\"id\":\"customer-7\",\"name\":\"Ada Lovelace\"}"));
                case 2 -> new LlmToolProposal("complete",
                    "{\"summary\":\"Customer lookup and update completed through persisted GraphQL operations.\"}");
                default -> throw new IllegalArgumentException("unsupported GraphQL proof turn: " + turn);
            };
        }

        private static String arguments(String operationKey, String variablesJson) throws Exception {
            ObjectNode arguments = JSON.createObjectNode();
            arguments.put("operationKey", operationKey);
            arguments.put("variablesJson", variablesJson);
            return JSON.writeValueAsString(arguments);
        }
    }
}
