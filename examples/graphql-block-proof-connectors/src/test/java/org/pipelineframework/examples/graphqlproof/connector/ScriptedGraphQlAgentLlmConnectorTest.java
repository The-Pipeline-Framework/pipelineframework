package org.pipelineframework.examples.graphqlproof.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;

import org.pipelineframework.connector.llm.LlmTurnRequest;
import org.pipelineframework.connector.llm.LlmToolDefinition;
import org.pipelineframework.connector.llm.StructuredOutputSchemaMode;

class ScriptedGraphQlAgentLlmConnectorTest {
    @Test
    void selectsPersistedOperationKeysThenCompletesFromTheLogicalTurn() {
        var recorder = new GraphQlAgentProofRecorder();
        var client = new ScriptedGraphQlAgentLlmConnector.StatelessDecisionClient(recorder);

        assertEquals("graphql_query", client.decide(request(0)).toCompletableFuture().join().proposal().alias());
        assertEquals("graphql_mutation", client.decide(request(1)).toCompletableFuture().join().proposal().alias());
        assertEquals("complete", client.decide(request(2)).toCompletableFuture().join().proposal().alias());
        assertEquals(List.of(0, 1, 2), recorder.turns());
    }

    private static LlmTurnRequest request(int turn) {
        return new LlmTurnRequest("decide", "{\"state\":{\"turn\":" + turn + "}}",
            List.of(new LlmToolDefinition("complete", "complete", "{\"type\":\"object\"}")),
            StructuredOutputSchemaMode.REQUIRED);
    }
}
