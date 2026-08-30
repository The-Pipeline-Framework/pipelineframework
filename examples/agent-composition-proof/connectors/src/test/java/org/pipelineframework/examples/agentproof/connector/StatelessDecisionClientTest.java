package org.pipelineframework.examples.agentproof.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.pipelineframework.connector.llm.LlmToolDefinition;
import org.pipelineframework.connector.llm.LlmToolProposal;
import org.pipelineframework.connector.llm.LlmTurnRequest;
import org.pipelineframework.connector.llm.StructuredOutputSchemaMode;

class StatelessDecisionClientTest {
    private static final List<LlmToolDefinition> TOOLS = List.of(
        new LlmToolDefinition("lookup", "lookup", "{}"),
        new LlmToolDefinition("record", "record", "{}"),
        new LlmToolDefinition("complete", "complete", "{}"));

    @Test
    void derivesTheSameDecisionFromTheSameCanonicalStateWithoutAHiddenCursor() {
        ProofInvocationRecorder recorder = new ProofInvocationRecorder();
        var client = new ScriptedLlmQueryConnector.StatelessDecisionClient(recorder);
        LlmTurnRequest request = request("lookup");

        LlmToolProposal first = client.decide(request).toCompletableFuture().join();
        LlmToolProposal repeated = client.decide(request).toCompletableFuture().join();

        assertEquals(first, repeated);
        assertEquals(new LlmToolProposal("lookup", "{\"subject\":\"missing-proof\"}"), first);
        assertEquals(2, recorder.inferenceCount());
        assertEquals(List.of("lookup", "lookup"), recorder.phases());
    }

    @Test
    void selectsEveryTurnOnlyFromTheAuthoredPhase() {
        ProofInvocationRecorder recorder = new ProofInvocationRecorder();
        var client = new ScriptedLlmQueryConnector.StatelessDecisionClient(recorder);

        assertEquals("lookup", client.decide(request("lookup")).toCompletableFuture().join().alias());
        assertEquals("record", client.decide(request("action")).toCompletableFuture().join().alias());
        assertEquals("complete", client.decide(request("complete")).toCompletableFuture().join().alias());
        assertEquals(List.of(
            StructuredOutputSchemaMode.REQUIRED,
            StructuredOutputSchemaMode.REQUIRED,
            StructuredOutputSchemaMode.REQUIRED), recorder.structuredOutputModes());
    }

    private static LlmTurnRequest request(String phase) {
        return new LlmTurnRequest(
            "Decide exactly once.",
            "{\"evidence\":\"proof\",\"phase\":\"" + phase + "\"}",
            TOOLS,
            StructuredOutputSchemaMode.REQUIRED);
    }
}
