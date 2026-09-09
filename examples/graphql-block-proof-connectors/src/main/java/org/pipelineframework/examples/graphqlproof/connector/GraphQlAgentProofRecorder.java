package org.pipelineframework.examples.graphqlproof.connector;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.connector.llm.LlmToolDefinition;

/** Test evidence only; it never selects an operation or grants authority. */
@ApplicationScoped
public final class GraphQlAgentProofRecorder {
    private final AtomicInteger inferences = new AtomicInteger();
    private final List<Integer> turns = new CopyOnWriteArrayList<>();
    private final List<String> modelInputs = new CopyOnWriteArrayList<>();
    private final List<List<LlmToolDefinition>> toolCatalogues = new CopyOnWriteArrayList<>();

    public void record(int turn, String modelInput, List<LlmToolDefinition> tools) {
        inferences.incrementAndGet();
        turns.add(turn);
        modelInputs.add(modelInput);
        toolCatalogues.add(List.copyOf(tools));
    }

    public int inferenceCount() {
        return inferences.get();
    }

    public List<Integer> turns() {
        return List.copyOf(turns);
    }

    public List<String> modelInputs() {
        return List.copyOf(modelInputs);
    }

    public List<List<LlmToolDefinition>> toolCatalogues() {
        return List.copyOf(toolCatalogues);
    }

    public void reset() {
        inferences.set(0);
        turns.clear();
        modelInputs.clear();
        toolCatalogues.clear();
    }
}
