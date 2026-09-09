package org.pipelineframework.examples.agentproof.connector;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.connector.ConnectorExecutionContext;
import org.pipelineframework.connector.llm.StructuredOutputSchemaMode;

/** Test evidence only; these observations never select or alter provider behavior. */
@ApplicationScoped
public class ProofInvocationRecorder {
    private final AtomicInteger inferences = new AtomicInteger();
    private final AtomicInteger queries = new AtomicInteger();
    private final AtomicInteger commands = new AtomicInteger();
    private final List<String> phases = new CopyOnWriteArrayList<>();
    private final List<StructuredOutputSchemaMode> structuredOutputModes = new CopyOnWriteArrayList<>();
    private final List<String> executionIds = new CopyOnWriteArrayList<>();
    private final List<String> modelInputs = new CopyOnWriteArrayList<>();
    private final List<String> effectKeys = new CopyOnWriteArrayList<>();

    public void recordInference(String phase, StructuredOutputSchemaMode mode, String modelInput) {
        phases.add(phase);
        structuredOutputModes.add(mode);
        modelInputs.add(modelInput);
        inferences.incrementAndGet();
    }

    public void recordQuery(ConnectorExecutionContext context) {
        context.executionId().ifPresent(executionIds::add);
        queries.incrementAndGet();
    }

    public void recordCommand(ConnectorExecutionContext context, String effectKey) {
        context.executionId().ifPresent(executionIds::add);
        effectKeys.add(effectKey);
        commands.incrementAndGet();
    }

    public int inferenceCount() {
        return inferences.get();
    }

    public int queryCount() {
        return queries.get();
    }

    public int commandCount() {
        return commands.get();
    }

    public List<String> phases() {
        return List.copyOf(phases);
    }

    public List<StructuredOutputSchemaMode> structuredOutputModes() {
        return List.copyOf(structuredOutputModes);
    }

    public List<String> executionIds() {
        return List.copyOf(executionIds);
    }

    public List<String> modelInputs() {
        return List.copyOf(modelInputs);
    }

    public List<String> effectKeys() {
        return List.copyOf(effectKeys);
    }

    public void reset() {
        inferences.set(0);
        queries.set(0);
        commands.set(0);
        phases.clear();
        structuredOutputModes.clear();
        executionIds.clear();
        modelInputs.clear();
        effectKeys.clear();
    }
}
