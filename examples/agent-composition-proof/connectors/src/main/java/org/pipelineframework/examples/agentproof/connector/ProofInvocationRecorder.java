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
    private final AtomicInteger transitions = new AtomicInteger();
    private final List<String> phases = new CopyOnWriteArrayList<>();
    private final List<StructuredOutputSchemaMode> structuredOutputModes = new CopyOnWriteArrayList<>();
    private final List<String> executionIds = new CopyOnWriteArrayList<>();
    private final List<String> observations = new CopyOnWriteArrayList<>();

    public void recordInference(String phase, StructuredOutputSchemaMode mode) {
        phases.add(phase);
        structuredOutputModes.add(mode);
        inferences.incrementAndGet();
    }

    public void recordQuery(ConnectorExecutionContext context) {
        context.executionId().ifPresent(executionIds::add);
        queries.incrementAndGet();
    }

    public void recordCommand(ConnectorExecutionContext context) {
        context.executionId().ifPresent(executionIds::add);
        commands.incrementAndGet();
    }

    public void recordTransition(String observation) {
        observations.add(observation);
        transitions.incrementAndGet();
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

    public int transitionCount() {
        return transitions.get();
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

    public List<String> observations() {
        return List.copyOf(observations);
    }

    public void reset() {
        inferences.set(0);
        queries.set(0);
        commands.set(0);
        transitions.set(0);
        phases.clear();
        structuredOutputModes.clear();
        executionIds.clear();
        observations.clear();
    }
}
