package org.pipelineframework.examples.agentproof;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Provider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.pipelineframework.config.PipelineConfig;
import org.pipelineframework.connector.llm.StructuredOutputSchemaMode;
import org.pipelineframework.examples.agentproof.connector.ProofInvocationRecorder;
import org.pipelineframework.examples.agentproof.domain.AgentState;
import org.pipelineframework.examples.agentproof.domain.ApplicationResult;
import org.pipelineframework.execution.PipelineExecutionContext;
import org.pipelineframework.execution.PipelineExecutionContextHolder;
import org.pipelineframework.invocation.PipelineRecursionLimitExceededException;
import org.pipelineframework.invocation.PipelineInvocationRuntime;
import org.pipelineframework.step.StepOneToOne;

@QuarkusTest
class CallableLoopProofIT {
    private String executionId;

    @Inject
    @Any
    Instance<StepOneToOne<AgentState, ApplicationResult>> pipelines;

    @Inject
    ProofInvocationRecorder recorder;

    @Inject
    PipelineConfig config;

    @Inject
    PipelineInvocationRuntime invocationRuntime;

    @BeforeEach
    void resetEvidence() {
        recorder.reset();
        config.maxRecursiveDepth(4);
        String identity = UUID.randomUUID().toString();
        executionId = "proof-execution-" + identity;
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("proof-tenant-" + identity, executionId, 0));
    }

    @AfterEach
    void clearContext() {
        PipelineExecutionContextHolder.clear();
    }

    @Test
    void composesApplicationBoundCallablesWithIndependentTrustedProjections() {
        AtomicInteger terminalResults = new AtomicInteger();
        ApplicationResult result = invoke(new AgentState("proof-scope", "effect-1", "none", "lookup"))
            .invoke(ignored -> terminalResults.incrementAndGet())
            .await().indefinitely();

        assertEquals(new ApplicationResult("query-not-found then command-succeeded", 3), result);
        assertEquals(3, recorder.inferenceCount());
        assertEquals(List.of("lookup", "action", "complete"), recorder.phases());
        assertEquals(List.of(
            StructuredOutputSchemaMode.REQUIRED,
            StructuredOutputSchemaMode.REQUIRED,
            StructuredOutputSchemaMode.REQUIRED), recorder.structuredOutputModes());
        assertEquals(1, recorder.queryCount());
        assertEquals(1, recorder.commandCount());
        assertEquals(1, terminalResults.get());
        assertEquals(List.of("effect-1"), recorder.effectKeys());
        assertTrue(recorder.modelInputs().stream().allMatch(input -> !input.contains("effectScope")));
        assertTrue(recorder.modelInputs().stream().allMatch(input -> !input.contains("nextEffectKey")));
        assertTrue(recorder.modelInputs().stream().allMatch(input -> !input.contains("proof-scope")));
        assertTrue(recorder.modelInputs().stream().allMatch(input -> !input.contains("effect-1")));
        assertEquals(List.of(executionId, executionId), recorder.executionIds());
    }

    @Test
    void failsBeforeExceedingTheConfiguredRecursiveDepth() {
        config.maxRecursiveDepth(1);

        RuntimeException failure = assertThrows(RuntimeException.class, () ->
            invoke(new AgentState("proof-scope", "effect-1", "none", "lookup")).await().indefinitely());
        PipelineRecursionLimitExceededException limit = findCause(
            failure, PipelineRecursionLimitExceededException.class);

        assertEquals("org.pipelineframework.examples.callable/callable-loop-proof", limit.definitionId());
        assertEquals("Recur", limit.callsiteId());
        assertEquals(2, limit.attemptedDepth());
        assertEquals(1, limit.maximumDepth());
        assertEquals(2, recorder.inferenceCount());
        assertEquals(1, recorder.queryCount());
        assertEquals(1, recorder.commandCount());
    }

    @Test
    void generatedMetadataMakesTheCompositionFiniteAndInspectable() throws Exception {
        String contract = metadata("pipeline-contract.json");
        String order = metadata("order.json");
        String branching = metadata("branching.json");
        String bindings = metadata("connector-bindings.json");

        assertTrue(contract.contains("tpf.llm.AgentCall"), contract);
        assertTrue(contract.contains("tpf.connector.OperationObservation"), contract);
        assertTrue(bindings.contains("evidence.lookup"), bindings);
        assertTrue(bindings.contains("evidence.record"), bindings);
        assertTrue(bindings.contains("\"alias\": \"lookup\""), bindings);
        assertTrue(bindings.contains("\"alias\": \"record\""), bindings);
        assertTrue(bindings.contains("\"trustedArguments\""), bindings);
        assertTrue(bindings.contains("\"effectKey\": \"nextEffectKey\""), bindings);
        assertTrue(contract.contains("OperationDispatchDescriptor"), contract);
        assertTrue(contract.contains("org.pipelineframework.examples.callable/callable-loop-proof"), contract);
        assertTrue(contract.contains("resolvedCallables"), contract);
        assertTrue(contract.contains("callable-loop-proof"), contract);
        assertTrue(contract.length() < 50_000, "recursive metadata must remain finite");
        assertTrue(order.contains("PipelineInvocation_"), order);
        assertTrue(order.length() < 500, "root order must remain finite");
        assertTrue(contract.contains("Decide"), contract);
        assertTrue(contract.contains("Invoke proposal"), contract);
        assertTrue(contract.contains("Reduce observation"), contract);
        assertTrue(contract.contains("Recur"), contract);
        assertTrue(branching.contains("AgentCall"), branching);
        assertTrue(branching.contains("ApplicationResult"), branching);
        assertFalse(contract.contains("proof-scope"));
        assertFalse(contract.contains("effect-1"));
        assertFalse(contract.contains("deterministic-proof"));
        assertFalse(contract.contains("offline"));
        assertFalse((contract + order + branching).contains("AgentRuntime"));
        assertFalse((contract + order + branching).contains("\"kind\":\"dispatch\""));
    }

    private static String metadata(String name) throws Exception {
        String resource = "META-INF/pipeline/" + name;
        try (InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource)) {
            if (stream == null) {
                throw new AssertionError("generated metadata resource not found: " + resource);
            }
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private StepOneToOne<AgentState, ApplicationResult> pipeline() {
        return pipelines.stream()
            .filter(candidate -> Arrays.stream(candidate.getClass().getDeclaredFields())
                .anyMatch(field -> field.getName().equals("child3") && !field.getType().equals(Provider.class)))
            .findFirst()
            .orElseThrow(() -> new AssertionError("generated root callable-loop-proof bean not found among "
                + pipelines.stream().map(candidate -> candidate.getClass().getName()).toList()));
    }

    private io.smallrye.mutiny.Uni<ApplicationResult> invoke(AgentState state) {
        return invocationRuntime.invokeStepUni(null, null, () -> pipeline().applyOneToOne(state));
    }

    private static <T extends Throwable> T findCause(Throwable failure, Class<T> type) {
        Throwable current = failure;
        while (current != null) {
            if (type.isInstance(current)) {
                return type.cast(current);
            }
            current = current.getCause();
        }
        throw new AssertionError("missing cause " + type.getName(), failure);
    }
}
