package org.pipelineframework.invocation;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.PipelineRunner;
import org.pipelineframework.execution.PipelineExecutionContext;
import org.pipelineframework.execution.PipelineExecutionContextHolder;
import org.pipelineframework.step.StepOneToOne;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PipelineInvocationStepsTest {

    @BeforeEach
    void resetInvocationContext() {
        PipelineInvocationContextHolder.clear();
        PipelineExecutionContextHolder.clear();
    }

    @AfterEach
    void clearInvocationContext() {
        PipelineInvocationContextHolder.clear();
        PipelineExecutionContextHolder.clear();
    }

    @Test
    void rejectsBlankCompiledDefinitionIdentity() {
        assertThrows(
            IllegalArgumentException.class,
            () -> PipelineInvocationSteps.oneToOne(new PipelineRunner(), "   ", -1, List.of()));
    }

    @Test
    void recursiveAdapterRequiresAnActiveParentInvocationContext() {
        assertThrows(
            IllegalStateException.class,
            () -> PipelineInvocationSteps.recursiveOneToOne(
                new PipelineRunner(), "agent", "continue", 2, List.of()));
    }

    @Test
    void recursiveAdapterRejectsSubscriptionFromAnotherParentInvocation() {
        PipelineInvocationContextHolder.set(PipelineInvocationContext.root(8));
        StepOneToOne<String, String> adapter = PipelineInvocationSteps.recursiveOneToOne(
            new PipelineRunner(), "agent", "continue", 2, List.of());
        PipelineInvocationContextHolder.set(PipelineInvocationContext.root(9));
        var invocation = new PipelineInvocationRuntime().invokeStepUni(
            null,
            null,
            () -> adapter.applyOneToOne("value"));

        IllegalStateException failure = assertThrows(
            IllegalStateException.class,
            () -> invocation.await().indefinitely());

        assertEquals(
            "Recursive pipeline invocation adapter does not belong to the active parent invocation",
            failure.getMessage());
    }

    @Test
    void recursiveAdapterRestoresCapturedContextAcrossTheExecutorHop() {
        PipelineRunner runner = mock(PipelineRunner.class);
        AtomicReference<PipelineExecutionContext> observedExecution = new AtomicReference<>();
        AtomicReference<PipelineInvocationContext> observedParent = new AtomicReference<>();
        AtomicReference<PipelineInvocationContext> observedChild = new AtomicReference<>();
        AtomicReference<String> subscriptionThread = new AtomicReference<>();
        when(runner.runNestedWithContext(
                any(), anyList(), anyString(), anyInt(), any(PipelineInvocationContext.class)))
            .thenAnswer(invocation -> {
                observedExecution.set(PipelineExecutionContextHolder.get().orElseThrow());
                observedParent.set(PipelineInvocationContextHolder.get().orElseThrow());
                observedChild.set(invocation.getArgument(4));
                subscriptionThread.set(Thread.currentThread().getName());
                Uni<?> input = invocation.getArgument(0);
                return new PipelineRunner.ExecutionResult(input.replaceWith("done"), null);
            });

        PipelineInvocationContext parent = PipelineInvocationContext.root(8);
        PipelineExecutionContext execution = new PipelineExecutionContext("tenant", "execution", 0);
        PipelineInvocationContextHolder.set(parent);
        PipelineExecutionContextHolder.set(execution);
        StepOneToOne<String, String> adapter = PipelineInvocationSteps.recursiveOneToOne(
            runner, "agent", "continue", 2, List.of());
        PipelineInvocationContextHolder.clear();
        PipelineExecutionContextHolder.clear();
        String callerThread = Thread.currentThread().getName();

        assertEquals("done", adapter.applyOneToOne("value").await().indefinitely());
        assertEquals(execution, observedExecution.get());
        assertEquals(parent, observedParent.get());
        assertEquals(parent.enterRecursive("agent", "continue"), observedChild.get());
        assertNotEquals(callerThread, subscriptionThread.get());
        assertTrue(PipelineInvocationContextHolder.get().isEmpty());
        assertTrue(PipelineExecutionContextHolder.get().isEmpty());
    }
}
