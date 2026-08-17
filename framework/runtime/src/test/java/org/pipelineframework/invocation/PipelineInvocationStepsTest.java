package org.pipelineframework.invocation;

import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.PipelineRunner;
import org.pipelineframework.step.StepOneToOne;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

class PipelineInvocationStepsTest {

    @BeforeEach
    void resetInvocationContext() {
        PipelineInvocationContextHolder.clear();
    }

    @AfterEach
    void clearInvocationContext() {
        PipelineInvocationContextHolder.clear();
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
}
