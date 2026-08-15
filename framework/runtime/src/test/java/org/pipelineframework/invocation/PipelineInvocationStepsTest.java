package org.pipelineframework.invocation;

import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.PipelineRunner;

import static org.junit.jupiter.api.Assertions.assertThrows;

class PipelineInvocationStepsTest {

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
}
