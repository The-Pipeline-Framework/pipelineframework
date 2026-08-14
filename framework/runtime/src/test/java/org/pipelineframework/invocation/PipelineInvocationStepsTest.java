package org.pipelineframework.invocation;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.pipelineframework.PipelineRunner;

import static org.junit.jupiter.api.Assertions.assertThrows;

class PipelineInvocationStepsTest {

    @Test
    void rejectsBlankCompiledDefinitionIdentity() {
        assertThrows(
            IllegalArgumentException.class,
            () -> PipelineInvocationSteps.oneToOne(new PipelineRunner(), "   ", -1, List.of()));
    }
}
