package org.pipelineframework.processor.routing;

import java.util.List;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PipelineBranchingPlanTest {

    @Test
    void rejectsBranchStepIndexThatDoesNotMatchListPosition() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            new PipelineBranchingPlan(
                true,
                1,
                List.of(
                    branchStep(0, "Classify", false),
                    branchStep(2, "Finalize", true))));

        org.junit.jupiter.api.Assertions.assertTrue(exception.getMessage().contains("BranchStep index mismatch"));
    }

    @Test
    void acceptsIndexedTerminalStepAtMatchingPosition() {
        assertDoesNotThrow(() -> new PipelineBranchingPlan(
            true,
            1,
            List.of(
                branchStep(0, "Classify", false),
                branchStep(1, "Finalize", true))));
    }

    private static PipelineBranchingPlan.BranchStep branchStep(int index, String name, boolean terminal) {
        return new PipelineBranchingPlan.BranchStep(
            index,
            name,
            name + "Input",
            name + "Output",
            List.of(name + "Accepted"),
            List.of(name + "Leaf"),
            List.of(ClassName.get("com.example.common.domain", name + "Accepted")),
            terminal);
    }
}
