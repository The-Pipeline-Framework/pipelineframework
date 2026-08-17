package org.pipelineframework.invocation;

import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipelineInvocationContextTest {

    @AfterEach
    void clearContext() {
        PipelineInvocationContextHolder.clear();
    }

    @Test
    void assignsDeterministicRecursiveInvocationFrames() {
        PipelineInvocationContext root = PipelineInvocationContext.root(2);

        PipelineInvocationContext first = root.enterRecursive("agent", "continue");
        PipelineInvocationContext second = first.enterRecursive("agent", "continue");

        assertEquals(0, root.recursiveDepth());
        assertEquals(2, second.recursiveDepth());
        assertEquals(List.of("agent:continue#1", "agent:continue#2"),
            second.recursivePath().stream().map(PipelineInvocationContext.Frame::display).toList());
    }

    @Test
    void exactDepthSucceedsAndNextInvocationFailsExplicitly() {
        PipelineInvocationContext exact = PipelineInvocationContext.root(1)
            .enterRecursive("agent", "continue");

        PipelineRecursionLimitExceededException failure = assertThrows(
            PipelineRecursionLimitExceededException.class,
            () -> exact.enterRecursive("agent", "continue"));

        assertEquals("agent", failure.definitionId());
        assertEquals("continue", failure.callsiteId());
        assertEquals(2, failure.attemptedDepth());
        assertEquals(1, failure.maximumDepth());
        assertEquals(1, failure.parentPath().size());
    }

    @Test
    void holderScopesAndRestoresImmutableInvocationContext() {
        PipelineInvocationContext parent = PipelineInvocationContext.root(3);
        PipelineInvocationContext child = parent.enterRecursive("agent", "continue");
        PipelineInvocationContextHolder.set(parent);

        int depth = PipelineInvocationContextHolder.call(
            child, () -> PipelineInvocationContextHolder.get().orElseThrow().recursiveDepth());

        assertEquals(1, depth);
        assertTrue(PipelineInvocationContextHolder.get().isPresent());
        assertEquals(parent, PipelineInvocationContextHolder.get().orElseThrow());
    }
}
