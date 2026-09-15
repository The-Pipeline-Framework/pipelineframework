package org.pipelineframework.awaitable;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AwaitInteractionStatusTest {

    @Test
    void waitingIsNotTerminal() {
        assertFalse(AwaitInteractionStatus.WAITING.terminal());
    }

    @Test
    void dispatchedIsNotTerminal() {
        assertFalse(AwaitInteractionStatus.DISPATCHED.terminal());
    }

    @Test
    void dispatchingIsNotTerminal() {
        assertFalse(AwaitInteractionStatus.DISPATCHING.terminal());
    }

    @Test
    void completedIsTerminal() {
        assertTrue(AwaitInteractionStatus.COMPLETED.terminal());
    }

    @Test
    void failedIsTerminal() {
        assertTrue(AwaitInteractionStatus.FAILED.terminal());
    }

    @Test
    void timedOutIsTerminal() {
        assertTrue(AwaitInteractionStatus.TIMED_OUT.terminal());
    }

    @Test
    void cancelledIsTerminal() {
        assertTrue(AwaitInteractionStatus.CANCELLED.terminal());
    }

    @Test
    void expiredIsTerminal() {
        assertTrue(AwaitInteractionStatus.EXPIRED.terminal());
    }

    @Test
    void observationRemainsNonTerminalAlongsidePendingDispatchStates() {
        long nonTerminalCount = java.util.Arrays.stream(AwaitInteractionStatus.values())
            .filter(s -> !s.terminal())
            .count();
        assertFalse(AwaitInteractionStatus.COMPLETION_OBSERVED.terminal());
        assertTrue(nonTerminalCount == 4,
            "Expected 4 non-terminal states, found " + nonTerminalCount);
    }

    @Test
    void allEnumValuesAreDeclared() {
        AwaitInteractionStatus[] values = AwaitInteractionStatus.values();
        assertTrue(values.length == 9,
            "Expected 9 enum values but found " + values.length);
    }
}
