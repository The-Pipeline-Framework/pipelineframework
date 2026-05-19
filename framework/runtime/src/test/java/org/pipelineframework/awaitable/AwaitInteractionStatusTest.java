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
    void allNonTerminalStatusesHaveExactlyTwoValues() {
        long nonTerminalCount = java.util.Arrays.stream(AwaitInteractionStatus.values())
            .filter(s -> !s.terminal())
            .count();
        // WAITING and DISPATCHED are the only non-terminal states
        assertTrue(nonTerminalCount == 2,
            "Expected 2 non-terminal states (WAITING, DISPATCHED), found " + nonTerminalCount);
    }

    @Test
    void allEnumValuesAreDeclared() {
        AwaitInteractionStatus[] values = AwaitInteractionStatus.values();
        assertTrue(values.length == 7,
            "Expected 7 enum values but found " + values.length);
    }
}