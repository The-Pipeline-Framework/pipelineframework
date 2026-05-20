package org.pipelineframework.awaitable;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AwaitSuspendedExceptionTest {

    @Test
    void constructsWithAllFields() {
        AwaitSuspendedException ex = new AwaitSuspendedException(
            "tenant1", "exec-123", "interaction-456", 3);

        assertEquals("tenant1", ex.tenantId());
        assertEquals("exec-123", ex.executionId());
        assertEquals("interaction-456", ex.interactionId());
        assertEquals(3, ex.stepIndex());
    }

    @Test
    void messageContainsInteractionId() {
        AwaitSuspendedException ex = new AwaitSuspendedException(
            "tenant1", "exec-123", "interaction-abc", 0);

        assertTrue(ex.getMessage().contains("interaction-abc"),
            "Expected message to contain interaction id but was: " + ex.getMessage());
    }

    @Test
    void isRuntimeException() {
        AwaitSuspendedException ex = new AwaitSuspendedException(
            "tenant1", "exec-123", "interaction-456", 0);

        assertTrue(ex instanceof RuntimeException);
    }

    @Test
    void stepIndexZeroIsValid() {
        AwaitSuspendedException ex = new AwaitSuspendedException(
            "tenant1", "exec-123", "interaction-456", 0);

        assertEquals(0, ex.stepIndex());
    }

    @Test
    void preservesAllFieldsIndependently() {
        AwaitSuspendedException ex1 = new AwaitSuspendedException("t1", "e1", "i1", 1);
        AwaitSuspendedException ex2 = new AwaitSuspendedException("t2", "e2", "i2", 2);

        assertEquals("t1", ex1.tenantId());
        assertEquals("t2", ex2.tenantId());
        assertEquals("i1", ex1.interactionId());
        assertEquals("i2", ex2.interactionId());
        assertEquals(1, ex1.stepIndex());
        assertEquals(2, ex2.stepIndex());
    }
}