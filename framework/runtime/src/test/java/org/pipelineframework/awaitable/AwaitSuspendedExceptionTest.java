package org.pipelineframework.awaitable;

import org.junit.jupiter.api.Test;
import org.pipelineframework.step.NonRetryableException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AwaitSuspendedExceptionTest {

    @Test
    void constructsWithAllFields() {
        AwaitSuspendedException ex = new AwaitSuspendedException(
            "tenant1", "exec-123", "unit-456", 3);

        assertEquals("tenant1", ex.tenantId());
        assertEquals("exec-123", ex.executionId());
        assertEquals("unit-456", ex.unitId());
        assertEquals(3, ex.stepIndex());
    }

    @Test
    void messageContainsUnitId() {
        AwaitSuspendedException ex = new AwaitSuspendedException(
            "tenant1", "exec-123", "unit-abc", 0);

        assertTrue(ex.getMessage().contains("unit-abc"),
            "Expected message to contain unit id but was: " + ex.getMessage());
    }

    @Test
    void isRuntimeException() {
        AwaitSuspendedException ex = new AwaitSuspendedException(
            "tenant1", "exec-123", "unit-456", 0);

        assertTrue(ex instanceof RuntimeException);
    }

    @Test
    void isNonRetryableException() {
        AwaitSuspendedException ex = new AwaitSuspendedException(
            "tenant1", "exec-123", "unit-456", 0);

        assertTrue(ex instanceof NonRetryableException);
    }

    @Test
    void stepIndexZeroIsValid() {
        AwaitSuspendedException ex = new AwaitSuspendedException(
            "tenant1", "exec-123", "unit-456", 0);

        assertEquals(0, ex.stepIndex());
    }

    @Test
    void preservesAllFieldsIndependently() {
        AwaitSuspendedException ex1 = new AwaitSuspendedException("t1", "e1", "i1", 1);
        AwaitSuspendedException ex2 = new AwaitSuspendedException("t2", "e2", "i2", 2);

        assertEquals("t1", ex1.tenantId());
        assertEquals("t2", ex2.tenantId());
        assertEquals("i1", ex1.unitId());
        assertEquals("i2", ex2.unitId());
        assertEquals(1, ex1.stepIndex());
        assertEquals(2, ex2.stepIndex());
    }
}
