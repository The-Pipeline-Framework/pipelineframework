package org.pipelineframework.awaitable;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AwaitExecutionContextTest {

    @Test
    void constructsWithValidFields() {
        AwaitExecutionContext context = new AwaitExecutionContext("tenant1", "exec-123", 2);

        assertEquals("tenant1", context.tenantId());
        assertEquals("exec-123", context.executionId());
        assertEquals(2, context.currentStepIndex());
    }

    @Test
    void acceptsZeroStepIndex() {
        AwaitExecutionContext context = new AwaitExecutionContext("tenant1", "exec-123", 0);

        assertEquals(0, context.currentStepIndex());
    }

    @Test
    void rejectsBlankTenantId() {
        assertThrows(IllegalArgumentException.class, () ->
            new AwaitExecutionContext("", "exec-123", 0));
    }

    @Test
    void rejectsWhitespaceTenantId() {
        assertThrows(IllegalArgumentException.class, () ->
            new AwaitExecutionContext("   ", "exec-123", 0));
    }

    @Test
    void rejectsNullTenantId() {
        assertThrows(IllegalArgumentException.class, () ->
            new AwaitExecutionContext(null, "exec-123", 0));
    }

    @Test
    void rejectsBlankExecutionId() {
        assertThrows(IllegalArgumentException.class, () ->
            new AwaitExecutionContext("tenant1", "", 0));
    }

    @Test
    void rejectsNullExecutionId() {
        assertThrows(IllegalArgumentException.class, () ->
            new AwaitExecutionContext("tenant1", null, 0));
    }

    @Test
    void rejectsNegativeStepIndex() {
        assertThrows(IllegalArgumentException.class, () ->
            new AwaitExecutionContext("tenant1", "exec-123", -1));
    }

    @Test
    void updatesStepIndexMutation() {
        AwaitExecutionContext context = new AwaitExecutionContext("tenant1", "exec-123", 0);
        context.currentStepIndex(5);

        assertEquals(5, context.currentStepIndex());
    }

    @Test
    void rejectsNegativeStepIndexOnMutation() {
        AwaitExecutionContext context = new AwaitExecutionContext("tenant1", "exec-123", 0);

        assertThrows(IllegalArgumentException.class, () -> context.currentStepIndex(-1));
    }

    @Test
    void allowsIncrementingStepIndexRepeatedly() {
        AwaitExecutionContext context = new AwaitExecutionContext("tenant1", "exec-123", 0);
        context.currentStepIndex(1);
        context.currentStepIndex(2);
        context.currentStepIndex(10);

        assertEquals(10, context.currentStepIndex());
    }
}