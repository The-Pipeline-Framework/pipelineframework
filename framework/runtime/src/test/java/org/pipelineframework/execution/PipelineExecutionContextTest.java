package org.pipelineframework.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class PipelineExecutionContextTest {

    @Test
    void constructsContextWithValidFields() {
        PipelineExecutionContext ctx = new PipelineExecutionContext("tenant-1", "exec-abc", 3);

        assertEquals("tenant-1", ctx.tenantId());
        assertEquals("exec-abc", ctx.executionId());
        assertEquals(3, ctx.currentStepIndex());
    }

    @Test
    void acceptsZeroStepIndex() {
        PipelineExecutionContext ctx = new PipelineExecutionContext("tenant-1", "exec-abc", 0);

        assertEquals(0, ctx.currentStepIndex());
    }

    @Test
    void rejectsNullTenantId() {
        assertThrows(IllegalArgumentException.class, () ->
            new PipelineExecutionContext(null, "exec-abc", 0));
    }

    @Test
    void rejectsBlankTenantId() {
        assertThrows(IllegalArgumentException.class, () ->
            new PipelineExecutionContext("  ", "exec-abc", 0));
    }

    @Test
    void rejectsEmptyTenantId() {
        assertThrows(IllegalArgumentException.class, () ->
            new PipelineExecutionContext("", "exec-abc", 0));
    }

    @Test
    void rejectsNullExecutionId() {
        assertThrows(IllegalArgumentException.class, () ->
            new PipelineExecutionContext("tenant-1", null, 0));
    }

    @Test
    void rejectsBlankExecutionId() {
        assertThrows(IllegalArgumentException.class, () ->
            new PipelineExecutionContext("tenant-1", "  ", 0));
    }

    @Test
    void rejectsEmptyExecutionId() {
        assertThrows(IllegalArgumentException.class, () ->
            new PipelineExecutionContext("tenant-1", "", 0));
    }

    @Test
    void rejectsNegativeStepIndex() {
        assertThrows(IllegalArgumentException.class, () ->
            new PipelineExecutionContext("tenant-1", "exec-abc", -1));
    }

    @Test
    void contextIsARecord() {
        PipelineExecutionContext ctx1 = new PipelineExecutionContext("tenant-1", "exec-abc", 2);
        PipelineExecutionContext ctx2 = new PipelineExecutionContext("tenant-1", "exec-abc", 2);

        assertEquals(ctx1, ctx2);
        assertEquals(ctx1.hashCode(), ctx2.hashCode());
    }
}