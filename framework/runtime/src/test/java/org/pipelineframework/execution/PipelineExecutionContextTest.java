package org.pipelineframework.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.runtime.core.RuntimeAdapters;

class PipelineExecutionContextTest {

    @BeforeEach
    void resetContext() {
        RuntimeAdapters.resetForTests();
        PipelineExecutionContextHolder.clear();
    }

    @AfterEach
    void cleanupContext() {
        RuntimeAdapters.resetForTests();
        PipelineExecutionContextHolder.clear();
    }

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
    void contextsWithTheSameExecutionIdentityAreEqual() {
        PipelineExecutionContext ctx1 = new PipelineExecutionContext("tenant-1", "exec-abc", 2);
        PipelineExecutionContext ctx2 = new PipelineExecutionContext("tenant-1", "exec-abc", 2);

        assertEquals(ctx1, ctx2);
        assertEquals(ctx1.hashCode(), ctx2.hashCode());
    }

    @Test
    void commandRetryAdmissionCanBeClaimedOnlyOnceAcrossStepViews() {
        PipelineExecutionContext root = PipelineExecutionContext.forCommandRetry(
            "tenant-1", "exec-abc", 0, 2, "command-retry:exec-abc:7");
        PipelineExecutionContext target = root.atStep(2);

        assertTrue(target.commandRetryTargetsCurrentStep());
        assertTrue(target.claimCommandRetry("archive:invoice-1"));
        org.junit.jupiter.api.Assertions.assertFalse(
            root.atStep(2).claimCommandRetry("notify:invoice-1"));
        assertTrue(root.commandRetryClaimed());
        assertEquals(
            target.commandRetryAttemptId("archive:invoice-1"),
            root.atStep(2).commandRetryAttemptId("archive:invoice-1"));
    }

    @Test
    void unclaimedCommandRetryFailsCompletionValidation() {
        PipelineExecutionContext context = PipelineExecutionContext.forCommandRetry(
            "tenant-1", "exec-abc", 2, 2, "command-retry:exec-abc:7");

        assertThrows(IllegalStateException.class, context::requireCommandRetryClaimed);
    }

    @Test
    void holderReturnsEmptyWhenContextIsAbsent() {
        assertTrue(PipelineExecutionContextHolder.get().isEmpty());
    }

    @Test
    void holderReturnsPresentContextWhenSet() {
        PipelineExecutionContext context = new PipelineExecutionContext("tenant-1", "exec-abc", 2);

        PipelineExecutionContextHolder.set(context);

        assertEquals(context, PipelineExecutionContextHolder.get().orElseThrow());
    }

    @Test
    void holderClearRemovesContext() {
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant-1", "exec-abc", 2));

        PipelineExecutionContextHolder.clear();

        assertTrue(PipelineExecutionContextHolder.get().isEmpty());
    }
}
