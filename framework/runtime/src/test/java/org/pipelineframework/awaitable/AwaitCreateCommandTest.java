package org.pipelineframework.awaitable;

import java.time.Instant;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AwaitCreateCommandTest {

    private static final long NOW = 10_000L;
    private static final long DEADLINE = 70_000L;

    @Test
    void constructsFullCommandWithAllFields() {
        AwaitCreateCommand command = new AwaitCreateCommand(
            "tenant1", "exec-1", "step-review", 2, "com.example.Output",
            "cause-1", "idem-1", "corr-1", "payload", "alice", "finance",
            "webhook", NOW, DEADLINE, Long.MAX_VALUE);

        assertEquals("tenant1", command.tenantId());
        assertEquals("exec-1", command.executionId());
        assertEquals("step-review", command.stepId());
        assertEquals(2, command.stepIndex());
        assertEquals("com.example.Output", command.outputType());
        assertEquals("cause-1", command.causationId());
        assertEquals("idem-1", command.idempotencyKey());
        assertEquals("corr-1", command.correlationId());
        assertEquals("payload", command.requestPayload());
        assertEquals("alice", command.assignee());
        assertEquals("finance", command.group());
        assertEquals("webhook", command.transportType());
        assertEquals(NOW, command.nowEpochMs());
        assertEquals(DEADLINE, command.deadlineEpochMs());
        assertEquals(Long.MAX_VALUE, command.ttlEpochS());
    }

    @Test
    void constructsBarrierItemCommand() {
        AwaitCreateCommand command = new AwaitCreateCommand(
            "tenant1", "exec-1", "await-payment-provider", 3, "com.example.PaymentStatus",
            "cause-1", "idem-1", "corr-1", "payload", null, null,
            "kafka", "barrier-1", 1, 2, NOW, DEADLINE, Long.MAX_VALUE);

        assertEquals("barrier-1", command.barrierId());
        assertEquals(1, command.barrierItemIndex());
        assertEquals(2, command.barrierItemCount());
    }

    @Test
    void backwardCompatConstructorDefaultsStepIndexAndOutputType() {
        AwaitCreateCommand command = new AwaitCreateCommand(
            "tenant1", "exec-1", "step-review",
            "cause-1", "idem-1", "corr-1", "payload", "alice", "finance",
            "webhook", NOW, DEADLINE, Long.MAX_VALUE);

        assertEquals(0, command.stepIndex());
        assertEquals(Object.class.getName(), command.outputType());
        assertEquals("step-review", command.stepId());
    }

    @Test
    void rejectsBlankTenantId() {
        assertThrows(IllegalArgumentException.class, () ->
            new AwaitCreateCommand("", "exec-1", "step-1", 0, "Out.class",
                "cause", "idem", "corr", null, null, null, "webhook", NOW, DEADLINE, Long.MAX_VALUE));
    }

    @Test
    void rejectsNullTenantId() {
        assertThrows(IllegalArgumentException.class, () ->
            new AwaitCreateCommand(null, "exec-1", "step-1", 0, "Out.class",
                "cause", "idem", "corr", null, null, null, "webhook", NOW, DEADLINE, Long.MAX_VALUE));
    }

    @Test
    void rejectsBlankExecutionId() {
        assertThrows(IllegalArgumentException.class, () ->
            new AwaitCreateCommand("tenant1", "", "step-1", 0, "Out.class",
                "cause", "idem", "corr", null, null, null, "webhook", NOW, DEADLINE, Long.MAX_VALUE));
    }

    @Test
    void rejectsBlankStepId() {
        assertThrows(IllegalArgumentException.class, () ->
            new AwaitCreateCommand("tenant1", "exec-1", " ", 0, "Out.class",
                "cause", "idem", "corr", null, null, null, "webhook", NOW, DEADLINE, Long.MAX_VALUE));
    }

    @Test
    void rejectsBlankOutputType() {
        assertThrows(IllegalArgumentException.class, () ->
            new AwaitCreateCommand("tenant1", "exec-1", "step-1", 0, "",
                "cause", "idem", "corr", null, null, null, "webhook", NOW, DEADLINE, Long.MAX_VALUE));
    }

    @Test
    void rejectsBlankIdempotencyKey() {
        assertThrows(IllegalArgumentException.class, () ->
            new AwaitCreateCommand("tenant1", "exec-1", "step-1", 0, "Out.class",
                "cause", "", "corr", null, null, null, "webhook", NOW, DEADLINE, Long.MAX_VALUE));
    }

    @Test
    void rejectsBlankCorrelationId() {
        assertThrows(IllegalArgumentException.class, () ->
            new AwaitCreateCommand("tenant1", "exec-1", "step-1", 0, "Out.class",
                "cause", "idem", "  ", null, null, null, "webhook", NOW, DEADLINE, Long.MAX_VALUE));
    }

    @Test
    void rejectsBlankTransportType() {
        assertThrows(IllegalArgumentException.class, () ->
            new AwaitCreateCommand("tenant1", "exec-1", "step-1", 0, "Out.class",
                "cause", "idem", "corr", null, null, null, "", NOW, DEADLINE, Long.MAX_VALUE));
    }

    @Test
    void rejectsDeadlineNotAfterNow() {
        assertThrows(IllegalArgumentException.class, () ->
            new AwaitCreateCommand("tenant1", "exec-1", "step-1", 0, "Out.class",
                "cause", "idem", "corr", null, null, null, "webhook", NOW, NOW, Long.MAX_VALUE));

        assertThrows(IllegalArgumentException.class, () ->
            new AwaitCreateCommand("tenant1", "exec-1", "step-1", 0, "Out.class",
                "cause", "idem", "corr", null, null, null, "webhook", NOW, NOW - 1, Long.MAX_VALUE));
    }

    @Test
    void rejectsInvalidBarrierMetadata() {
        assertThrows(IllegalArgumentException.class, () ->
            new AwaitCreateCommand("tenant1", "exec-1", "step-1", 0, "Out.class",
                "cause", "idem", "corr", null, null, null, "webhook",
                null, 0, 2, NOW, DEADLINE, Long.MAX_VALUE));

        assertThrows(IllegalArgumentException.class, () ->
            new AwaitCreateCommand("tenant1", "exec-1", "step-1", 0, "Out.class",
                "cause", "idem", "corr", null, null, null, "webhook",
                "barrier-1", null, 2, NOW, DEADLINE, Long.MAX_VALUE));

        assertThrows(IllegalArgumentException.class, () ->
            new AwaitCreateCommand("tenant1", "exec-1", "step-1", 0, "Out.class",
                "cause", "idem", "corr", null, null, null, "webhook",
                "barrier-1", 2, 2, NOW, DEADLINE, Long.MAX_VALUE));
    }

    @Test
    void rejectsTtlBeforeDeadline() {
        long deadlineEpochMs = DEADLINE;
        long deadlineEpochS = Instant.ofEpochMilli(deadlineEpochMs).getEpochSecond();
        long ttlBeforeDeadline = deadlineEpochS - 1;

        assertThrows(IllegalArgumentException.class, () ->
            new AwaitCreateCommand("tenant1", "exec-1", "step-1", 0, "Out.class",
                "cause", "idem", "corr", null, null, null, "webhook", NOW, deadlineEpochMs, ttlBeforeDeadline));
    }

    @Test
    void autoSetsTtlWhenZero() {
        AwaitCreateCommand command = new AwaitCreateCommand(
            "tenant1", "exec-1", "step-1", 0, "Out.class",
            "cause", "idem", "corr", null, null, null, "webhook", NOW, DEADLINE, 0L);

        long expectedTtl = Instant.ofEpochMilli(DEADLINE).plusSeconds(86_400).getEpochSecond();
        assertEquals(expectedTtl, command.ttlEpochS());
    }

    @Test
    void autoSetsNowEpochMsWhenZeroOrNegative() {
        long before = System.currentTimeMillis();
        long futureDeadline = before + 60_000L;
        AwaitCreateCommand command = new AwaitCreateCommand(
            "tenant1", "exec-1", "step-1", 0, "Out.class",
            "cause", "idem", "corr", null, null, null, "webhook", 0L, futureDeadline, Long.MAX_VALUE);
        long after = System.currentTimeMillis();

        assertTrue(command.nowEpochMs() >= before);
        assertTrue(command.nowEpochMs() <= after);

        before = System.currentTimeMillis();
        AwaitCreateCommand negativeNowCommand = new AwaitCreateCommand(
            "tenant1", "exec-1", "step-1", 0, "Out.class",
            "cause", "idem", "corr", null, null, null, "webhook", -1L, futureDeadline, Long.MAX_VALUE);
        after = System.currentTimeMillis();

        assertTrue(negativeNowCommand.nowEpochMs() >= before);
        assertTrue(negativeNowCommand.nowEpochMs() <= after);
    }
}
