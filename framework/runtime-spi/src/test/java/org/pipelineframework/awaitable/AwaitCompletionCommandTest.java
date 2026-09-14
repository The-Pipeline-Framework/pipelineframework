package org.pipelineframework.awaitable;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AwaitCompletionCommandTest {

    @Test
    void constructsWithInteractionId() {
        AwaitCompletionCommand command = new AwaitCompletionCommand(
            "tenant1", "interaction-123", null, "idem-1", "response", "alice", 1000L);

        assertEquals("tenant1", command.tenantId());
        assertEquals("interaction-123", command.interactionId());
        assertEquals("idem-1", command.idempotencyKey());
        assertEquals("alice", command.actor());
        assertEquals("response", command.responsePayload());
        assertEquals(1000L, command.nowEpochMs());
    }

    @Test
    void constructsWithCorrelationIdOnly() {
        AwaitCompletionCommand command = new AwaitCompletionCommand(
            "tenant1", null, "corr-456", "idem-1", "response", null, 2000L);

        assertEquals("corr-456", command.correlationId());
        assertNotNull(command.nowEpochMs());
    }

    @Test
    void rejectsBlankTenantId() {
        assertThrows(IllegalArgumentException.class, () -> new AwaitCompletionCommand(
            "", "interaction-123", null, "idem-1", null, null, 1000L));
    }

    @Test
    void rejectsNullTenantId() {
        assertThrows(IllegalArgumentException.class, () -> new AwaitCompletionCommand(
            null, "interaction-123", null, "idem-1", null, null, 1000L));
    }

    @Test
    void rejectsBothInteractionIdAndCorrelationIdMissing() {
        assertThrows(IllegalArgumentException.class, () -> new AwaitCompletionCommand(
            "tenant1", null, null, "idem-1", null, null, 1000L));
    }

    @Test
    void rejectsBothBlankInteractionIdAndBlankCorrelationId() {
        assertThrows(IllegalArgumentException.class, () -> new AwaitCompletionCommand(
            "tenant1", "  ", "  ", "idem-1", null, null, 1000L));
    }

    @Test
    void autoSetsNowEpochMsWhenZero() {
        long before = System.currentTimeMillis();
        AwaitCompletionCommand command = new AwaitCompletionCommand(
            "tenant1", "interaction-123", null, "idem-1", null, null, 0L);
        long after = System.currentTimeMillis();

        assertTrue(command.nowEpochMs() >= before);
        assertTrue(command.nowEpochMs() <= after);
    }

    @Test
    void autoSetsNowEpochMsWhenNegative() {
        long before = System.currentTimeMillis();
        AwaitCompletionCommand command = new AwaitCompletionCommand(
            "tenant1", "interaction-123", null, "idem-1", null, null, -500L);
        long after = System.currentTimeMillis();

        assertTrue(command.nowEpochMs() >= before);
        assertTrue(command.nowEpochMs() <= after);
    }

    @Test
    void acceptsOnlyInteractionIdWithNullCorrelation() {
        AwaitCompletionCommand command = new AwaitCompletionCommand(
            "tenant1", "interaction-abc", null, "idem-x", "payload", "bob", 5000L);

        assertEquals("interaction-abc", command.interactionId());
        assertEquals("payload", command.responsePayload());
        assertEquals("bob", command.actor());
    }

    @Test
    void acceptsNullResponsePayload() {
        AwaitCompletionCommand command = new AwaitCompletionCommand(
            "tenant1", "interaction-abc", null, "idem-x", null, "alice", 5000L);

        assertEquals("alice", command.actor());
        assertEquals(null, command.responsePayload());
    }
}
