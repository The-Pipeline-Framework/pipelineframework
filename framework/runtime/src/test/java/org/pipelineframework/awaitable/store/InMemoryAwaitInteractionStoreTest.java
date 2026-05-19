package org.pipelineframework.awaitable.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitCreateCommand;
import org.pipelineframework.awaitable.AwaitInteractionStatus;

class InMemoryAwaitInteractionStoreTest {

    @Test
    void createOrGetDeduplicatesActiveInteractionByIdempotencyKey() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        AwaitCreateCommand command = createCommand("idem-1", 10_000L, 70_000L);

        var first = store.createOrGet(command).await().indefinitely();
        var duplicate = store.createOrGet(command).await().indefinitely();

        assertFalse(first.duplicate());
        assertTrue(duplicate.duplicate());
        assertEquals(first.record().interactionId(), duplicate.record().interactionId());
        assertEquals(AwaitInteractionStatus.WAITING, first.record().status());
    }

    @Test
    void markDispatchedUsesOptimisticVersion() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        var created = store.createOrGet(createCommand("idem-1", 10_000L, 70_000L)).await().indefinitely();

        var stale = store.markDispatched(
            "tenant",
            created.record().interactionId(),
            99L,
            Map.of("messageId", "m-1"),
            11_000L).await().indefinitely();
        var updated = store.markDispatched(
            "tenant",
            created.record().interactionId(),
            created.record().version(),
            Map.of("messageId", "m-1"),
            11_000L).await().indefinitely();

        assertTrue(stale.isEmpty());
        assertTrue(updated.isPresent());
        assertEquals(AwaitInteractionStatus.DISPATCHED, updated.get().status());
        assertEquals("m-1", updated.get().transportMetadata().get("messageId"));
    }

    @Test
    void completionCanResolveByCorrelationAndIsIdempotent() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        var created = store.createOrGet(createCommand("idem-1", 10_000L, 70_000L)).await().indefinitely();

        var completed = store.complete(new AwaitCompletionCommand(
            "tenant",
            null,
            created.record().correlationId(),
            "completion-1",
            Map.of("decision", "approved"),
            "alice",
            12_000L)).await().indefinitely();
        var duplicate = store.complete(new AwaitCompletionCommand(
            "tenant",
            created.record().interactionId(),
            null,
            "completion-1",
            Map.of("decision", "approved"),
            "alice",
            13_000L)).await().indefinitely();

        assertFalse(completed.duplicate());
        assertTrue(duplicate.duplicate());
        assertEquals(AwaitInteractionStatus.COMPLETED, completed.record().status());
        assertEquals("alice", completed.record().actor());
    }

    @Test
    void completionAfterDeadlineMarksTimedOutAndFails() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        var created = store.createOrGet(createCommand("idem-1", 10_000L, 20_000L)).await().indefinitely();

        assertThrows(IllegalStateException.class, () -> store.complete(new AwaitCompletionCommand(
            "tenant",
            created.record().interactionId(),
            null,
            "completion-1",
            Map.of("decision", "approved"),
            "alice",
            21_000L)).await().indefinitely());

        var stored = store.get("tenant", created.record().interactionId()).await().indefinitely().orElseThrow();
        assertEquals(AwaitInteractionStatus.TIMED_OUT, stored.status());
    }

    @Test
    void queryPendingFiltersByTenantAndAssignee() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        store.createOrGet(createCommand("idem-1", 10_000L, 70_000L)).await().indefinitely();
        store.createOrGet(new AwaitCreateCommand(
            "tenant",
            "execution-2",
            "review",
            "execution-2",
            "idem-2",
            "corr-2",
            Map.of("orderId", "o-2"),
            "bob",
            "finance",
            "interaction-api",
            10_000L,
            70_000L,
            Long.MAX_VALUE)).await().indefinitely();

        var alice = store.queryPending("tenant", "alice", null, null, 10).await().indefinitely();
        var finance = store.queryPending("tenant", null, "finance", null, 10).await().indefinitely();

        assertEquals(1, alice.size());
        assertEquals("alice", alice.getFirst().assignee());
        assertEquals(2, finance.size());
    }

    private AwaitCreateCommand createCommand(String idempotencyKey, long nowEpochMs, long deadlineEpochMs) {
        return new AwaitCreateCommand(
            "tenant",
            "execution-1",
            "review",
            "execution-1",
            idempotencyKey,
            "corr-1",
            Map.of("orderId", "o-1"),
            "alice",
            "finance",
            "interaction-api",
            nowEpochMs,
            deadlineEpochMs,
            Long.MAX_VALUE);
    }
}
