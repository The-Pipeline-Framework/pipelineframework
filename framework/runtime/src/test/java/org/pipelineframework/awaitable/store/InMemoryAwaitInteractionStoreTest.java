package org.pipelineframework.awaitable.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitCreateCommand;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitInteractionNotFoundException;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.awaitable.AwaitContinuationStatus;
import org.pipelineframework.awaitable.AwaitInteractionTerminalException;
import org.pipelineframework.orchestrator.InMemoryControlPlaneTransactionLock;
import org.pipelineframework.orchestrator.InMemoryExecutionStateStore;
import org.pipelineframework.orchestrator.stream.StreamRegionPageCommit;
import org.pipelineframework.orchestrator.stream.StreamRegionRecord;
import org.pipelineframework.orchestrator.stream.StreamRegionStatus;
import org.pipelineframework.stream.OpaqueSourceCheckpoint;
import org.pipelineframework.stream.ResumableSourceDescriptor;

class InMemoryAwaitInteractionStoreTest {

    @Test
    void linkedContinuationAppliesAndReturnsItsCreditUnderOneLocalControlPlaneLock() {
        InMemoryControlPlaneTransactionLock transactionLock = new InMemoryControlPlaneTransactionLock();
        InMemoryExecutionStateStore executionStore = new InMemoryExecutionStateStore(transactionLock);
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore(transactionLock, executionStore);
        StreamRegionRecord region = streamRegion();
        executionStore.createStreamRegion(region).await().indefinitely();
        StreamRegionRecord claimedRegion = executionStore.claimStreamRegion(
            region.tenantId(), region.executionId(), region.regionId(), "region-worker", 1_000L, 10_000L)
            .await().indefinitely().orElseThrow();
        AwaitInteractionRecord interaction = streamInteraction(claimedRegion, 0);

        StreamRegionRecord materialized = store.materializeStreamRegionPage(new StreamRegionPageCommit(
            claimedRegion,
            new OpaqueSourceCheckpoint(java.util.Optional.of("checkpoint-1")),
            true,
            List.of(interaction),
            1_100L)).await().indefinitely().orElseThrow();
        AwaitInteractionRecord completed = store.complete(new AwaitCompletionCommand(
            interaction.tenantId(), interaction.interactionId(), null, "completion-1", "approved", "operator", 1_200L))
            .await().indefinitely().record();
        AwaitInteractionRecord ready = store.activateContinuationIfEligible(
            completed.tenantId(), completed.interactionId(), completed.version(), 1_200L)
            .await().indefinitely().orElseThrow();
        AwaitInteractionRecord claimed = store.claimDueContinuation(
            ready.tenantId(), ready.interactionId(), "continuation-worker", 1_200L, 1_000L)
            .await().indefinitely().orElseThrow();

        AwaitInteractionRecord applied = store.completeContinuationAndReleaseStreamCredit(
            claimed.tenantId(), claimed.interactionId(), claimed.version(), "continuation-worker", "suffix", 1_300L)
            .await().indefinitely().orElseThrow();
        var replay = store.completeContinuationAndReleaseStreamCredit(
            claimed.tenantId(), claimed.interactionId(), claimed.version(), "continuation-worker", "suffix", 1_301L)
            .await().indefinitely();
        StreamRegionRecord released = executionStore.getStreamRegion(
            region.tenantId(), region.executionId(), region.regionId()).await().indefinitely().orElseThrow();

        assertEquals(StreamRegionStatus.SOURCE_SEALED, materialized.status());
        assertEquals(AwaitContinuationStatus.APPLIED, applied.continuationStatus());
        assertEquals("suffix", applied.continuationOutputPayload());
        assertTrue(replay.isEmpty());
        assertEquals(0, released.outstandingCredits());
        assertEquals(StreamRegionStatus.COMPLETED, released.status());
    }

    @Test
    void findsOnlyWaitingInteractionsLinkedToAStreamRegionForDispatchRecovery() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        StreamRegionRecord region = streamRegion();
        store.importRecord(streamInteraction(region, 0)).await().indefinitely();
        AwaitInteractionRecord ordinary = new AwaitInteractionRecord(
            "tenant", "execution-1", "await-payment-provider", 3, String.class.getName(),
            "ordinary", "ordinary", "cause", "ordinary", 0L, AwaitInteractionStatus.WAITING,
            Map.of(), null, "unit-1", 1, null, null, null, "kafka", Map.of(), 70_000L,
            1_000L, 1_000L, Long.MAX_VALUE, String.class.getName(), AwaitContinuationStatus.HELD,
            0, 0L, "", 0L, null, "");
        store.importRecord(ordinary).await().indefinitely();

        List<AwaitInteractionRecord> recovered = store.findDueStreamInteractionDispatches(2_000L, 10)
            .await().indefinitely();

        assertEquals(List.of(StreamRegionPageCommit.interactionId(region, 0)), recovered.stream()
            .map(AwaitInteractionRecord::interactionId).toList());
    }

    @Test
    void itemContinuationIsClaimedAppliedAndNeverReclaimed() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        var created = store.createOrGet(itemCommand("continuation", "continuation-correlation", 0))
            .await().indefinitely();
        var completed = store.complete(new AwaitCompletionCommand(
            "tenant", created.record().interactionId(), null, "completion", "approved", "operator", 12_000L))
            .await().indefinitely().record();

        var ready = store.activateContinuationIfEligible(
            "tenant", completed.interactionId(), completed.version(), 12_000L).await().indefinitely().orElseThrow();
        var claimed = store.claimDueContinuation(
            "tenant", ready.interactionId(), "worker-a", 12_000L, 1_000L).await().indefinitely().orElseThrow();
        var duplicateClaim = store.claimDueContinuation(
            "tenant", ready.interactionId(), "worker-b", 12_001L, 1_000L).await().indefinitely();
        var applied = store.completeContinuation(
            "tenant", claimed.interactionId(), claimed.version(), List.of("suffix"), 12_100L)
            .await().indefinitely().orElseThrow();

        assertEquals(AwaitContinuationStatus.READY, ready.continuationStatus());
        assertEquals(AwaitContinuationStatus.CLAIMED, claimed.continuationStatus());
        assertTrue(duplicateClaim.isEmpty());
        assertEquals(AwaitContinuationStatus.APPLIED, applied.continuationStatus());
        assertEquals(List.of("suffix"), applied.continuationOutputPayload());
        assertTrue(store.findDueContinuations(20_000L, 10).await().indefinitely().isEmpty());
    }

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

        var stale = store.markDispatching(
            "tenant",
            created.record().interactionId(),
            99L,
            11_000L).await().indefinitely();
        var claimed = store.markDispatching(
            "tenant",
            created.record().interactionId(),
            created.record().version(),
            11_000L).await().indefinitely();
        var updated = store.markDispatched(
            "tenant",
            created.record().interactionId(),
            claimed.orElseThrow().version(),
            Map.of("messageId", "m-1"),
            12_000L).await().indefinitely();

        assertTrue(stale.isEmpty());
        assertTrue(claimed.isPresent());
        assertEquals(AwaitInteractionStatus.DISPATCHING, claimed.get().status());
        assertTrue(updated.isPresent());
        assertEquals(AwaitInteractionStatus.DISPATCHED, updated.get().status());
        assertEquals("m-1", updated.get().transportMetadata().get("messageId"));
    }

    @Test
    void dispatchedInteractionCannotBeClaimedAgain() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        var created = store.createOrGet(createCommand("idem-1", 10_000L, 70_000L)).await().indefinitely();
        var claimed = store.markDispatching(
            "tenant",
            created.record().interactionId(),
            created.record().version(),
            11_000L).await().indefinitely().orElseThrow();
        var dispatched = store.markDispatched(
            "tenant",
            created.record().interactionId(),
            claimed.version(),
            Map.of("messageId", "m-1"),
            12_000L).await().indefinitely().orElseThrow();

        var duplicateClaim = store.markDispatching(
            "tenant",
            created.record().interactionId(),
            dispatched.version(),
            13_000L).await().indefinitely();

        assertTrue(duplicateClaim.isEmpty());
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

        assertThrows(AwaitInteractionTerminalException.class, () -> store.complete(new AwaitCompletionCommand(
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
            "tenant", "execution-2", "review", 0, String.class.getName(),
            "cause-2", "idem-2", "corr-2", Map.of("orderId", "o-2"),
            "bob", "finance", "interaction-api", "unit-2", null,
            10_000L, 70_000L, Long.MAX_VALUE)).await().indefinitely();

        var alice = store.queryPending("tenant", "alice", null, null, 10).await().indefinitely();
        var finance = store.queryPending("tenant", null, "finance", null, 10).await().indefinitely();

        assertEquals(1, alice.size());
        assertEquals("alice", alice.getFirst().assignee());
        assertEquals(2, finance.size());
    }

    @Test
    void queryPendingTreatsBlankFiltersAsUnset() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        store.createOrGet(createCommand("idem-1", 10_000L, 70_000L)).await().indefinitely();
        store.createOrGet(new AwaitCreateCommand(
            "tenant", "execution-2", "review", 0, String.class.getName(),
            "cause-2", "idem-2", "corr-2", Map.of("orderId", "o-2"),
            "bob", "finance", "interaction-api", "unit-2", null,
            10_000L, 70_000L, Long.MAX_VALUE)).await().indefinitely();

        var noAssignee = store.queryPending("tenant", "", null, "", 10).await().indefinitely();
        var whitespaceAssignee = store.queryPending("tenant", "   ", "   ", " ", 10).await().indefinitely();

        assertEquals(2, noAssignee.size());
        assertEquals(2, whitespaceAssignee.size());
    }

    @Test
    void getReturnsEmptyForUnknownInteractionId() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();

        var result = store.get("tenant", "unknown-id").await().indefinitely();

        assertTrue(result.isEmpty());
    }

    @Test
    void getReturnsRecordAfterCreation() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        var created = store.createOrGet(createCommand("idem-1", 10_000L, 70_000L)).await().indefinitely();

        var found = store.get("tenant", created.record().interactionId()).await().indefinitely();

        assertTrue(found.isPresent());
        assertEquals(created.record().interactionId(), found.get().interactionId());
    }

    @Test
    void findByCorrelationReturnsEmptyForUnknownId() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();

        var result = store.findByCorrelation("tenant", "unknown-corr").await().indefinitely();

        assertTrue(result.isEmpty());
    }

    @Test
    void findByCorrelationReturnsRecordAfterCreation() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        var created = store.createOrGet(createCommand("idem-1", 10_000L, 70_000L)).await().indefinitely();
        String correlationId = created.record().correlationId();

        var found = store.findByCorrelation("tenant", correlationId).await().indefinitely();

        assertTrue(found.isPresent());
        assertEquals(created.record().interactionId(), found.get().interactionId());
    }

    @Test
    void findByUnitReturnsItemsInIndexOrder() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        store.createOrGet(itemCommand("idem-2", "corr-2", 2)).await().indefinitely();
        store.createOrGet(itemCommand("idem-0", "corr-0", 0)).await().indefinitely();
        store.createOrGet(itemCommand("idem-1", "corr-1", 1)).await().indefinitely();

        List<AwaitInteractionRecord> records = store.findByUnit(
                "tenant",
                "unit-1")
            .await().indefinitely();

        assertEquals(3, records.size());
        assertEquals(List.of(0, 1, 2), records.stream().map(AwaitInteractionRecord::itemIndex).toList());
        assertTrue(records.stream().allMatch(AwaitInteractionRecord::itemInteraction));
    }

    @Test
    void cancelTransitionsToTerminalState() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        var created = store.createOrGet(createCommand("idem-1", 10_000L, 70_000L)).await().indefinitely();
        String interactionId = created.record().interactionId();

        var cancelled = store.cancel("tenant", interactionId, 0L, "user request", 15_000L)
            .await().indefinitely();

        assertTrue(cancelled.isPresent());
        assertEquals(AwaitInteractionStatus.CANCELLED, cancelled.get().status());
        assertTrue(cancelled.get().status().terminal());
    }

    @Test
    void cancelWithWrongVersionReturnsEmpty() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        var created = store.createOrGet(createCommand("idem-1", 10_000L, 70_000L)).await().indefinitely();

        var result = store.cancel("tenant", created.record().interactionId(), 999L, "reason", 15_000L)
            .await().indefinitely();

        assertTrue(result.isEmpty());
    }

    @Test
    void failTransitionsToFailedStatus() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        var created = store.createOrGet(createCommand("idem-1", 10_000L, 70_000L)).await().indefinitely();
        String interactionId = created.record().interactionId();

        var failed = store.fail("tenant", interactionId, 0L, "transport error", 15_000L)
            .await().indefinitely();

        assertTrue(failed.isPresent());
        assertEquals(AwaitInteractionStatus.FAILED, failed.get().status());
    }

    @Test
    void markTimedOutTransitionsToTimedOutStatus() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        var created = store.createOrGet(createCommand("idem-1", 10_000L, 70_000L)).await().indefinitely();
        String interactionId = created.record().interactionId();

        var timedOut = store.markTimedOut("tenant", interactionId, 0L, 71_000L)
            .await().indefinitely();

        assertTrue(timedOut.isPresent());
        assertEquals(AwaitInteractionStatus.TIMED_OUT, timedOut.get().status());
    }

    @Test
    void transitionOnTerminalInteractionReturnsEmpty() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        var created = store.createOrGet(createCommand("idem-1", 10_000L, 70_000L)).await().indefinitely();
        String interactionId = created.record().interactionId();

        // First cancel
        store.cancel("tenant", interactionId, 0L, "user request", 15_000L).await().indefinitely();
        // Attempting to cancel again should return empty since it's terminal
        var result = store.cancel("tenant", interactionId, 1L, "again", 16_000L).await().indefinitely();

        assertTrue(result.isEmpty());
    }

    @Test
    void findTimedOutReturnsOnlyExpiredNonTerminal() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        // Create one that will be expired
        store.createOrGet(createCommand("idem-1", 10_000L, 20_000L)).await().indefinitely();
        // Create one that won't expire
        store.createOrGet(new AwaitCreateCommand(
            "tenant", "execution-2", "review", 0, String.class.getName(),
            "cause-2", "idem-2", "corr-2", Map.of(), null, null, "interaction-api",
            "unit-2", null, 10_000L, 80_000L, Long.MAX_VALUE)).await().indefinitely();

        var timedOut = store.findTimedOut(21_000L, 10).await().indefinitely();

        assertEquals(1, timedOut.size());
        assertEquals("idem-1", timedOut.getFirst().idempotencyKey());
    }

    @Test
    void findTimedOutRespectsLimit() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        // Create multiple expired interactions
        for (int i = 0; i < 5; i++) {
            store.createOrGet(new AwaitCreateCommand(
                "tenant", "execution-" + i, "review", 0, String.class.getName(),
                "cause-" + i, "idem-" + i, "corr-" + i, Map.of(), null, null, "interaction-api",
                "unit-" + i, null, 10_000L, 20_000L, Long.MAX_VALUE)).await().indefinitely();
        }

        var timedOut = store.findTimedOut(21_000L, 3).await().indefinitely();

        assertEquals(3, timedOut.size());
    }

    @Test
    void queryPendingFiltersByStepId() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        store.createOrGet(createCommand("idem-1", 10_000L, 70_000L)).await().indefinitely();
        store.createOrGet(new AwaitCreateCommand(
            "tenant", "execution-2", "fraud-check", 0, String.class.getName(),
            "cause-2", "idem-2", "corr-2", Map.of(), "alice", "finance", "interaction-api",
            "unit-2", null, 10_000L, 70_000L, Long.MAX_VALUE)).await().indefinitely();

        var reviewSteps = store.queryPending("tenant", null, null, "review", 10).await().indefinitely();
        var fraudSteps = store.queryPending("tenant", null, null, "fraud-check", 10).await().indefinitely();

        assertEquals(1, reviewSteps.size());
        assertEquals("review", reviewSteps.getFirst().stepId());
        assertEquals(1, fraudSteps.size());
        assertEquals("fraud-check", fraudSteps.getFirst().stepId());
    }

    @Test
    void queryPendingExcludesTerminalInteractions() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        var created = store.createOrGet(createCommand("idem-1", 10_000L, 70_000L)).await().indefinitely();
        // Complete the interaction
        store.complete(new AwaitCompletionCommand(
            "tenant", created.record().interactionId(), null, "completion-1", "result", "alice", 15_000L))
            .await().indefinitely();

        var pending = store.queryPending("tenant", null, null, null, 10).await().indefinitely();

        assertTrue(pending.isEmpty());
    }

    @Test
    void queryPendingExcludesDifferentTenant() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        store.createOrGet(createCommand("idem-1", 10_000L, 70_000L)).await().indefinitely();

        var otherTenant = store.queryPending("other-tenant", null, null, null, 10).await().indefinitely();
        var sameTenant = store.queryPending("tenant", null, null, null, 10).await().indefinitely();

        assertTrue(otherTenant.isEmpty());
        assertEquals(1, sameTenant.size());
    }

    @Test
    void providerNameIsMemory() {
        assertEquals("memory", new InMemoryAwaitInteractionStore().providerName());
    }

    @Test
    void priorityIsNegative() {
        assertTrue(new InMemoryAwaitInteractionStore().priority() < 0);
    }

    @Test
    void completionCanResolveByInteractionId() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        var created = store.createOrGet(createCommand("idem-1", 10_000L, 70_000L)).await().indefinitely();

        var completed = store.complete(new AwaitCompletionCommand(
            "tenant",
            created.record().interactionId(),
            null,
            "completion-1",
            "result-payload",
            "charlie",
            12_000L)).await().indefinitely();

        assertFalse(completed.duplicate());
        assertEquals(AwaitInteractionStatus.COMPLETED, completed.record().status());
        assertEquals("result-payload", completed.record().responsePayload());
        assertEquals("charlie", completed.record().actor());
    }

    @Test
    void completionThrowsWhenNoMatchFound() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();

        assertThrows(AwaitInteractionNotFoundException.class, () -> store.complete(new AwaitCompletionCommand(
            "tenant", "nonexistent-id", null, "completion-1", null, null, 12_000L))
            .await().indefinitely());
    }

    @Test
    void completionThrowsForOtherTerminalStates() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        var created = store.createOrGet(createCommand("idem-1", 10_000L, 70_000L)).await().indefinitely();
        // Cancel to put in terminal state
        store.cancel("tenant", created.record().interactionId(), 0L, "cancelled", 15_000L)
            .await().indefinitely();

        assertThrows(AwaitInteractionTerminalException.class, () -> store.complete(new AwaitCompletionCommand(
            "tenant", created.record().interactionId(), null, "completion-1", "result", "alice", 16_000L))
            .await().indefinitely());
    }

    @Test
    void createsNewInteractionAfterTerminalDeduplication() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        AwaitCreateCommand command = createCommand("idem-1", 10_000L, 70_000L);
        var first = store.createOrGet(command).await().indefinitely();

        // Complete the first interaction
        store.complete(new AwaitCompletionCommand(
            "tenant", first.record().interactionId(), null, "completion-1", "result", "alice", 15_000L))
            .await().indefinitely();

        // Create new one with same idempotency key - completed state is terminal, so new record is created
        var second = store.createOrGet(command).await().indefinitely();

        assertFalse(second.duplicate());
        // A new interaction id should be generated
        assertNotNull(second.record().interactionId());
    }

    @Test
    void versionIncrementsOnMarkDispatched() {
        InMemoryAwaitInteractionStore store = new InMemoryAwaitInteractionStore();
        var created = store.createOrGet(createCommand("idem-1", 10_000L, 70_000L)).await().indefinitely();
        assertEquals(0L, created.record().version());

        var claimed = store.markDispatching(
            "tenant", created.record().interactionId(), 0L, 11_000L)
            .await().indefinitely().orElseThrow();
        var dispatched = store.markDispatched(
            "tenant", created.record().interactionId(), claimed.version(), Map.of(), 12_000L)
            .await().indefinitely();

        assertTrue(dispatched.isPresent());
        assertEquals(2L, dispatched.get().version());
    }

    private AwaitCreateCommand createCommand(String idempotencyKey, long nowEpochMs, long deadlineEpochMs) {
        return new AwaitCreateCommand(
            "tenant",
            "execution-1",
            "review",
            0,
            String.class.getName(),
            "cause-1",
            idempotencyKey,
            "corr-1",
            Map.of("orderId", "o-1"),
            "alice",
            "finance",
            "interaction-api",
            "unit-1",
            null,
            nowEpochMs,
            deadlineEpochMs,
            Long.MAX_VALUE);
    }

    private AwaitCreateCommand itemCommand(String idempotencyKey, String correlationId, int itemIndex) {
        return new AwaitCreateCommand(
            "tenant",
            "execution-1",
            "await-payment-provider",
            3,
            String.class.getName(),
            "execution-1:3:" + itemIndex,
            idempotencyKey,
            correlationId,
            Map.of("index", itemIndex),
            null,
            null,
            "kafka",
            "unit-1",
            itemIndex,
            10_000L,
            70_000L,
            Long.MAX_VALUE);
    }

    private static StreamRegionRecord streamRegion() {
        return new StreamRegionRecord(
            "tenant", "execution-1", "resumable-source",
            new ResumableSourceDescriptor("test", "deterministic", "sha256:source"),
            OpaqueSourceCheckpoint.initial(), 0L, 0, 2, StreamRegionStatus.ACTIVE, java.util.Optional.empty(),
            0L, "", 0L, 1_000L, 1L, 1L, Long.MAX_VALUE);
    }

    private static AwaitInteractionRecord streamInteraction(StreamRegionRecord region, int ordinal) {
        return new AwaitInteractionRecord(
            region.tenantId(), region.executionId(), "await-payment-provider", 3, String.class.getName(),
            StreamRegionPageCommit.interactionId(region, ordinal), "stream-correlation-" + ordinal,
            region.executionId() + ":3:" + ordinal, "stream-idempotency-" + ordinal, 0L,
            AwaitInteractionStatus.WAITING, Map.of("ordinal", ordinal), null, "unit-1", ordinal,
            null, null, null, "kafka", Map.of(), 70_000L, 1_000L, 1_000L, Long.MAX_VALUE,
            String.class.getName(), AwaitContinuationStatus.HELD, 0, 0L, "", 0L, null, region.regionId());
    }
}
