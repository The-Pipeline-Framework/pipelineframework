package org.pipelineframework.orchestrator;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class InMemoryExecutionStateStoreTest {

    @Test
    void createOrGetReturnsDuplicateForSameKey() {
        InMemoryExecutionStateStore store = new InMemoryExecutionStateStore();
        long now = System.currentTimeMillis();
        ExecutionCreateCommand command = new ExecutionCreateCommand(
            "tenant-a",
            "key-1",
            "org.example.pipeline",
            "sha256:contract",
            "sha256:release",
            "payload",
            ExecutionResultShape.SINGLE,
            now,
            now / 1000 + 60);

        CreateExecutionResult first = store.createOrGetExecution(command).await().indefinitely();
        CreateExecutionResult second = store.createOrGetExecution(command).await().indefinitely();

        assertFalse(first.duplicate());
        assertTrue(second.duplicate());
        assertEquals(first.record().executionId(), second.record().executionId());
        assertEquals("org.example.pipeline", first.record().pipelineId());
        assertEquals("sha256:contract", first.record().contractVersion());
        assertEquals("sha256:release", first.record().releaseVersion());
    }

    @Test
    void claimAndMarkSucceededUsesVersionGuard() {
        InMemoryExecutionStateStore store = new InMemoryExecutionStateStore();
        long now = System.currentTimeMillis();
        CreateExecutionResult created = store.createOrGetExecution(
            new ExecutionCreateCommand("tenant-a", "key-2", "payload", ExecutionResultShape.SINGLE, now, now / 1000 + 60))
            .await().indefinitely();

        Optional<ExecutionRecord<Object, Object>> claimed = store.claimLease(
                "tenant-a",
                created.record().executionId(),
                "worker-1",
                now,
                1000)
            .await().indefinitely();

        assertTrue(claimed.isPresent());
        ExecutionRecord<Object, Object> record = claimed.get();
        assertEquals(ExecutionStatus.RUNNING, record.status());

        Optional<ExecutionRecord<Object, Object>> staleCommit = store.markSucceeded(
                "tenant-a",
                record.executionId(),
                record.version() - 1,
                "transition",
                List.of("ok"),
                now + 1)
            .await().indefinitely();
        assertTrue(staleCommit.isEmpty());

        Optional<ExecutionRecord<Object, Object>> committed = store.markSucceeded(
                "tenant-a",
                record.executionId(),
                record.version(),
                "transition",
                List.of("ok"),
                now + 2)
            .await().indefinitely();

        assertTrue(committed.isPresent());
        assertEquals(ExecutionStatus.SUCCEEDED, committed.get().status());
    }

    @Test
    void dueSweepOnlyReturnsNonTerminalDueRecords() {
        InMemoryExecutionStateStore store = new InMemoryExecutionStateStore();
        long now = System.currentTimeMillis();

        CreateExecutionResult created = store.createOrGetExecution(
                new ExecutionCreateCommand("tenant-a", "key-3", "payload", ExecutionResultShape.SINGLE, now, now / 1000 + 60))
            .await().indefinitely();

        Optional<ExecutionRecord<Object, Object>> claimed = store.claimLease(
                "tenant-a",
                created.record().executionId(),
                "worker-1",
                now,
                10)
            .await().indefinitely();

        assertTrue(claimed.isPresent());

        Optional<ExecutionRecord<Object, Object>> retried = store.scheduleRetry(
                "tenant-a",
                created.record().executionId(),
                claimed.get().version(),
                1,
                now - 1,
                "transition",
                "ERR",
                "failure",
                now + 1)
            .await().indefinitely();

        assertTrue(retried.isPresent());

        List<ExecutionRecord<Object, Object>> due = store.findDueExecutions(now + 2, 10).await().indefinitely();
        assertEquals(1, due.size());
        assertEquals(created.record().executionId(), due.get(0).executionId());
    }

    @Test
    void redriveTerminalExecutionQueuesDlqAndPreservesPinnedIdentity() {
        InMemoryExecutionStateStore store = new InMemoryExecutionStateStore();
        long now = System.currentTimeMillis();
        CreateExecutionResult created = store.createOrGetExecution(
                new ExecutionCreateCommand(
                    "tenant-a",
                    "key-redrive",
                    "org.example.pipeline",
                    "sha256:contract",
                    "sha256:release",
                    "payload",
                    ExecutionResultShape.SINGLE,
                    now,
                    now / 1000 + 60))
            .await().indefinitely();
        Optional<ExecutionRecord<Object, Object>> terminal = store.markTerminalFailure(
                "tenant-a",
                created.record().executionId(),
                created.record().version(),
                ExecutionStatus.DLQ,
                "transition",
                "ERR",
                "failure",
                now + 1)
            .await().indefinitely();

        assertTrue(terminal.isPresent());
        Optional<ExecutionRecord<Object, Object>> redriven = store.redriveTerminalExecution(
                "tenant-a",
                created.record().executionId(),
                terminal.get().version(),
                false,
                "redrive",
                now + 2)
            .await().indefinitely();

        assertTrue(redriven.isPresent());
        ExecutionRecord<Object, Object> record = redriven.get();
        assertEquals(ExecutionStatus.QUEUED, record.status());
        assertEquals("org.example.pipeline", record.pipelineId());
        assertEquals("sha256:contract", record.contractVersion());
        assertEquals("sha256:release", record.releaseVersion());
        assertEquals(terminal.get().attempt() + 1, record.attempt());
        assertNull(record.leaseOwner());
        assertEquals(0L, record.leaseExpiresEpochMs());
        assertEquals(now + 2, record.nextDueEpochMs());
        assertNull(record.awaitUnitId());
        assertNull(record.resultPayload());
        assertNull(record.errorCode());
        assertNull(record.errorMessage());
    }

    @Test
    void redriveTerminalExecutionRequiresAllowFailedForFailedStatus() {
        InMemoryExecutionStateStore store = new InMemoryExecutionStateStore();
        long now = System.currentTimeMillis();
        CreateExecutionResult created = store.createOrGetExecution(
                new ExecutionCreateCommand("tenant-a", "key-redrive-failed", "payload", ExecutionResultShape.SINGLE, now, now / 1000 + 60))
            .await().indefinitely();
        Optional<ExecutionRecord<Object, Object>> terminal = store.markTerminalFailure(
                "tenant-a",
                created.record().executionId(),
                created.record().version(),
                ExecutionStatus.FAILED,
                "transition",
                "ERR",
                "failure",
                now + 1)
            .await().indefinitely();

        assertTrue(terminal.isPresent());
        assertTrue(store.redriveTerminalExecution(
                "tenant-a",
                created.record().executionId(),
                terminal.get().version(),
                false,
                "redrive",
                now + 2)
            .await().indefinitely().isEmpty());
        assertTrue(store.redriveTerminalExecution(
                "tenant-a",
                created.record().executionId(),
                terminal.get().version(),
                true,
                "redrive",
                now + 3)
            .await().indefinitely().isPresent());
    }

    @Test
    void redriveTerminalExecutionRejectsStaleVersionAndNonTerminalStatus() {
        InMemoryExecutionStateStore store = new InMemoryExecutionStateStore();
        long now = System.currentTimeMillis();
        CreateExecutionResult created = store.createOrGetExecution(
                new ExecutionCreateCommand("tenant-a", "key-redrive-stale", "payload", ExecutionResultShape.SINGLE, now, now / 1000 + 60))
            .await().indefinitely();

        assertTrue(store.redriveTerminalExecution(
                "tenant-a",
                created.record().executionId(),
                created.record().version(),
                true,
                "redrive",
                now + 1)
            .await().indefinitely().isEmpty());
        Optional<ExecutionRecord<Object, Object>> terminal = store.markTerminalFailure(
                "tenant-a",
                created.record().executionId(),
                created.record().version(),
                ExecutionStatus.DLQ,
                "transition",
                "ERR",
                "failure",
                now + 2)
            .await().indefinitely();
        assertTrue(terminal.isPresent());

        assertTrue(store.redriveTerminalExecution(
                "tenant-a",
                created.record().executionId(),
                terminal.get().version() - 1,
                false,
                "redrive",
                now + 3)
            .await().indefinitely().isEmpty());
    }

    @Test
    void redriveTerminalExecutionPreservesAwaitResumePointer() {
        InMemoryExecutionStateStore store = new InMemoryExecutionStateStore();
        long now = System.currentTimeMillis();
        CreateExecutionResult created = store.createOrGetExecution(
                new ExecutionCreateCommand(
                    "tenant-a",
                    "key-redrive-await",
                    "payload",
                    ExecutionResultShape.SINGLE,
                    now,
                    now / 1000 + 60))
            .await().indefinitely();

        Optional<ExecutionRecord<Object, Object>> waiting = store.markWaitingExternal(
                "tenant-a",
                created.record().executionId(),
                created.record().version(),
                "transition-await",
                "unit-1",
                2,
                now + 1)
            .await().indefinitely();
        assertTrue(waiting.isPresent());
        Optional<ExecutionRecord<Object, Object>> queued = store.markAwaitCompleted(
                "tenant-a",
                created.record().executionId(),
                "unit-1",
                3,
                now + 2)
            .await().indefinitely();
        assertTrue(queued.isPresent());
        Optional<ExecutionRecord<Object, Object>> terminal = store.markTerminalFailure(
                "tenant-a",
                created.record().executionId(),
                queued.get().version(),
                ExecutionStatus.FAILED,
                "transition-failed-after-await",
                "ERR",
                "failure",
                now + 3)
            .await().indefinitely();
        assertTrue(terminal.isPresent());

        Optional<ExecutionRecord<Object, Object>> redriven = store.redriveTerminalExecution(
                "tenant-a",
                created.record().executionId(),
                terminal.get().version(),
                true,
                "redrive",
                now + 4)
            .await().indefinitely();

        assertTrue(redriven.isPresent());
        assertEquals("unit-1", redriven.get().awaitUnitId());
        assertEquals(3, redriven.get().currentStepIndex());
    }

    @Test
    void retryAfterAwaitResumePreservesAwaitUnitId() {
        InMemoryExecutionStateStore store = new InMemoryExecutionStateStore();
        long now = System.currentTimeMillis();
        CreateExecutionResult created = store.createOrGetExecution(
                new ExecutionCreateCommand(
                    "tenant-a",
                    "key-await-retry",
                    "payload",
                    ExecutionResultShape.MATERIALIZED_MULTI,
                    now,
                    now / 1000 + 60))
            .await().indefinitely();

        Optional<ExecutionRecord<Object, Object>> waiting = store.markWaitingExternal(
                "tenant-a",
                created.record().executionId(),
                created.record().version(),
                "transition-await",
                "unit-1",
                5,
                now + 1)
            .await().indefinitely();
        assertTrue(waiting.isPresent());

        Optional<ExecutionRecord<Object, Object>> queued = store.markAwaitCompleted(
                "tenant-a",
                created.record().executionId(),
                "unit-1",
                6,
                now + 2)
            .await().indefinitely();
        assertTrue(queued.isPresent());

        Optional<ExecutionRecord<Object, Object>> claimed = store.claimLease(
                "tenant-a",
                created.record().executionId(),
                "worker-1",
                now + 3,
                1000)
            .await().indefinitely();
        assertTrue(claimed.isPresent());

        Optional<ExecutionRecord<Object, Object>> retried = store.scheduleRetry(
                "tenant-a",
                created.record().executionId(),
                claimed.get().version(),
                1,
                now + 1000,
                "transition-retry",
                "ERR",
                "failure",
                now + 4)
            .await().indefinitely();

        assertTrue(retried.isPresent());
        assertEquals("unit-1", retried.get().awaitUnitId());
    }

    @Test
    void itemizedAwaitReleaseStoresSnapshotValueCopy() {
        InMemoryExecutionStateStore store = new InMemoryExecutionStateStore();
        long now = System.currentTimeMillis();
        CreateExecutionResult created = store.createOrGetExecution(
                new ExecutionCreateCommand(
                    "tenant-a",
                    "key-itemized-release",
                    "payload",
                    ExecutionResultShape.MATERIALIZED_MULTI,
                    now,
                    now / 1000 + 60))
            .await().indefinitely();
        Optional<ExecutionRecord<Object, Object>> waiting = store.markWaitingExternal(
                "tenant-a",
                created.record().executionId(),
                created.record().version(),
                "transition-await",
                "unit-1",
                5,
                now + 1)
            .await().indefinitely();
        assertTrue(waiting.isPresent());
        ExecutionInputSnapshot resumeInput = new ExecutionInputSnapshot(
            ExecutionInputShape.MULTI,
            new java.util.ArrayList<>(List.of("a", "b")));

        Optional<ExecutionRecord<Object, Object>> queued = store.markAwaitItemContinuationsCompleted(
                "tenant-a",
                created.record().executionId(),
                "unit-1",
                6,
                resumeInput,
                now + 2)
            .await().indefinitely();

        assertTrue(queued.isPresent());
        ExecutionInputSnapshot stored = assertInstanceOf(ExecutionInputSnapshot.class, queued.get().inputPayload());
        assertNotSame(resumeInput, stored);
        assertEquals(ExecutionInputShape.MULTI, stored.shape());
        assertEquals(List.of("a", "b"), stored.payload());
        assertNotSame(resumeInput.payload(), stored.payload());
        assertEquals(created.record().pipelineId(), queued.get().pipelineId());
        assertEquals(created.record().contractVersion(), queued.get().contractVersion());
        assertEquals(created.record().releaseVersion(), queued.get().releaseVersion());
    }

    @Test
    void dueSweepReturnsEmptyWhenLimitIsNonPositive() {
        InMemoryExecutionStateStore store = new InMemoryExecutionStateStore();
        long now = System.currentTimeMillis();
        store.createOrGetExecution(
                new ExecutionCreateCommand("tenant-a", "key-limit", "payload", ExecutionResultShape.SINGLE, now, now / 1000 + 60))
            .await().indefinitely();

        assertTrue(store.findDueExecutions(now + 1, 0).await().indefinitely().isEmpty());
        assertTrue(store.findDueExecutions(now + 1, -1).await().indefinitely().isEmpty());
    }

    @Test
    void createOrGetRecreatesExecutionWhenExistingRecordExpired() {
        InMemoryExecutionStateStore store = new InMemoryExecutionStateStore();
        long now = System.currentTimeMillis();
        CreateExecutionResult expired = store.createOrGetExecution(
                new ExecutionCreateCommand("tenant-a", "key-expired", "payload", ExecutionResultShape.SINGLE, now, now / 1000 - 1))
            .await().indefinitely();

        CreateExecutionResult recreated = store.createOrGetExecution(
                new ExecutionCreateCommand("tenant-a", "key-expired", "payload", ExecutionResultShape.SINGLE, now + 2000, now / 1000 + 60))
            .await().indefinitely();

        assertFalse(expired.duplicate());
        assertFalse(recreated.duplicate());
        assertNotEquals(expired.record().executionId(), recreated.record().executionId());
        assertTrue(store.getExecution("tenant-a", expired.record().executionId()).await().indefinitely().isEmpty());
    }

    @Test
    void expiredExecutionIsEvictedForGetAndClaim() {
        InMemoryExecutionStateStore store = new InMemoryExecutionStateStore();
        long now = System.currentTimeMillis();
        CreateExecutionResult expired = store.createOrGetExecution(
                new ExecutionCreateCommand("tenant-a", "key-expired-evict", "payload", ExecutionResultShape.SINGLE, now, now / 1000 - 1))
            .await().indefinitely();

        Optional<ExecutionRecord<Object, Object>> fetched = store.getExecution(
                "tenant-a",
                expired.record().executionId())
            .await().indefinitely();
        Optional<ExecutionRecord<Object, Object>> claimed = store.claimLease(
                "tenant-a",
                expired.record().executionId(),
                "worker-1",
                now + 1,
                1000)
            .await().indefinitely();

        assertTrue(fetched.isEmpty());
        assertTrue(claimed.isEmpty());
    }

    @Test
    void scopedCompositeKeysAvoidTenantAndKeySeparatorCollisions() {
        InMemoryExecutionStateStore store = new InMemoryExecutionStateStore();
        long now = System.currentTimeMillis();

        CreateExecutionResult first = store.createOrGetExecution(
                new ExecutionCreateCommand("a|b", "c", "payload", ExecutionResultShape.SINGLE, now, now / 1000 + 60))
            .await().indefinitely();
        CreateExecutionResult second = store.createOrGetExecution(
                new ExecutionCreateCommand("a", "b|c", "payload", ExecutionResultShape.SINGLE, now, now / 1000 + 60))
            .await().indefinitely();

        assertFalse(first.duplicate());
        assertFalse(second.duplicate());
        assertNotEquals(first.record().executionId(), second.record().executionId());
    }

    @Test
    void scopedCompositeKeysIsolateTerminalFailureAndLookupByExecutionId() {
        InMemoryExecutionStateStore store = new InMemoryExecutionStateStore();
        long now = System.currentTimeMillis();
        CreateExecutionResult first = store.createOrGetExecution(
                new ExecutionCreateCommand("a|b", "c", "payload", ExecutionResultShape.SINGLE, now, now / 1000 + 60))
            .await().indefinitely();
        CreateExecutionResult second = store.createOrGetExecution(
                new ExecutionCreateCommand("a", "b|c", "payload", ExecutionResultShape.SINGLE, now, now / 1000 + 60))
            .await().indefinitely();

        Optional<ExecutionRecord<Object, Object>> failed = store.markTerminalFailure(
                first.record().tenantId(),
                first.record().executionId(),
                first.record().version(),
                ExecutionStatus.FAILED,
                "transition",
                "ERR",
                "failed",
                now + 1)
            .await().indefinitely();
        Optional<ExecutionRecord<Object, Object>> firstRead = store.getExecution(
                first.record().tenantId(),
                first.record().executionId())
            .await().indefinitely();
        Optional<ExecutionRecord<Object, Object>> secondRead = store.getExecution(
                second.record().tenantId(),
                second.record().executionId())
            .await().indefinitely();

        assertTrue(failed.isPresent());
        assertTrue(firstRead.isPresent());
        assertTrue(secondRead.isPresent());
        assertEquals(ExecutionStatus.FAILED, firstRead.get().status());
        assertTrue(firstRead.get().status().terminal());
        assertFalse(secondRead.get().status().terminal());
    }
}
