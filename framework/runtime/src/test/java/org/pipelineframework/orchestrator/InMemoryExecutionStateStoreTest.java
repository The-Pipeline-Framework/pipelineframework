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
        ExecutionCreateCommand command = new ExecutionCreateCommand("tenant-a", "key-1", "payload", now, now / 1000 + 60);

        CreateExecutionResult first = store.createOrGetExecution(command).await().indefinitely();
        CreateExecutionResult second = store.createOrGetExecution(command).await().indefinitely();

        assertFalse(first.duplicate());
        assertTrue(second.duplicate());
        assertEquals(first.record().executionId(), second.record().executionId());
    }

    @Test
    void claimAndMarkSucceededUsesVersionGuard() {
        InMemoryExecutionStateStore store = new InMemoryExecutionStateStore();
        long now = System.currentTimeMillis();
        CreateExecutionResult created = store.createOrGetExecution(
            new ExecutionCreateCommand("tenant-a", "key-2", "payload", now, now / 1000 + 60))
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
                new ExecutionCreateCommand("tenant-a", "key-3", "payload", now, now / 1000 + 60))
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
    void scopedCompositeKeysAvoidTenantAndKeySeparatorCollisions() {
        InMemoryExecutionStateStore store = new InMemoryExecutionStateStore();
        long now = System.currentTimeMillis();

        CreateExecutionResult first = store.createOrGetExecution(
                new ExecutionCreateCommand("a|b", "c", "payload", now, now / 1000 + 60))
            .await().indefinitely();
        CreateExecutionResult second = store.createOrGetExecution(
                new ExecutionCreateCommand("a", "b|c", "payload", now, now / 1000 + 60))
            .await().indefinitely();

        assertFalse(first.duplicate());
        assertFalse(second.duplicate());
        assertNotEquals(first.record().executionId(), second.record().executionId());
    }
}
