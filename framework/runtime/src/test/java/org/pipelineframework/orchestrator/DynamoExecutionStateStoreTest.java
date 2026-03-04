package org.pipelineframework.orchestrator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DynamoExecutionStateStoreTest {

    private DynamoExecutionStateStore store;

    @BeforeEach
    void setUp() {
        store = new DynamoExecutionStateStore();
    }

    @Test
    void providerNameIsDynamo() {
        assertEquals("dynamo", store.providerName());
    }

    @Test
    void priorityIsNegative() {
        assertEquals(-1000, store.priority());
    }

    @Test
    void createOrGetExecutionThrowsUnsupportedOperation() {
        ExecutionCreateCommand command = new ExecutionCreateCommand(
            "tenant1",
            "key1",
            null,
            System.currentTimeMillis(),
            System.currentTimeMillis() / 1000 + 86400);

        assertThrows(UnsupportedOperationException.class,
            () -> store.createOrGetExecution(command).await().indefinitely());
    }

    @Test
    void getExecutionThrowsUnsupportedOperation() {
        assertThrows(UnsupportedOperationException.class,
            () -> store.getExecution("tenant1", "exec1").await().indefinitely());
    }

    @Test
    void claimLeaseThrowsUnsupportedOperation() {
        assertThrows(UnsupportedOperationException.class,
            () -> store.claimLease("tenant1", "exec1", "worker1", System.currentTimeMillis(), 30000L)
                .await().indefinitely());
    }

    @Test
    void markSucceededThrowsUnsupportedOperation() {
        assertThrows(UnsupportedOperationException.class,
            () -> store.markSucceeded("tenant1", "exec1", 1L, "key1", null, System.currentTimeMillis())
                .await().indefinitely());
    }

    @Test
    void scheduleRetryThrowsUnsupportedOperation() {
        assertThrows(UnsupportedOperationException.class,
            () -> store.scheduleRetry("tenant1", "exec1", 1L, 2, System.currentTimeMillis() + 5000L,
                "key1", "TestError", "test message", System.currentTimeMillis())
                .await().indefinitely());
    }

    @Test
    void markTerminalFailureThrowsUnsupportedOperation() {
        assertThrows(UnsupportedOperationException.class,
            () -> store.markTerminalFailure("tenant1", "exec1", 1L, ExecutionStatus.FAILED,
                "key1", "TestError", "test message", System.currentTimeMillis())
                .await().indefinitely());
    }

    @Test
    void findDueExecutionsThrowsUnsupportedOperation() {
        assertThrows(UnsupportedOperationException.class,
            () -> store.findDueExecutions(System.currentTimeMillis(), 10)
                .await().indefinitely());
    }

    @Test
    void errorMessageIndicatesMissingImplementation() {
        ExecutionCreateCommand command = new ExecutionCreateCommand(
            "tenant1",
            "key1",
            null,
            System.currentTimeMillis(),
            System.currentTimeMillis() / 1000 + 86400);

        try {
            store.createOrGetExecution(command).await().indefinitely();
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            assertTrue(e.getMessage().contains("DynamoExecutionStateStore"));
            assertTrue(e.getMessage().contains("not implemented"));
        }
    }
}