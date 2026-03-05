package org.pipelineframework.orchestrator;

import java.util.List;
import java.util.Optional;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ExecutionStateStoreTest {

    @Test
    void defaultProviderNameIsMemory() {
        ExecutionStateStore store = new TestExecutionStateStore();
        assertEquals("memory", store.providerName());
    }

    @Test
    void defaultPriorityIsZero() {
        ExecutionStateStore store = new TestExecutionStateStore();
        assertEquals(0, store.priority());
    }

    @Test
    void customProviderNameOverridesDefault() {
        ExecutionStateStore store = new ExecutionStateStore() {
            @Override
            public String providerName() {
                return "redis";
            }

            @Override
            public Uni<CreateExecutionResult> createOrGetExecution(ExecutionCreateCommand command) {
                return Uni.createFrom().nullItem();
            }

            @Override
            public Uni<Optional<ExecutionRecord<Object, Object>>> getExecution(String tenantId, String executionId) {
                return Uni.createFrom().item(Optional.empty());
            }

            @Override
            public Uni<Optional<ExecutionRecord<Object, Object>>> claimLease(
                String tenantId, String executionId, String leaseOwner, long nowEpochMs, long leaseMs) {
                return Uni.createFrom().item(Optional.empty());
            }

            @Override
            public Uni<Optional<ExecutionRecord<Object, Object>>> markSucceeded(
                String tenantId, String executionId, long expectedVersion, String transitionKey,
                Object resultPayload, long nowEpochMs) {
                return Uni.createFrom().item(Optional.empty());
            }

            @Override
            public Uni<Optional<ExecutionRecord<Object, Object>>> scheduleRetry(
                String tenantId, String executionId, long expectedVersion, int nextAttempt,
                long nextDueEpochMs, String transitionKey, String errorCode, String errorMessage, long nowEpochMs) {
                return Uni.createFrom().item(Optional.empty());
            }

            @Override
            public Uni<Optional<ExecutionRecord<Object, Object>>> markTerminalFailure(
                String tenantId, String executionId, long expectedVersion, ExecutionStatus finalStatus,
                String transitionKey, String errorCode, String errorMessage, long nowEpochMs) {
                return Uni.createFrom().item(Optional.empty());
            }

            @Override
            public Uni<List<ExecutionRecord<Object, Object>>> findDueExecutions(long nowEpochMs, int limit) {
                return Uni.createFrom().item(List.of());
            }
        };

        assertEquals("redis", store.providerName());
    }

    @Test
    void customPriorityOverridesDefault() {
        ExecutionStateStore store = new ExecutionStateStore() {
            @Override
            public int priority() {
                return 50;
            }

            @Override
            public Uni<CreateExecutionResult> createOrGetExecution(ExecutionCreateCommand command) {
                return Uni.createFrom().nullItem();
            }

            @Override
            public Uni<Optional<ExecutionRecord<Object, Object>>> getExecution(String tenantId, String executionId) {
                return Uni.createFrom().item(Optional.empty());
            }

            @Override
            public Uni<Optional<ExecutionRecord<Object, Object>>> claimLease(
                String tenantId, String executionId, String leaseOwner, long nowEpochMs, long leaseMs) {
                return Uni.createFrom().item(Optional.empty());
            }

            @Override
            public Uni<Optional<ExecutionRecord<Object, Object>>> markSucceeded(
                String tenantId, String executionId, long expectedVersion, String transitionKey,
                Object resultPayload, long nowEpochMs) {
                return Uni.createFrom().item(Optional.empty());
            }

            @Override
            public Uni<Optional<ExecutionRecord<Object, Object>>> scheduleRetry(
                String tenantId, String executionId, long expectedVersion, int nextAttempt,
                long nextDueEpochMs, String transitionKey, String errorCode, String errorMessage, long nowEpochMs) {
                return Uni.createFrom().item(Optional.empty());
            }

            @Override
            public Uni<Optional<ExecutionRecord<Object, Object>>> markTerminalFailure(
                String tenantId, String executionId, long expectedVersion, ExecutionStatus finalStatus,
                String transitionKey, String errorCode, String errorMessage, long nowEpochMs) {
                return Uni.createFrom().item(Optional.empty());
            }

            @Override
            public Uni<List<ExecutionRecord<Object, Object>>> findDueExecutions(long nowEpochMs, int limit) {
                return Uni.createFrom().item(List.of());
            }
        };

        assertEquals(50, store.priority());
    }

    @Test
    void allMethodsRequireImplementation() {
        ExecutionStateStore store = new TestExecutionStateStore();
        long now = System.currentTimeMillis();

        ExecutionCreateCommand command = new ExecutionCreateCommand(
            "tenant1",
            "key1",
            new ExecutionInputSnapshot(ExecutionInputShape.UNI, "input"),
            now,
            now / 1000 + 86400
        );

        assertNotNull(store.createOrGetExecution(command));
        assertNotNull(store.getExecution("tenant1", "exec1"));
        assertNotNull(store.claimLease("tenant1", "exec1", "worker1", now, 30000L));
        assertNotNull(store.markSucceeded("tenant1", "exec1", 1L, "key1", "result", now));
        assertNotNull(store.scheduleRetry("tenant1", "exec1", 1L, 2, now + 5000, "key1", "Error", "msg", now));
        assertNotNull(store.markTerminalFailure("tenant1", "exec1", 1L, ExecutionStatus.FAILED, "key1", "Error", "msg", now));
        assertNotNull(store.findDueExecutions(now, 10));
    }

    private static class TestExecutionStateStore implements ExecutionStateStore {
        @Override
        public Uni<CreateExecutionResult> createOrGetExecution(ExecutionCreateCommand command) {
            return Uni.createFrom().item(new CreateExecutionResult(createTestRecord(), false));
        }

        @Override
        public Uni<Optional<ExecutionRecord<Object, Object>>> getExecution(String tenantId, String executionId) {
            return Uni.createFrom().item(Optional.of(createTestRecord()));
        }

        @Override
        public Uni<Optional<ExecutionRecord<Object, Object>>> claimLease(
            String tenantId, String executionId, String leaseOwner, long nowEpochMs, long leaseMs) {
            return Uni.createFrom().item(Optional.of(createTestRecord()));
        }

        @Override
        public Uni<Optional<ExecutionRecord<Object, Object>>> markSucceeded(
            String tenantId, String executionId, long expectedVersion, String transitionKey,
            Object resultPayload, long nowEpochMs) {
            return Uni.createFrom().item(Optional.of(createTestRecord()));
        }

        @Override
        public Uni<Optional<ExecutionRecord<Object, Object>>> scheduleRetry(
            String tenantId, String executionId, long expectedVersion, int nextAttempt,
            long nextDueEpochMs, String transitionKey, String errorCode, String errorMessage, long nowEpochMs) {
            return Uni.createFrom().item(Optional.of(createTestRecord()));
        }

        @Override
        public Uni<Optional<ExecutionRecord<Object, Object>>> markTerminalFailure(
            String tenantId, String executionId, long expectedVersion, ExecutionStatus finalStatus,
            String transitionKey, String errorCode, String errorMessage, long nowEpochMs) {
            return Uni.createFrom().item(Optional.of(createTestRecord()));
        }

        @Override
        public Uni<List<ExecutionRecord<Object, Object>>> findDueExecutions(long nowEpochMs, int limit) {
            return Uni.createFrom().item(List.of(createTestRecord()));
        }

        private ExecutionRecord<Object, Object> createTestRecord() {
            return new ExecutionRecord<>(
                "tenant1",
                "exec1",
                "key1",
                ExecutionStatus.QUEUED,
                0L,
                0,
                0,
                null,
                0L,
                0L,
                null,
                "input",
                null,
                null,
                null,
                1L,
                1L,
                99999999L
            );
        }
    }
}