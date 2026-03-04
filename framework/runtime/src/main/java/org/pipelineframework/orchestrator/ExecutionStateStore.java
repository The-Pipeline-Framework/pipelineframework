package org.pipelineframework.orchestrator;

import java.util.List;
import java.util.Optional;

import io.smallrye.mutiny.Uni;

/**
 * SPI for async execution state persistence.
 */
public interface ExecutionStateStore {

    /**
     * Provider name used for configuration-based selection.
     *
     * @return provider name
     */
    default String providerName() {
        return "memory";
    }

    /**
     * Provider priority used when multiple stores are available.
     *
     * @return provider priority
     */
    default int priority() {
        return 0;
    }

    /**
     * Creates a new execution or returns an existing one for the same execution key.
     *
     * @param command create command
     * @return create-or-get result
     */
    Uni<CreateExecutionResult> createOrGetExecution(ExecutionCreateCommand command);

    /**
     * Fetches one execution by tenant and execution id.
     *
     * @param tenantId tenant identifier
     * @param executionId execution identifier
     * @return execution record when available
     */
    Uni<Optional<ExecutionRecord>> getExecution(String tenantId, String executionId);

    /**
     * Claims the lease and marks execution RUNNING.
     *
     * @param tenantId tenant identifier
     * @param executionId execution identifier
     * @param leaseOwner worker identifier
     * @param nowEpochMs current timestamp
     * @param leaseMs lease duration in ms
     * @return claimed execution with incremented version when claim succeeds
     */
    Uni<Optional<ExecutionRecord>> claimLease(
        String tenantId,
        String executionId,
        String leaseOwner,
        long nowEpochMs,
        long leaseMs);

    /**
     * Marks an execution as succeeded if expected version matches.
     *
     * @param tenantId tenant identifier
     * @param executionId execution identifier
     * @param expectedVersion expected record version
     * @param transitionKey transition idempotency key
     * @param resultPayload final payload
     * @param nowEpochMs current timestamp
     * @return updated execution when write succeeds
     */
    Uni<Optional<ExecutionRecord>> markSucceeded(
        String tenantId,
        String executionId,
        long expectedVersion,
        String transitionKey,
        Object resultPayload,
        long nowEpochMs);

    /**
     * Schedules a retry if expected version matches.
     *
     * @param tenantId tenant identifier
     * @param executionId execution identifier
     * @param expectedVersion expected record version
     * @param nextAttempt next attempt number
     * @param nextDueEpochMs next due timestamp
     * @param transitionKey transition idempotency key
     * @param errorCode error code
     * @param errorMessage error message
     * @param nowEpochMs current timestamp
     * @return updated execution when write succeeds
     */
    Uni<Optional<ExecutionRecord>> scheduleRetry(
        String tenantId,
        String executionId,
        long expectedVersion,
        int nextAttempt,
        long nextDueEpochMs,
        String transitionKey,
        String errorCode,
        String errorMessage,
        long nowEpochMs);

    /**
     * Marks an execution as failed or dead-lettered if expected version matches.
     *
     * @param tenantId tenant identifier
     * @param executionId execution identifier
     * @param expectedVersion expected record version
     * @param finalStatus terminal status FAILED or DLQ
     * @param transitionKey transition idempotency key
     * @param errorCode error code
     * @param errorMessage error message
     * @param nowEpochMs current timestamp
     * @return updated execution when write succeeds
     */
    Uni<Optional<ExecutionRecord>> markTerminalFailure(
        String tenantId,
        String executionId,
        long expectedVersion,
        ExecutionStatus finalStatus,
        String transitionKey,
        String errorCode,
        String errorMessage,
        long nowEpochMs);

    /**
     * Finds executions due for dispatch.
     *
     * @param nowEpochMs current timestamp
     * @param limit max records to return
     * @return due executions
     */
    Uni<List<ExecutionRecord>> findDueExecutions(long nowEpochMs, int limit);
}
