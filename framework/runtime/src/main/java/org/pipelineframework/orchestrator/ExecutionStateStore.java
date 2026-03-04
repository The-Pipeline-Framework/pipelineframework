package org.pipelineframework.orchestrator;

import java.util.List;
import java.util.Optional;

import io.smallrye.mutiny.Uni;

/**
 * SPI for async execution state persistence.
 */
public interface ExecutionStateStore {

    /**
     * The provider identifier used to select an implementation via configuration.
     *
     * @return the provider name used for configuration-based selection
     */
    default String providerName() {
        return "memory";
    }

    /**
     * Priority value used to select between multiple available stores.
     *
     * Higher values indicate higher selection precedence.
     *
     * @return the provider priority value; higher numbers have greater precedence
     */
    default int priority() {
        return 0;
    }

    /**
 * Create a new execution or retrieve an existing execution that shares the same execution key.
 *
 * <p>If an execution with the provided key already exists, the existing execution is returned;
 * otherwise a new execution is created and returned.</p>
 *
 * @param command the creation parameters, including tenant, execution key, initial payload and scheduling metadata
 * @return a CreateExecutionResult indicating whether a new execution was created and containing the resulting execution record
 */
    Uni<CreateExecutionResult> createOrGetExecution(ExecutionCreateCommand command);

    /**
 * Fetches the execution record for the given tenant and execution id.
 *
 * @return an Optional containing the execution record if found, otherwise an empty Optional
 */
    Uni<Optional<ExecutionRecord<Object, Object>>> getExecution(String tenantId, String executionId);

    /**
         * Claims the execution lease and marks the execution RUNNING.
         *
         * @param tenantId    tenant identifier
         * @param executionId execution identifier
         * @param leaseOwner  worker identifier claiming the lease
         * @param nowEpochMs  current timestamp in milliseconds
         * @param leaseMs     lease duration in milliseconds
         * @return            an Optional containing the claimed ExecutionRecord with an incremented version if the claim succeeds, empty otherwise
         */
    Uni<Optional<ExecutionRecord<Object, Object>>> claimLease(
        String tenantId,
        String executionId,
        String leaseOwner,
        long nowEpochMs,
        long leaseMs);

    /**
         * Mark the execution as succeeded using the provided transition key for idempotency.
         *
         * @param expectedVersion the expected current version of the execution record; update occurs only if it matches
         * @param transitionKey   idempotency key to ensure the success transition is applied at most once
         * @param resultPayload   final payload to store with the succeeded execution
         * @param nowEpochMs      current epoch timestamp (milliseconds) used for setting timestamps on the record
         * @return                 an Optional containing the updated execution record if the version matched and the update was applied, empty otherwise
         */
    Uni<Optional<ExecutionRecord<Object, Object>>> markSucceeded(
        String tenantId,
        String executionId,
        long expectedVersion,
        String transitionKey,
        Object resultPayload,
        long nowEpochMs);

    /**
         * Schedules a retry for an execution when the provided version matches the current record.
         *
         * @param tenantId      tenant identifier
         * @param executionId   execution identifier
         * @param expectedVersion expected record version for optimistic concurrency control
         * @param nextAttempt   the attempt number to record for the next retry
         * @param nextDueEpochMs epoch milliseconds when the execution should next be retried
         * @param transitionKey idempotency key for the state transition
         * @param errorCode     error code that caused the retry
         * @param errorMessage  error message that caused the retry
         * @param nowEpochMs    current epoch milliseconds used for timestamps
         * @return              an Optional containing the updated execution record if the write succeeds, otherwise an empty Optional
         */
    Uni<Optional<ExecutionRecord<Object, Object>>> scheduleRetry(
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
         * Mark an execution as terminally failed or moved to the dead-letter queue when the record version matches the provided expected version.
         *
         * @param tenantId         tenant identifier
         * @param executionId      execution identifier
         * @param expectedVersion  expected record version; the update is applied only if the stored version equals this value
         * @param finalStatus      terminal status to set; typically FAILED or DLQ
         * @param transitionKey    idempotency key for the terminal transition to prevent duplicate side effects
         * @param errorCode        optional error code describing the failure
         * @param errorMessage     optional human-readable error message
         * @param nowEpochMs       current time in milliseconds since epoch used for timestamping the transition
         * @return an Optional containing the updated execution record if the terminal transition was applied, or Optional.empty() if the update did not occur
         */
    Uni<Optional<ExecutionRecord<Object, Object>>> markTerminalFailure(
        String tenantId,
        String executionId,
        long expectedVersion,
        ExecutionStatus finalStatus,
        String transitionKey,
        String errorCode,
        String errorMessage,
        long nowEpochMs);

    /**
 * Retrieve executions that are scheduled for dispatch at or before the given timestamp.
 *
 * @param nowEpochMs epoch milliseconds cutoff; include executions with next-due ≤ this value
 * @param limit maximum number of execution records to return
 * @return a list of execution records due as of the given timestamp, capped by {@code limit}
 */
    Uni<List<ExecutionRecord<Object, Object>>> findDueExecutions(long nowEpochMs, int limit);
}
