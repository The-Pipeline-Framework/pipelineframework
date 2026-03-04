package org.pipelineframework.orchestrator;

import java.util.List;
import java.util.Optional;

import io.smallrye.mutiny.Uni;

/**
 * SPI for async execution state persistence.
 */
public interface ExecutionStateStore {

    /**
     * Name of the provider used to select an implementation via configuration.
     *
     * @return the provider name; default is "memory"
     */
    default String providerName() {
        return "memory";
    }

    /**
     * Provider priority used when multiple stores are available.
     *
     * @return the provider priority; higher values are preferred when selecting among multiple stores
     */
    default int priority() {
        return 0;
    }

    /**
 * Create a new execution or return an existing execution with the same execution key.
 *
 * @param command creation parameters that include the tenant, execution key, and initial payload/metadata
 * @return a CreateExecutionResult indicating whether a new execution was created or an existing one was returned, along with the execution record
 */
    Uni<CreateExecutionResult> createOrGetExecution(ExecutionCreateCommand command);

    /**
 * Retrieves the execution record for the given tenant and execution id.
 *
 * @return an Optional containing the execution record if found, empty otherwise
 */
    Uni<Optional<ExecutionRecord>> getExecution(String tenantId, String executionId);

    /**
         * Claim the lease for an execution and mark the execution RUNNING.
         *
         * @param tenantId   tenant identifier
         * @param executionId execution identifier
         * @param leaseOwner worker identifier claiming the lease
         * @param nowEpochMs current timestamp in epoch milliseconds used to evaluate/record the lease
         * @param leaseMs    lease duration in milliseconds
         * @return an Optional containing the claimed ExecutionRecord with an incremented version if the claim succeeds, empty otherwise
         */
    Uni<Optional<ExecutionRecord>> claimLease(
        String tenantId,
        String executionId,
        String leaseOwner,
        long nowEpochMs,
        long leaseMs);

    /**
         * Marks the execution as succeeded when the current record version matches expectedVersion.
         *
         * @param tenantId tenant identifier
         * @param executionId execution identifier
         * @param expectedVersion expected record version for the conditional update
         * @param transitionKey idempotency key for this state transition
         * @param resultPayload final result payload to attach to the execution
         * @param nowEpochMs current timestamp in epoch milliseconds
         * @return an Optional containing the updated ExecutionRecord if the write succeeds, empty otherwise
         */
    Uni<Optional<ExecutionRecord>> markSucceeded(
        String tenantId,
        String executionId,
        long expectedVersion,
        String transitionKey,
        Object resultPayload,
        long nowEpochMs);

    /**
             * Schedule a retry for the specified execution when the stored version equals the provided expected version.
             *
             * @param tenantId     tenant identifier
             * @param executionId  execution identifier
             * @param expectedVersion expected record version used for optimistic concurrency
             * @param nextAttempt  next attempt number to record
             * @param nextDueEpochMs epoch millisecond timestamp when the next attempt becomes due
             * @param transitionKey idempotency key for this state transition
             * @param errorCode    error code describing the failure that triggered the retry
             * @param errorMessage human-readable error message for the retry record
             * @param nowEpochMs   current epoch millisecond timestamp used for timestamps
             * @return             an Optional containing the updated ExecutionRecord if the write succeeds and the version matched, or an empty Optional otherwise
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
         * Mark the execution as a terminal failure (FAILED or DLQ) when the stored version matches the expected version.
         *
         * @param tenantId      tenant identifier
         * @param executionId   execution identifier
         * @param expectedVersion expected record version for optimistic concurrency
         * @param finalStatus   terminal status to set: FAILED or DLQ
         * @param transitionKey idempotency key for this state transition
         * @param errorCode     error code describing the failure
         * @param errorMessage  human-readable error message
         * @param nowEpochMs    current timestamp in epoch milliseconds
         * @return              an Optional containing the updated ExecutionRecord if the version matched and the update succeeded, `Optional.empty()` otherwise
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
 * Finds executions that are scheduled to be dispatched at or before the given timestamp.
 *
 * @param nowEpochMs epoch milliseconds cutoff; executions with due time less than or equal to this value are eligible
 * @param limit maximum number of records to return
 * @return a list of ExecutionRecord objects due for dispatch, containing at most {@code limit} entries
 */
    Uni<List<ExecutionRecord>> findDueExecutions(long nowEpochMs, int limit);
}
