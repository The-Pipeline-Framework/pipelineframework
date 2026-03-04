package org.pipelineframework.orchestrator;

import java.util.List;
import java.util.Optional;
import jakarta.enterprise.context.ApplicationScoped;

import io.smallrye.mutiny.Uni;

/**
 * Placeholder provider for DynamoDB-backed execution state.
 *
 * <p>The core runtime ships the SPI and an in-memory provider. A production DynamoDB
 * provider is expected to be supplied by deployment-specific modules.</p>
 */
@ApplicationScoped
public class DynamoExecutionStateStore implements ExecutionStateStore {

    private static final String ERROR =
        "DynamoExecutionStateStore is selected but not implemented in core runtime. " +
            "Provide a deployment-specific provider module.";

    /**
     * Identifies this execution state store implementation as the DynamoDB-backed provider.
     *
     * @return the provider name "dynamo"
     */
    @Override
    public String providerName() {
        return "dynamo";
    }

    /**
     * Specifies the provider selection priority for this ExecutionStateStore implementation.
     *
     * @return the selection priority; lower numbers denote lower priority (returns -1000)
     */
    @Override
    public int priority() {
        return -1000;
    }

    /**
     * Create or retrieve an execution record for the given command; placeholder that signals
     * a DynamoDB-backed provider must be supplied by the deployment.
     *
     * @param command creation parameters for the execution
     * @return a failure containing an {@link UnsupportedOperationException} with the provider-not-implemented error message
     */
    @Override
    public Uni<CreateExecutionResult> createOrGetExecution(ExecutionCreateCommand command) {
        return Uni.createFrom().failure(new UnsupportedOperationException(ERROR));
    }

    /**
     * Look up an execution record for the given tenant and execution id.
     *
     * @return an Optional containing the execution record if found, or empty if not
     */
    @Override
    public Uni<Optional<ExecutionRecord>> getExecution(String tenantId, String executionId) {
        return Uni.createFrom().failure(new UnsupportedOperationException(ERROR));
    }

    /**
     * Attempts to claim a lease for the specified execution.
     *
     * @param tenantId     the tenant identifier owning the execution
     * @param executionId  the execution identifier to claim
     * @param leaseOwner   the identifier of the entity requesting the lease
     * @param nowEpochMs   the current time in epoch milliseconds used to evaluate and set lease timestamps
     * @param leaseMs      the requested lease duration in milliseconds
     * @return             an Optional containing the updated ExecutionRecord if the lease was successfully claimed, or an empty Optional if the lease could not be acquired
     * @throws UnsupportedOperationException if the DynamoDB-backed provider is not supplied by the deployment (this placeholder implementation always fails)
     */
    @Override
    public Uni<Optional<ExecutionRecord>> claimLease(
        String tenantId,
        String executionId,
        String leaseOwner,
        long nowEpochMs,
        long leaseMs) {
        return Uni.createFrom().failure(new UnsupportedOperationException(ERROR));
    }

    /**
     * Marks the specified execution as succeeded and persists the provided result and transition metadata.
     *
     * @param tenantId the tenant identifier owning the execution
     * @param executionId the execution's unique identifier
     * @param expectedVersion the expected current version of the execution record for conditional update
     * @param transitionKey an identifier for the transition that produced the success
     * @param resultPayload the result payload to attach to the execution record
     * @param nowEpochMs the current time in milliseconds since the epoch used as the update timestamp
     * @return an Optional containing the updated ExecutionRecord if the state was updated, empty otherwise
     */
    @Override
    public Uni<Optional<ExecutionRecord>> markSucceeded(
        String tenantId,
        String executionId,
        long expectedVersion,
        String transitionKey,
        Object resultPayload,
        long nowEpochMs) {
        return Uni.createFrom().failure(new UnsupportedOperationException(ERROR));
    }

    /**
     * Schedule a retry for an existing execution, updating its next attempt, due time, and recorded error.
     *
     * @param tenantId        the tenant identifier owning the execution
     * @param executionId     the execution identifier to update
     * @param expectedVersion the expected current version of the execution for optimistic concurrency checks
     * @param nextAttempt     the attempt number to schedule next
     * @param nextDueEpochMs  the epoch milliseconds when the next attempt should become due
     * @param transitionKey   the transition key associated with the retry
     * @param errorCode       an error code describing the failure that caused the retry
     * @param errorMessage    a human-readable message describing the failure
     * @param nowEpochMs      the current epoch milliseconds used to timestamp the update
     * @return an Optional containing the updated ExecutionRecord if the execution was updated, otherwise an empty Optional
     */
    @Override
    public Uni<Optional<ExecutionRecord>> scheduleRetry(
        String tenantId,
        String executionId,
        long expectedVersion,
        int nextAttempt,
        long nextDueEpochMs,
        String transitionKey,
        String errorCode,
        String errorMessage,
        long nowEpochMs) {
        return Uni.createFrom().failure(new UnsupportedOperationException(ERROR));
    }

    /**
     * Mark the specified execution as failed with a terminal status.
     *
     * @param tenantId the tenant identifier owning the execution
     * @param executionId the identifier of the execution to update
     * @param expectedVersion the expected current version for optimistic concurrency checks
     * @param finalStatus the terminal execution status to set
     * @param transitionKey the transition key that triggered the terminal failure
     * @param errorCode an optional machine-readable error code describing the failure
     * @param errorMessage an optional human-readable error message describing the failure
     * @param nowEpochMs the current time in epoch milliseconds to record as the update time
     * @return an `Optional` containing the updated `ExecutionRecord` if the state was updated, `Optional.empty()` otherwise
     * @throws UnsupportedOperationException if the DynamoDB-backed provider is not supplied by the deployment
     */
    @Override
    public Uni<Optional<ExecutionRecord>> markTerminalFailure(
        String tenantId,
        String executionId,
        long expectedVersion,
        ExecutionStatus finalStatus,
        String transitionKey,
        String errorCode,
        String errorMessage,
        long nowEpochMs) {
        return Uni.createFrom().failure(new UnsupportedOperationException(ERROR));
    }

    /**
     * Finds executions that are due to run at or before the specified epoch millisecond.
     *
     * @param nowEpochMs epoch milliseconds cutoff; executions with due time less than or equal to this value are considered due
     * @param limit maximum number of executions to return
     * @return a list of due ExecutionRecord objects, up to the specified limit; empty list if none are due
     * @throws UnsupportedOperationException always thrown in this placeholder implementation indicating a deployment-specific DynamoDB provider must be supplied
     */
    @Override
    public Uni<List<ExecutionRecord>> findDueExecutions(long nowEpochMs, int limit) {
        return Uni.createFrom().failure(new UnsupportedOperationException(ERROR));
    }
}
