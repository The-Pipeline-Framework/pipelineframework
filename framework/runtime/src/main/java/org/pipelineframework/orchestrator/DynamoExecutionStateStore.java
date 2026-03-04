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
     * Identifier for this ExecutionStateStore provider.
     *
     * @return the provider name "dynamo"
     */
    @Override
    public String providerName() {
        return "dynamo";
    }

    /**
     * Provides a low selection priority for the DynamoDB placeholder provider.
     *
     * @return `-1000` indicating a very low priority relative to other providers.
     */
    @Override
    public int priority() {
        return -1000;
    }

    /**
     * Placeholder implementation that indicates a DynamoDB-backed execution store is not provided by the core runtime.
     *
     * @param command the execution creation request parameters
     * @return a failed result signaling an `UnsupportedOperationException` with the provider-not-implemented message
     */
    @Override
    public Uni<CreateExecutionResult> createOrGetExecution(ExecutionCreateCommand command) {
        return Uni.createFrom().failure(new UnsupportedOperationException(ERROR));
    }

    /**
     * Retrieve the execution record for the given tenant and execution ID.
     *
     * @param tenantId    the tenant identifier owning the execution
     * @param executionId the execution identifier to look up
     * @return             an Optional containing the execution record if found, otherwise empty
     * @throws UnsupportedOperationException always thrown by this placeholder implementation indicating the DynamoDB-backed store is not provided in the core runtime
     */
    @Override
    public Uni<Optional<ExecutionRecord<Object, Object>>> getExecution(String tenantId, String executionId) {
        return Uni.createFrom().failure(new UnsupportedOperationException(ERROR));
    }

    /**
     * Attempts to claim a lease for the specified execution and, if successful, returns the execution record.
     *
     * @param tenantId    the tenant identifier owning the execution
     * @param executionId the execution identifier to claim a lease for
     * @param leaseOwner  the owner identifier requesting the lease
     * @param nowEpochMs  the current time in epoch milliseconds used to evaluate lease timing
     * @param leaseMs     the requested lease duration in milliseconds
     * @return an Optional containing the execution record if the lease was successfully claimed, empty otherwise
     * @throws UnsupportedOperationException if the DynamoDB provider is not provided by the deployment (placeholder implementation)
     */
    @Override
    public Uni<Optional<ExecutionRecord<Object, Object>>> claimLease(
        String tenantId,
        String executionId,
        String leaseOwner,
        long nowEpochMs,
        long leaseMs) {
        return Uni.createFrom().failure(new UnsupportedOperationException(ERROR));
    }

    /**
     * Mark an execution as succeeded and record its result payload and transition.
     *
     * @param tenantId        the tenant identifier owning the execution
     * @param executionId     the execution identifier to update
     * @param expectedVersion the expected current version of the execution for optimistic concurrency
     * @param transitionKey   the transition key to record for the success transition
     * @param resultPayload   the payload produced by the successful execution
     * @param nowEpochMs      the current time in epoch milliseconds used as the operation timestamp
     * @return an Optional containing the updated execution record if the update was applied (version matched), or an empty Optional otherwise
     * @throws UnsupportedOperationException indicating the DynamoDB-backed store is not implemented in the core runtime and must be provided by a deployment-specific module
     */
    @Override
    public Uni<Optional<ExecutionRecord<Object, Object>>> markSucceeded(
        String tenantId,
        String executionId,
        long expectedVersion,
        String transitionKey,
        Object resultPayload,
        long nowEpochMs) {
        return Uni.createFrom().failure(new UnsupportedOperationException(ERROR));
    }

    /**
     * Schedule a retry for the specified execution by advancing its attempt count and setting the next due time.
     *
     * @param tenantId        the tenant identifier for the execution
     * @param executionId     the execution identifier
     * @param expectedVersion the expected current version for optimistic concurrency control
     * @param nextAttempt     the attempt number to set for the next retry
     * @param nextDueEpochMs  epoch milliseconds when the execution should be retried next
     * @param transitionKey   the transition key associated with this retry
     * @param errorCode       a machine-readable code describing the error that caused the retry
     * @param errorMessage    a human-readable message describing the error that caused the retry
     * @param nowEpochMs      current epoch milliseconds used for timestamping and validation
     * @return an Optional containing the updated ExecutionRecord if the retry was scheduled; empty if the execution was not found or the expected version did not match
     */
    @Override
    public Uni<Optional<ExecutionRecord<Object, Object>>> scheduleRetry(
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
     * Mark an execution as failed permanently and record final failure details.
     *
     * This updates the execution to the provided terminal status, sets failure metadata,
     * and returns the updated execution record when the update operation succeeds and the
     * expected version matches the current version.
     *
     * @param tenantId       the tenant identifier owning the execution
     * @param executionId    the execution identifier to update
     * @param expectedVersion the expected current version used for optimistic concurrency control
     * @param finalStatus    the terminal execution status to set (e.g., FAILED)
     * @param transitionKey  the transition identifier associated with the failure
     * @param errorCode      an optional machine-readable error code describing the failure
     * @param errorMessage   a human-readable error message describing the failure
     * @param nowEpochMs     the current time in milliseconds since epoch to record as the update timestamp
     * @return an Optional containing the updated ExecutionRecord when the update succeeds; `Optional.empty()` if the execution was not found or the version did not match
     */
    @Override
    public Uni<Optional<ExecutionRecord<Object, Object>>> markTerminalFailure(
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
     * Finds executions due at or before the specified epoch millisecond timestamp.
     *
     * @param nowEpochMs the cutoff time in milliseconds since the Unix epoch used to identify due executions
     * @param limit the maximum number of execution records to return
     * @return a Uni that fails with an {@link UnsupportedOperationException} indicating the DynamoDB provider is not implemented in the core runtime
     */
    @Override
    public Uni<List<ExecutionRecord<Object, Object>>> findDueExecutions(long nowEpochMs, int limit) {
        return Uni.createFrom().failure(new UnsupportedOperationException(ERROR));
    }
}
