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
     * Higher numeric values have higher precedence and are selected over lower values.
     * The default {@link #priority()} implementation returns {@code 0}.
     *
     * @return provider priority
     */
    default int priority() {
        return 0;
    }

    /**
     * Validates provider readiness for queue-async orchestrator mode startup.
     *
     * <p>Return a non-empty value when the provider is selected but cannot safely operate
     * with the current runtime configuration.</p>
     *
     * @param config orchestrator configuration
     * @return optional startup validation error
     */
    default Optional<String> startupValidationError(PipelineOrchestratorConfig config) {
        return Optional.empty();
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
    Uni<Optional<ExecutionRecord<Object, Object>>> getExecution(String tenantId, String executionId);

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
    Uni<Optional<ExecutionRecord<Object, Object>>> claimLease(
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
    Uni<Optional<ExecutionRecord<Object, Object>>> markSucceeded(
        String tenantId,
        String executionId,
        long expectedVersion,
        String transitionKey,
        Object resultPayload,
        long nowEpochMs);

    /**
     * Marks an execution as durably waiting on an external await interaction.
     *
     * @param tenantId tenant identifier
     * @param executionId execution identifier
     * @param expectedVersion current execution record version for optimistic concurrency
     * @param transitionKey idempotency key for the suspend transition
     * @param awaitInteractionId external await interaction id used to correlate and deduplicate completions
     * @param awaitStepIndex index of the await step that suspended execution
     * @param nowEpochMs transition timestamp
     * @return updated waiting execution when the transition wins optimistic concurrency, otherwise empty
     */
    default Uni<Optional<ExecutionRecord<Object, Object>>> markWaitingExternal(
        String tenantId,
        String executionId,
        long expectedVersion,
        String transitionKey,
        String awaitInteractionId,
        int awaitStepIndex,
        long nowEpochMs) {
        return Uni.createFrom().failure(new UnsupportedOperationException("markWaitingExternal is not implemented"));
    }

    /**
     * Stores a completed await payload and makes the execution due for continuation.
     *
     * <p>This method matches by {@link ExecutionStatus#WAITING_EXTERNAL} plus
     * {@code awaitInteractionId}, not by expected version. Completion admission is idempotent and can race
     * with duplicate external callbacks, so stores should return empty when the execution is no longer
     * waiting for that interaction.</p>
     *
     * @param tenantId tenant identifier
     * @param executionId execution identifier
     * @param awaitInteractionId external await interaction id used to match the waiting execution
     * @param resumePayload payload used as input for the step after the await boundary
     * @param nextStepIndex next pipeline step index to execute
     * @param nowEpochMs transition timestamp
     * @return updated queued execution when completion is accepted, otherwise empty
     */
    default Uni<Optional<ExecutionRecord<Object, Object>>> markAwaitCompleted(
        String tenantId,
        String executionId,
        String awaitInteractionId,
        Object resumePayload,
        int nextStepIndex,
        long nowEpochMs) {
        return Uni.createFrom().failure(new UnsupportedOperationException("markAwaitCompleted is not implemented"));
    }

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
     * Finds executions due for dispatch.
     *
     * @param nowEpochMs current timestamp
     * @param limit max records to return
     * @return due executions
     */
    Uni<List<ExecutionRecord<Object, Object>>> findDueExecutions(long nowEpochMs, int limit);
}
