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
     * Fetches one execution by tenant and idempotency execution key.
     *
     * @param tenantId tenant identifier
     * @param executionKey execution key
     * @return execution record when available
     */
    Uni<Optional<ExecutionRecord<Object, Object>>> getExecutionByKey(String tenantId, String executionKey);

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
     * @param awaitUnitId durable await unit id that owns the suspended boundary
     * @param awaitStepIndex index of the await step that suspended execution
     * @param nowEpochMs transition timestamp
     * @return updated waiting execution when the transition wins optimistic concurrency, otherwise empty
     */
    Uni<Optional<ExecutionRecord<Object, Object>>> markWaitingExternal(
        String tenantId,
        String executionId,
        long expectedVersion,
        String transitionKey,
        String awaitUnitId,
        int awaitStepIndex,
        long nowEpochMs);

    /**
     * Stores a completed await payload and makes the execution due for continuation.
     *
     * <p>This method matches by {@link ExecutionStatus#WAITING_EXTERNAL} plus
     * {@code awaitUnitId}, not by expected version. Completion admission is idempotent and can race
     * with duplicate external callbacks, so stores should return empty when the execution is no longer
     * waiting for that await unit.</p>
     *
     * @param tenantId tenant identifier
     * @param executionId execution identifier
     * @param awaitUnitId durable await unit id used to match the waiting execution
     * @param nextStepIndex next pipeline step index to execute
     * @param nowEpochMs transition timestamp
     * @return updated queued execution when completion is accepted, otherwise empty
     */
    Uni<Optional<ExecutionRecord<Object, Object>>> markAwaitCompleted(
        String tenantId,
        String executionId,
        String awaitUnitId,
        int nextStepIndex,
        long nowEpochMs);

    /**
     * Replaces a waiting execution's input with itemized continuation output and queues the parent
     * at {@code nextStepIndex}.
     *
     * <p>{@code markAwaitItemContinuationsCompleted} is an idempotent release operation for
     * itemized await continuations. Implementations should match the execution by
     * {@link ExecutionStatus#WAITING_EXTERNAL} plus the provided {@code awaitUnitId}, not by an
     * expected version, because duplicate or racing completion checks may safely retry this
     * transition. When accepted, {@code inputPayload} becomes the replacement input for the
     * resumed aggregate step and {@code nowEpochMs} is the transition timestamp.</p>
     *
     * @param tenantId tenant identifier
     * @param executionId execution identifier
     * @param awaitUnitId durable await unit id used to match the waiting execution
     * @param nextStepIndex next pipeline step index to execute
     * @param inputPayload replacement input payload for the resumed step
     * @param nowEpochMs transition timestamp
     * @return updated queued execution when completion is accepted, otherwise {@link Optional#empty()};
     *         empty results are safe to retry or treat as an already-lost idempotency race
     */
    Uni<Optional<ExecutionRecord<Object, Object>>> markAwaitItemContinuationsCompleted(
        String tenantId,
        String executionId,
        String awaitUnitId,
        int nextStepIndex,
        Object inputPayload,
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
     * Re-queues a terminal execution for operator-controlled re-drive.
     *
     * @param tenantId tenant identifier
     * @param executionId execution identifier
     * @param expectedVersion expected record version
     * @param allowFailed whether FAILED executions can be re-driven in addition to DLQ
     * @param transitionKey operator re-drive transition marker
     * @param nowEpochMs current timestamp
     * @return updated queued execution when the transition wins optimistic concurrency
     */
    Uni<Optional<ExecutionRecord<Object, Object>>> redriveTerminalExecution(
        String tenantId,
        String executionId,
        long expectedVersion,
        boolean allowFailed,
        String transitionKey,
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
