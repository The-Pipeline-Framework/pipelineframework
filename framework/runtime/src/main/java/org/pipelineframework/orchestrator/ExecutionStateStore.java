package org.pipelineframework.orchestrator;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.orchestrator.stream.StreamRegionRecord;

/**
 * SPI for async execution state persistence.
 */
public interface ExecutionStateStore {

    /**
     * Creates or returns a bounded execution-owned stream-region projection.
     *
     * <p>This is deliberately part of the active execution projection surface rather than an await
     * aggregate or a separate scheduler store.
     */
    default Uni<Optional<StreamRegionRecord>> createStreamRegion(StreamRegionRecord region) {
        return Uni.createFrom().failure(new UnsupportedOperationException(
            "Execution state store does not support stream-region projections"));
    }

    /**
     * Atomically transfers a claimed producer execution into a producer-owned stream region.
     *
     * <p>The parent {@code WAITING_EXTERNAL} state is a generic durable-boundary suspension here:
     * it does <em>not</em> assert that an Await unit has dispatched or even materialised all of
     * its interactions. The newly created region may correctly own zero interactions.
     */
    default Uni<Optional<StreamRegionRecord>> activateStreamRegion(
        StreamRegionRecord region,
        long expectedExecutionVersion,
        String transitionKey,
        String awaitUnitId,
        int awaitStepIndex,
        long nowEpochMs
    ) {
        return Uni.createFrom().failure(new UnsupportedOperationException(
            "Execution state store does not support atomic stream-region activation"));
    }

    /** Reads one execution-owned stream region. */
    default Uni<Optional<StreamRegionRecord>> getStreamRegion(String tenantId, String executionId, String regionId) {
        return Uni.createFrom().failure(new UnsupportedOperationException(
            "Execution state store does not support stream-region projections"));
    }

    /** Claims a due stream region independently of its waiting parent execution. */
    default Uni<Optional<StreamRegionRecord>> claimStreamRegion(
        String tenantId, String executionId, String regionId, String leaseOwner, long nowEpochMs, long leaseMs) {
        return Uni.createFrom().failure(new UnsupportedOperationException(
            "Execution state store does not support stream-region projections"));
    }

    /** Applies one bounded materialisation page with optimistic concurrency. */
    default Uni<Optional<StreamRegionRecord>> recordStreamRegionPage(
        String tenantId,
        String executionId,
        String regionId,
        long expectedVersion,
        org.pipelineframework.stream.OpaqueSourceCheckpoint nextCheckpoint,
        int itemCount,
        boolean endOfSource,
        long nowEpochMs) {
        return Uni.createFrom().failure(new UnsupportedOperationException(
            "Execution state store does not support stream-region projections"));
    }

    /** Releases one bounded credit after a scalar continuation commits. */
    default Uni<Optional<StreamRegionRecord>> releaseStreamRegionCredit(
        String tenantId, String executionId, String regionId, long expectedVersion, long nowEpochMs) {
        return Uni.createFrom().failure(new UnsupportedOperationException(
            "Execution state store does not support stream-region projections"));
    }

    /**
     * Returns bounded producer regions whose durable due time or expired lease makes them eligible
     * for another ordinary coordinator work delivery.
     */
    default Uni<List<StreamRegionRecord>> findDueStreamRegions(long nowEpochMs, int limit) {
        return Uni.createFrom().failure(new UnsupportedOperationException(
            "Execution state store does not support stream-region projections"));
    }

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
     * Fetches executions by idempotency execution key in the input order.
     *
     * <p>Stores with a native batch-read operation should override this method. The default is
     * deliberately sequential so an otherwise unsupported batch read cannot fan out an
     * unbounded number of remote requests.</p>
     *
     * @param tenantId tenant identifier
     * @param executionKeys execution keys to resolve
     * @return one optional execution per requested key, in the same order
     */
    default Uni<List<Optional<ExecutionRecord<Object, Object>>>> getExecutionsByKey(
        String tenantId,
        List<String> executionKeys
    ) {
        List<String> requestedKeys = List.copyOf(executionKeys);
        if (requestedKeys.isEmpty()) {
            return Uni.createFrom().item(List.of());
        }
        return Multi.createFrom().iterable(requestedKeys)
            .onItem().transformToUniAndConcatenate(executionKey -> getExecutionByKey(tenantId, executionKey))
            .collect().asList();
    }

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
     * Defers an execution whose dependency invocation was denied before it started.
     *
     * <p>This transition deliberately preserves {@code attempt}; circuit deferrals are bounded by
     * their own durable lifetime policy rather than being presented as failed remote attempts.</p>
     */
    default Uni<Optional<ExecutionRecord<Object, Object>>> deferCircuit(
        String tenantId,
        String executionId,
        long expectedVersion,
        long nextDueEpochMs,
        String transitionKey,
        String circuitIdentity,
        String reason,
        String errorMessage,
        long firstCircuitDeferredAtEpochMs,
        int circuitDeferralCount,
        long nowEpochMs) {
        return Uni.createFrom().completionStage(CompletableFuture.failedFuture(
            new UnsupportedOperationException("Execution state store does not support circuit deferral")));
    }

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
