package org.pipelineframework.orchestrator;

import java.util.Objects;
import java.util.Optional;

/**
 * Decoded command passed to the in-process transition worker.
 *
 * @param tenantId tenant identifier
 * @param executionId execution identifier
 * @param currentStepIndex step index where execution should continue
 * @param stopBeforeStepIndex optional exclusive stop step index; negative means run to pipeline end
 * @param attempt current execution attempt
 * @param resultShape expected materialized result shape
 * @param executionVersion claimed execution record version
 * @param transitionKey idempotency key for the claimed transition
 * @param inputPayload materialized input payload for this transition
 * @param redriveIntent explicit terminal-redrive intent
 * @param redriveStepIndex failed Command step targeted by deliberate retry, or {@code -1}
 * @param redriveCommandId exact logical Command effect targeted by deliberate retry
 */
public record TransitionWorkerCommand(
    String tenantId,
    String executionId,
    int currentStepIndex,
    int stopBeforeStepIndex,
    int attempt,
    ExecutionResultShape resultShape,
    long executionVersion,
    String transitionKey,
    Object inputPayload,
    ExecutionRedriveIntent redriveIntent,
    int redriveStepIndex,
    Optional<String> redriveCommandId
) {
    public TransitionWorkerCommand(
        String tenantId,
        String executionId,
        int currentStepIndex,
        int stopBeforeStepIndex,
        int attempt,
        ExecutionResultShape resultShape,
        long executionVersion,
        String transitionKey,
        Object inputPayload
    ) {
        this(tenantId, executionId, currentStepIndex, stopBeforeStepIndex, attempt, resultShape,
            executionVersion, transitionKey, inputPayload, ExecutionRedriveIntent.REPLAY, -1, Optional.empty());
    }

    public TransitionWorkerCommand(
        String tenantId,
        String executionId,
        int currentStepIndex,
        int attempt,
        ExecutionResultShape resultShape,
        long executionVersion,
        String transitionKey,
        Object inputPayload
    ) {
        this(
            tenantId,
            executionId,
            currentStepIndex,
            -1,
            attempt,
            resultShape,
            executionVersion,
            transitionKey,
            inputPayload,
            ExecutionRedriveIntent.REPLAY,
            -1,
            Optional.empty());
    }

    public TransitionWorkerCommand(
        String tenantId,
        String executionId,
        int currentStepIndex,
        int attempt,
        ExecutionResultShape resultShape,
        long executionVersion,
        String transitionKey,
        Object inputPayload,
        ExecutionRedriveIntent redriveIntent,
        Optional<String> redriveCommandId
    ) {
        this(
            tenantId,
            executionId,
            currentStepIndex,
            -1,
            attempt,
            resultShape,
            executionVersion,
            transitionKey,
            inputPayload,
            redriveIntent,
            redriveIntent == ExecutionRedriveIntent.RETRY_FAILED_COMMAND ? currentStepIndex : -1,
            redriveCommandId);
    }

    public TransitionWorkerCommand {
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(executionId, "executionId");
        if (currentStepIndex < 0) {
            throw new IllegalArgumentException("currentStepIndex must be >= 0");
        }
        if (stopBeforeStepIndex >= 0 && stopBeforeStepIndex < currentStepIndex) {
            throw new IllegalArgumentException("stopBeforeStepIndex must be greater than or equal to currentStepIndex when set");
        }
        if (attempt < 0) {
            throw new IllegalArgumentException("attempt must be >= 0");
        }
        Objects.requireNonNull(resultShape, "resultShape");
        if (executionVersion < 0) {
            throw new IllegalArgumentException("executionVersion must be >= 0");
        }
        if (transitionKey == null || transitionKey.isBlank()) {
            throw new IllegalArgumentException("transitionKey must not be blank");
        }
        redriveIntent = redriveIntent == null ? ExecutionRedriveIntent.REPLAY : redriveIntent;
        redriveCommandId = Optional.ofNullable(redriveCommandId).orElseGet(Optional::empty);
        if (redriveIntent == ExecutionRedriveIntent.RETRY_FAILED_COMMAND && redriveStepIndex < currentStepIndex) {
            throw new IllegalArgumentException(
                "redriveStepIndex must identify a step at or after currentStepIndex for deliberate Command retry");
        }
        if (redriveIntent == ExecutionRedriveIntent.RETRY_FAILED_COMMAND
            && redriveCommandId.filter(value -> !value.isBlank()).isEmpty()) {
            throw new IllegalArgumentException(
                "redriveCommandId must identify the exact logical effect for deliberate Command retry");
        }
        if (redriveIntent == ExecutionRedriveIntent.REPLAY) {
            redriveStepIndex = -1;
            redriveCommandId = Optional.empty();
        }
    }
}
