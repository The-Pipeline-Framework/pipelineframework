package org.pipelineframework.orchestrator;

import java.util.Objects;
import java.util.Optional;

/**
 * Result of an operator-controlled execution re-drive.
 */
public record ExecutionRedriveResult(
    String tenantId,
    String executionId,
    ExecutionStatus previousStatus,
    ExecutionStatus status,
    long version,
    int currentStepIndex,
    int attempt,
    String pipelineId,
    String contractVersion,
    String releaseVersion,
    long updatedAtEpochMs,
    ExecutionRedriveIntent intent,
    Optional<String> targetCommandId
) {
    public ExecutionRedriveResult(
        String tenantId,
        String executionId,
        ExecutionStatus previousStatus,
        ExecutionStatus status,
        long version,
        int currentStepIndex,
        int attempt,
        String pipelineId,
        String contractVersion,
        String releaseVersion,
        long updatedAtEpochMs
    ) {
        this(tenantId, executionId, previousStatus, status, version, currentStepIndex, attempt,
            pipelineId, contractVersion, releaseVersion, updatedAtEpochMs,
            ExecutionRedriveIntent.REPLAY, Optional.empty());
    }

    public ExecutionRedriveResult {
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(executionId, "executionId");
        Objects.requireNonNull(previousStatus, "previousStatus");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(contractVersion, "contractVersion");
        Objects.requireNonNull(releaseVersion, "releaseVersion");
        intent = intent == null ? ExecutionRedriveIntent.REPLAY : intent;
        targetCommandId = Optional.ofNullable(targetCommandId).orElseGet(Optional::empty);
    }

    public static ExecutionRedriveResult from(
        ExecutionRecord<Object, Object> previous,
        ExecutionRecord<Object, Object> redriven
    ) {
        Objects.requireNonNull(previous, "previous");
        Objects.requireNonNull(redriven, "redriven");
        return new ExecutionRedriveResult(
            redriven.tenantId(),
            redriven.executionId(),
            previous.status(),
            redriven.status(),
            redriven.version(),
            redriven.currentStepIndex(),
            redriven.attempt(),
            redriven.pipelineId(),
            redriven.contractVersion(),
            redriven.releaseVersion(),
            redriven.updatedAtEpochMs(),
            redriven.redriveIntent(),
            redriven.redriveTargetCommandId());
    }
}
