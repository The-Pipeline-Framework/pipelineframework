package org.pipelineframework.orchestrator;

import java.util.Objects;

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
    long updatedAtEpochMs
) {
    public ExecutionRedriveResult {
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(executionId, "executionId");
        Objects.requireNonNull(previousStatus, "previousStatus");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(contractVersion, "contractVersion");
        Objects.requireNonNull(releaseVersion, "releaseVersion");
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
            redriven.updatedAtEpochMs());
    }
}
