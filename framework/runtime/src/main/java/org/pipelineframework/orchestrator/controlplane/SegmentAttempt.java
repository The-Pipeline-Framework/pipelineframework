package org.pipelineframework.orchestrator.controlplane;

import java.util.Objects;

public record SegmentAttempt(
    String tenantId,
    String runId,
    String segmentId,
    String attemptId,
    int attemptNumber,
    SegmentAttemptStatus status,
    String failureCode,
    String failureMessage,
    long startedAtEpochMs,
    long updatedAtEpochMs
) {
    public SegmentAttempt {
        tenantId = ControlPlaneChecks.requireText(tenantId, "tenantId");
        runId = ControlPlaneChecks.requireText(runId, "runId");
        segmentId = ControlPlaneChecks.requireText(segmentId, "segmentId");
        attemptId = ControlPlaneChecks.requireText(attemptId, "attemptId");
        ControlPlaneChecks.requireNonNegative(attemptNumber, "attemptNumber");
        Objects.requireNonNull(status, "status must not be null");
        ControlPlaneChecks.requireNonNegative(startedAtEpochMs, "startedAtEpochMs");
        ControlPlaneChecks.requireNonNegative(updatedAtEpochMs, "updatedAtEpochMs");
    }

    SegmentAttempt withStatus(SegmentAttemptStatus nextStatus, long nowEpochMs) {
        return new SegmentAttempt(
            tenantId,
            runId,
            segmentId,
            attemptId,
            attemptNumber,
            nextStatus,
            failureCode,
            failureMessage,
            startedAtEpochMs,
            nowEpochMs);
    }

    SegmentAttempt failed(String failureCode, String failureMessage, long nowEpochMs) {
        failureCode = ControlPlaneChecks.requireText(failureCode, "failureCode");
        failureMessage = failureMessage == null ? "" : failureMessage;
        return new SegmentAttempt(
            tenantId,
            runId,
            segmentId,
            attemptId,
            attemptNumber,
            SegmentAttemptStatus.FAILED,
            failureCode,
            failureMessage,
            startedAtEpochMs,
            nowEpochMs);
    }
}
