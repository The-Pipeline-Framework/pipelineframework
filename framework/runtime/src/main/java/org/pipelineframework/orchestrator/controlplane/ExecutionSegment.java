package org.pipelineframework.orchestrator.controlplane;

import java.util.List;
import java.util.Objects;

public record ExecutionSegment(
    String tenantId,
    String runId,
    String segmentId,
    int startStepIndex,
    int stopBeforeStepIndex,
    SegmentStatus status,
    Object inputPayload,
    List<?> outputItems,
    String boundaryUnitId,
    long nextDueEpochMs,
    long createdAtEpochMs,
    long updatedAtEpochMs
) {
    public ExecutionSegment {
        tenantId = ControlPlaneChecks.requireText(tenantId, "tenantId");
        runId = ControlPlaneChecks.requireText(runId, "runId");
        segmentId = ControlPlaneChecks.requireText(segmentId, "segmentId");
        ControlPlaneChecks.requireNonNegative(startStepIndex, "startStepIndex");
        stopBeforeStepIndex = ControlPlaneChecks.requireSegmentStopAfterStart(startStepIndex, stopBeforeStepIndex);
        Objects.requireNonNull(status, "status must not be null");
        inputPayload = ControlPlaneChecks.freezePayload(inputPayload);
        outputItems = ControlPlaneChecks.copyList(outputItems).stream()
            .map(ControlPlaneChecks::freezePayload)
            .toList();
        ControlPlaneChecks.requireNonNegative(nextDueEpochMs, "nextDueEpochMs");
        ControlPlaneChecks.requireNonNegative(createdAtEpochMs, "createdAtEpochMs");
        ControlPlaneChecks.requireNonNegative(updatedAtEpochMs, "updatedAtEpochMs");
    }

    ExecutionSegment running(long nowEpochMs) {
        return withStatus(SegmentStatus.RUNNING, null, outputItems, nextDueEpochMs, nowEpochMs);
    }

    ExecutionSegment completed(List<?> outputItems, long nowEpochMs) {
        return withStatus(SegmentStatus.COMPLETED, null, outputItems, nextDueEpochMs, nowEpochMs);
    }

    ExecutionSegment suspended(String boundaryUnitId, long nowEpochMs) {
        return withStatus(SegmentStatus.SUSPENDED, boundaryUnitId, outputItems, nextDueEpochMs, nowEpochMs);
    }

    ExecutionSegment failed(long nowEpochMs) {
        return withStatus(SegmentStatus.FAILED, boundaryUnitId, outputItems, nextDueEpochMs, nowEpochMs);
    }

    private ExecutionSegment withStatus(
        SegmentStatus nextStatus,
        String nextBoundaryUnitId,
        List<?> nextOutputItems,
        long nextDueEpochMs,
        long nowEpochMs
    ) {
        return new ExecutionSegment(
            tenantId,
            runId,
            segmentId,
            startStepIndex,
            stopBeforeStepIndex,
            nextStatus,
            inputPayload,
            nextOutputItems,
            nextBoundaryUnitId,
            nextDueEpochMs,
            createdAtEpochMs,
            nowEpochMs);
    }
}
