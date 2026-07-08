package org.pipelineframework.orchestrator.controlplane;

import java.util.Objects;
import java.util.Set;

public record BoundaryUnit(
    String tenantId,
    String runId,
    String unitId,
    BoundaryKind kind,
    BoundaryUnitStatus status,
    String segmentId,
    String attemptId,
    int stepIndex,
    Integer expectedItemCount,
    int completedItemCount,
    Set<String> completedInteractionKeys,
    boolean dispatchComplete,
    long createdAtEpochMs,
    long updatedAtEpochMs
) {
    public BoundaryUnit {
        tenantId = ControlPlaneChecks.requireText(tenantId, "tenantId");
        runId = ControlPlaneChecks.requireText(runId, "runId");
        unitId = ControlPlaneChecks.requireText(unitId, "unitId");
        Objects.requireNonNull(kind, "kind must not be null");
        Objects.requireNonNull(status, "status must not be null");
        segmentId = ControlPlaneChecks.requireText(segmentId, "segmentId");
        attemptId = ControlPlaneChecks.requireText(attemptId, "attemptId");
        ControlPlaneChecks.requireNonNegative(stepIndex, "stepIndex");
        if (expectedItemCount != null && expectedItemCount < 0) {
            throw new IllegalArgumentException("expectedItemCount must not be negative");
        }
        ControlPlaneChecks.requireNonNegative(completedItemCount, "completedItemCount");
        completedInteractionKeys = ControlPlaneChecks.copySet(completedInteractionKeys);
        if (completedItemCount != completedInteractionKeys.size()) {
            throw new IllegalArgumentException("completedItemCount must match completedInteractionKeys size");
        }
        ControlPlaneChecks.requireNonNegative(createdAtEpochMs, "createdAtEpochMs");
        ControlPlaneChecks.requireNonNegative(updatedAtEpochMs, "updatedAtEpochMs");
    }

    BoundaryUnit withDispatchComplete(int expectedCount, long nowEpochMs) {
        if (status.terminal()) {
            return this;
        }
        BoundaryUnitStatus nextStatus = completedInteractionKeys.size() >= expectedCount
            ? BoundaryUnitStatus.COMPLETED
            : BoundaryUnitStatus.DISPATCH_COMPLETE;
        return new BoundaryUnit(
            tenantId,
            runId,
            unitId,
            kind,
            nextStatus,
            segmentId,
            attemptId,
            stepIndex,
            expectedCount,
            completedInteractionKeys.size(),
            completedInteractionKeys,
            true,
            createdAtEpochMs,
            nowEpochMs);
    }

    BoundaryUnit withCompletion(String completionKey, long nowEpochMs) {
        if (status.terminal()) {
            return this;
        }
        Set<String> nextKeys = new java.util.HashSet<>(completedInteractionKeys);
        nextKeys.add(ControlPlaneChecks.requireText(completionKey, "completionKey"));
        int expected = expectedItemCount == null ? -1 : expectedItemCount;
        BoundaryUnitStatus nextStatus = dispatchComplete && expected >= 0 && nextKeys.size() >= expected
            ? BoundaryUnitStatus.COMPLETED
            : status;
        return new BoundaryUnit(
            tenantId,
            runId,
            unitId,
            kind,
            nextStatus,
            segmentId,
            attemptId,
            stepIndex,
            expectedItemCount,
            nextKeys.size(),
            nextKeys,
            dispatchComplete,
            createdAtEpochMs,
            nowEpochMs);
    }

    BoundaryUnit timedOut(long nowEpochMs) {
        return terminal(BoundaryUnitStatus.TIMED_OUT, nowEpochMs);
    }

    BoundaryUnit failed(long nowEpochMs) {
        return terminal(BoundaryUnitStatus.FAILED, nowEpochMs);
    }

    private BoundaryUnit terminal(BoundaryUnitStatus nextStatus, long nowEpochMs) {
        return new BoundaryUnit(
            tenantId,
            runId,
            unitId,
            kind,
            nextStatus,
            segmentId,
            attemptId,
            stepIndex,
            expectedItemCount,
            completedItemCount,
            completedInteractionKeys,
            dispatchComplete,
            createdAtEpochMs,
            nowEpochMs);
    }
}
