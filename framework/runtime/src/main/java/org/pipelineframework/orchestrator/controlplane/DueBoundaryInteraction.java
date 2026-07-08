package org.pipelineframework.orchestrator.controlplane;

public record DueBoundaryInteraction(
    String tenantId,
    String runId,
    String boundaryUnitId,
    String interactionId,
    BoundaryKind kind,
    long deadlineEpochMs
) {
    public DueBoundaryInteraction {
        tenantId = ControlPlaneChecks.requireText(tenantId, "tenantId");
        runId = ControlPlaneChecks.requireText(runId, "runId");
        boundaryUnitId = ControlPlaneChecks.requireText(boundaryUnitId, "boundaryUnitId");
        interactionId = ControlPlaneChecks.requireText(interactionId, "interactionId");
        java.util.Objects.requireNonNull(kind, "kind must not be null");
        ControlPlaneChecks.requireNonNegative(deadlineEpochMs, "deadlineEpochMs");
    }
}
