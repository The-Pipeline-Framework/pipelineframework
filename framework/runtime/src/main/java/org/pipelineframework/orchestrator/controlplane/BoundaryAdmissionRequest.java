package org.pipelineframework.orchestrator.controlplane;

import java.util.Objects;

public record BoundaryAdmissionRequest(
    String tenantId,
    String runId,
    String boundaryUnitId,
    BoundaryKind boundaryKind,
    String interactionId,
    String idempotencyKey,
    Object responsePayload
) {
    public BoundaryAdmissionRequest {
        tenantId = ControlPlaneChecks.requireText(tenantId, "tenantId");
        runId = ControlPlaneChecks.requireText(runId, "runId");
        boundaryUnitId = ControlPlaneChecks.requireText(boundaryUnitId, "boundaryUnitId");
        Objects.requireNonNull(boundaryKind, "boundaryKind must not be null");
        interactionId = ControlPlaneChecks.requireText(interactionId, "interactionId");
        idempotencyKey = ControlPlaneChecks.requireText(idempotencyKey, "idempotencyKey");
    }
}
