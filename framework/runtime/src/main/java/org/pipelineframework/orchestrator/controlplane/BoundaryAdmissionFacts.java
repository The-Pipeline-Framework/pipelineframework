package org.pipelineframework.orchestrator.controlplane;

public final class BoundaryAdmissionFacts {

    private BoundaryAdmissionFacts() {
    }

    public static ControlPlaneFact.BoundaryCompletionAdmitted completion(BoundaryAdmissionRequest request) {
        return new ControlPlaneFact.BoundaryCompletionAdmitted(
            request.tenantId(),
            request.runId(),
            request.boundaryUnitId(),
            request.boundaryKind(),
            request.interactionId(),
            request.idempotencyKey(),
            request.responsePayload());
    }
}
