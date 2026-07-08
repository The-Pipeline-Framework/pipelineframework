package org.pipelineframework.orchestrator.controlplane;

public record DueSegment(
    String tenantId,
    String runId,
    String segmentId,
    int startStepIndex,
    int stopBeforeStepIndex,
    Object inputPayload
) {
    public DueSegment {
        tenantId = ControlPlaneChecks.requireText(tenantId, "tenantId");
        runId = ControlPlaneChecks.requireText(runId, "runId");
        segmentId = ControlPlaneChecks.requireText(segmentId, "segmentId");
        ControlPlaneChecks.requireNonNegative(startStepIndex, "startStepIndex");
        stopBeforeStepIndex = ControlPlaneChecks.requireSegmentStopAfterStart(startStepIndex, stopBeforeStepIndex);
        inputPayload = ControlPlaneChecks.freezePayload(inputPayload);
    }
}
