package org.pipelineframework.orchestrator.controlplane;

import java.util.Objects;

public record BoundaryInteraction(
    String tenantId,
    String runId,
    String unitId,
    String interactionId,
    BoundaryKind kind,
    BoundaryInteractionStatus status,
    String correlationId,
    String idempotencyKey,
    Integer itemIndex,
    Object requestPayload,
    Object responsePayload,
    String transportType,
    long deadlineEpochMs,
    long createdAtEpochMs,
    long updatedAtEpochMs
) {
    public BoundaryInteraction {
        tenantId = ControlPlaneChecks.requireText(tenantId, "tenantId");
        runId = ControlPlaneChecks.requireText(runId, "runId");
        unitId = ControlPlaneChecks.requireText(unitId, "unitId");
        interactionId = ControlPlaneChecks.requireText(interactionId, "interactionId");
        Objects.requireNonNull(kind, "kind must not be null");
        Objects.requireNonNull(status, "status must not be null");
        correlationId = ControlPlaneChecks.requireText(correlationId, "correlationId");
        idempotencyKey = ControlPlaneChecks.requireText(idempotencyKey, "idempotencyKey");
        if (itemIndex != null && itemIndex < 0) {
            throw new IllegalArgumentException("itemIndex must not be negative");
        }
        requestPayload = ControlPlaneChecks.freezePayload(requestPayload);
        responsePayload = ControlPlaneChecks.freezePayload(responsePayload);
        transportType = ControlPlaneChecks.requireText(transportType, "transportType");
        ControlPlaneChecks.requireNonNegative(deadlineEpochMs, "deadlineEpochMs");
        ControlPlaneChecks.requireNonNegative(createdAtEpochMs, "createdAtEpochMs");
        ControlPlaneChecks.requireNonNegative(updatedAtEpochMs, "updatedAtEpochMs");
    }

    BoundaryInteraction completed(Object nextResponsePayload, long nowEpochMs) {
        return terminal(BoundaryInteractionStatus.COMPLETED, nextResponsePayload, nowEpochMs);
    }

    BoundaryInteraction timedOut(long nowEpochMs) {
        return terminal(BoundaryInteractionStatus.TIMED_OUT, responsePayload, nowEpochMs);
    }

    BoundaryInteraction failed(long nowEpochMs) {
        return terminal(BoundaryInteractionStatus.FAILED, responsePayload, nowEpochMs);
    }

    private BoundaryInteraction terminal(
        BoundaryInteractionStatus nextStatus,
        Object nextResponsePayload,
        long nowEpochMs
    ) {
        return new BoundaryInteraction(
            tenantId,
            runId,
            unitId,
            interactionId,
            kind,
            nextStatus,
            correlationId,
            idempotencyKey,
            itemIndex,
            requestPayload,
            nextResponsePayload,
            transportType,
            deadlineEpochMs,
            createdAtEpochMs,
            nowEpochMs);
    }
}
