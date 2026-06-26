package org.pipelineframework.orchestrator.controlplane;

import java.util.List;
import java.util.Objects;
import org.pipelineframework.orchestrator.ExecutionResultShape;

public sealed interface ControlPlaneFact permits
    ControlPlaneFact.RunSubmitted,
    ControlPlaneFact.SegmentAttemptStarted,
    ControlPlaneFact.SegmentCompleted,
    ControlPlaneFact.SegmentSuspended,
    ControlPlaneFact.BoundaryInteractionDispatched,
    ControlPlaneFact.BoundaryDispatchCompleted,
    ControlPlaneFact.BoundaryCompletionAdmitted,
    ControlPlaneFact.InteractionTimedOut,
    ControlPlaneFact.ContinuationSegmentCreated,
    ControlPlaneFact.TerminalPublicationCompleted,
    ControlPlaneFact.RunSucceeded,
    ControlPlaneFact.RunFailed {

    String tenantId();

    String runId();

    String factKey();

    record RunSubmitted(
        String tenantId,
        String runId,
        String executionKey,
        String pipelineId,
        String contractVersion,
        String releaseVersion,
        ExecutionResultShape resultShape,
        String initialSegmentId,
        int startStepIndex,
        int stopBeforeStepIndex,
        Object inputPayload,
        long ttlEpochS
    ) implements ControlPlaneFact {
        public RunSubmitted {
            tenantId = ControlPlaneChecks.requireText(tenantId, "tenantId");
            runId = ControlPlaneChecks.requireText(runId, "runId");
            executionKey = ControlPlaneChecks.requireText(executionKey, "executionKey");
            pipelineId = ControlPlaneChecks.requireText(pipelineId, "pipelineId");
            contractVersion = ControlPlaneChecks.requireText(contractVersion, "contractVersion");
            releaseVersion = ControlPlaneChecks.requireText(releaseVersion, "releaseVersion");
            Objects.requireNonNull(resultShape, "resultShape must not be null");
            initialSegmentId = ControlPlaneChecks.requireText(initialSegmentId, "initialSegmentId");
            ControlPlaneChecks.requireNonNegative(startStepIndex, "startStepIndex");
            if (stopBeforeStepIndex >= 0 && stopBeforeStepIndex < startStepIndex) {
                throw new IllegalArgumentException("stopBeforeStepIndex must be greater than or equal to startStepIndex");
            }
            ControlPlaneChecks.requireNonNegative(ttlEpochS, "ttlEpochS");
        }

        @Override
        public String factKey() {
            return "run-submitted:" + tenantId + ":" + runId;
        }
    }

    record SegmentAttemptStarted(
        String tenantId,
        String runId,
        String segmentId,
        String attemptId,
        int attemptNumber
    ) implements ControlPlaneFact {
        public SegmentAttemptStarted {
            tenantId = ControlPlaneChecks.requireText(tenantId, "tenantId");
            runId = ControlPlaneChecks.requireText(runId, "runId");
            segmentId = ControlPlaneChecks.requireText(segmentId, "segmentId");
            attemptId = ControlPlaneChecks.requireText(attemptId, "attemptId");
            ControlPlaneChecks.requireNonNegative(attemptNumber, "attemptNumber");
        }

        @Override
        public String factKey() {
            return "segment-attempt-started:" + attemptId;
        }
    }

    record SegmentCompleted(
        String tenantId,
        String runId,
        String segmentId,
        String attemptId,
        List<?> outputItems,
        boolean terminalSegment
    ) implements ControlPlaneFact {
        public SegmentCompleted {
            tenantId = ControlPlaneChecks.requireText(tenantId, "tenantId");
            runId = ControlPlaneChecks.requireText(runId, "runId");
            segmentId = ControlPlaneChecks.requireText(segmentId, "segmentId");
            attemptId = ControlPlaneChecks.requireText(attemptId, "attemptId");
            outputItems = ControlPlaneChecks.copyList(outputItems);
        }

        @Override
        public String factKey() {
            return "segment-completed:" + attemptId;
        }
    }

    record SegmentSuspended(
        String tenantId,
        String runId,
        String segmentId,
        String attemptId,
        String boundaryUnitId,
        BoundaryKind boundaryKind,
        int boundaryStepIndex,
        Integer expectedItemCount
    ) implements ControlPlaneFact {
        public SegmentSuspended {
            tenantId = ControlPlaneChecks.requireText(tenantId, "tenantId");
            runId = ControlPlaneChecks.requireText(runId, "runId");
            segmentId = ControlPlaneChecks.requireText(segmentId, "segmentId");
            attemptId = ControlPlaneChecks.requireText(attemptId, "attemptId");
            boundaryUnitId = ControlPlaneChecks.requireText(boundaryUnitId, "boundaryUnitId");
            Objects.requireNonNull(boundaryKind, "boundaryKind must not be null");
            ControlPlaneChecks.requireNonNegative(boundaryStepIndex, "boundaryStepIndex");
            if (expectedItemCount != null && expectedItemCount < 0) {
                throw new IllegalArgumentException("expectedItemCount must not be negative");
            }
        }

        @Override
        public String factKey() {
            return "segment-suspended:" + attemptId + ":" + boundaryUnitId;
        }
    }

    record BoundaryInteractionDispatched(
        String tenantId,
        String runId,
        String boundaryUnitId,
        BoundaryKind boundaryKind,
        String interactionId,
        String correlationId,
        String idempotencyKey,
        Integer itemIndex,
        Object requestPayload,
        String transportType,
        long deadlineEpochMs
    ) implements ControlPlaneFact {
        public BoundaryInteractionDispatched {
            tenantId = ControlPlaneChecks.requireText(tenantId, "tenantId");
            runId = ControlPlaneChecks.requireText(runId, "runId");
            boundaryUnitId = ControlPlaneChecks.requireText(boundaryUnitId, "boundaryUnitId");
            Objects.requireNonNull(boundaryKind, "boundaryKind must not be null");
            interactionId = ControlPlaneChecks.requireText(interactionId, "interactionId");
            correlationId = ControlPlaneChecks.requireText(correlationId, "correlationId");
            idempotencyKey = ControlPlaneChecks.requireText(idempotencyKey, "idempotencyKey");
            if (itemIndex != null && itemIndex < 0) {
                throw new IllegalArgumentException("itemIndex must not be negative");
            }
            transportType = ControlPlaneChecks.requireText(transportType, "transportType");
            ControlPlaneChecks.requireNonNegative(deadlineEpochMs, "deadlineEpochMs");
        }

        @Override
        public String factKey() {
            return "boundary-interaction-dispatched:" + boundaryUnitId + ":" + interactionId;
        }
    }

    record BoundaryDispatchCompleted(
        String tenantId,
        String runId,
        String boundaryUnitId,
        int expectedItemCount
    ) implements ControlPlaneFact {
        public BoundaryDispatchCompleted {
            tenantId = ControlPlaneChecks.requireText(tenantId, "tenantId");
            runId = ControlPlaneChecks.requireText(runId, "runId");
            boundaryUnitId = ControlPlaneChecks.requireText(boundaryUnitId, "boundaryUnitId");
            ControlPlaneChecks.requireNonNegative(expectedItemCount, "expectedItemCount");
        }

        @Override
        public String factKey() {
            return "boundary-dispatch-completed:" + boundaryUnitId;
        }
    }

    record BoundaryCompletionAdmitted(
        String tenantId,
        String runId,
        String boundaryUnitId,
        BoundaryKind boundaryKind,
        String interactionId,
        String idempotencyKey,
        Object responsePayload
    ) implements ControlPlaneFact {
        public BoundaryCompletionAdmitted {
            tenantId = ControlPlaneChecks.requireText(tenantId, "tenantId");
            runId = ControlPlaneChecks.requireText(runId, "runId");
            boundaryUnitId = ControlPlaneChecks.requireText(boundaryUnitId, "boundaryUnitId");
            Objects.requireNonNull(boundaryKind, "boundaryKind must not be null");
            interactionId = ControlPlaneChecks.requireText(interactionId, "interactionId");
            idempotencyKey = ControlPlaneChecks.requireText(idempotencyKey, "idempotencyKey");
        }

        @Override
        public String factKey() {
            return "boundary-completion-admitted:" + boundaryUnitId + ":" + idempotencyKey;
        }
    }

    record InteractionTimedOut(
        String tenantId,
        String runId,
        String boundaryUnitId,
        String interactionId,
        String reason
    ) implements ControlPlaneFact {
        public InteractionTimedOut {
            tenantId = ControlPlaneChecks.requireText(tenantId, "tenantId");
            runId = ControlPlaneChecks.requireText(runId, "runId");
            boundaryUnitId = ControlPlaneChecks.requireText(boundaryUnitId, "boundaryUnitId");
            interactionId = ControlPlaneChecks.requireText(interactionId, "interactionId");
            reason = reason == null || reason.isBlank() ? "Boundary interaction timed out" : reason;
        }

        @Override
        public String factKey() {
            return "interaction-timed-out:" + boundaryUnitId + ":" + interactionId;
        }
    }

    record ContinuationSegmentCreated(
        String tenantId,
        String runId,
        String parentSegmentId,
        String segmentId,
        String boundaryUnitId,
        int startStepIndex,
        int stopBeforeStepIndex,
        Object inputPayload
    ) implements ControlPlaneFact {
        public ContinuationSegmentCreated {
            tenantId = ControlPlaneChecks.requireText(tenantId, "tenantId");
            runId = ControlPlaneChecks.requireText(runId, "runId");
            parentSegmentId = ControlPlaneChecks.requireText(parentSegmentId, "parentSegmentId");
            segmentId = ControlPlaneChecks.requireText(segmentId, "segmentId");
            boundaryUnitId = ControlPlaneChecks.requireText(boundaryUnitId, "boundaryUnitId");
            ControlPlaneChecks.requireNonNegative(startStepIndex, "startStepIndex");
            if (stopBeforeStepIndex >= 0 && stopBeforeStepIndex < startStepIndex) {
                throw new IllegalArgumentException("stopBeforeStepIndex must be greater than or equal to startStepIndex");
            }
        }

        @Override
        public String factKey() {
            return "continuation-segment-created:" + segmentId;
        }
    }

    record TerminalPublicationCompleted(
        String tenantId,
        String runId,
        String segmentId,
        String publicationId,
        String idempotencyKey
    ) implements ControlPlaneFact {
        public TerminalPublicationCompleted {
            tenantId = ControlPlaneChecks.requireText(tenantId, "tenantId");
            runId = ControlPlaneChecks.requireText(runId, "runId");
            segmentId = ControlPlaneChecks.requireText(segmentId, "segmentId");
            publicationId = ControlPlaneChecks.requireText(publicationId, "publicationId");
            idempotencyKey = ControlPlaneChecks.requireText(idempotencyKey, "idempotencyKey");
        }

        @Override
        public String factKey() {
            return "terminal-publication-completed:" + publicationId + ":" + idempotencyKey;
        }
    }

    record RunSucceeded(
        String tenantId,
        String runId,
        String segmentId,
        Object resultPayload
    ) implements ControlPlaneFact {
        public RunSucceeded {
            tenantId = ControlPlaneChecks.requireText(tenantId, "tenantId");
            runId = ControlPlaneChecks.requireText(runId, "runId");
            segmentId = ControlPlaneChecks.requireText(segmentId, "segmentId");
        }

        @Override
        public String factKey() {
            return "run-succeeded:" + runId;
        }
    }

    record RunFailed(
        String tenantId,
        String runId,
        String segmentId,
        String errorCode,
        String errorMessage
    ) implements ControlPlaneFact {
        public RunFailed {
            tenantId = ControlPlaneChecks.requireText(tenantId, "tenantId");
            runId = ControlPlaneChecks.requireText(runId, "runId");
            segmentId = ControlPlaneChecks.requireText(segmentId, "segmentId");
            errorCode = ControlPlaneChecks.requireText(errorCode, "errorCode");
            errorMessage = errorMessage == null ? "" : errorMessage;
        }

        @Override
        public String factKey() {
            return "run-failed:" + runId + ":" + errorCode;
        }
    }
}
