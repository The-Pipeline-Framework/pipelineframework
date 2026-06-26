package org.pipelineframework.orchestrator.controlplane;

import java.util.List;

public sealed interface SegmentOutcome permits
    SegmentOutcome.Completed,
    SegmentOutcome.Suspended,
    SegmentOutcome.Failed {

    record Completed(List<?> outputItems, boolean terminalSegment) implements SegmentOutcome {
        public Completed {
            outputItems = ControlPlaneChecks.copyList(outputItems);
        }
    }

    record Suspended(
        String boundaryUnitId,
        BoundaryKind boundaryKind,
        int boundaryStepIndex,
        Integer expectedItemCount
    ) implements SegmentOutcome {
        public Suspended {
            boundaryUnitId = ControlPlaneChecks.requireText(boundaryUnitId, "boundaryUnitId");
            java.util.Objects.requireNonNull(boundaryKind, "boundaryKind must not be null");
            ControlPlaneChecks.requireNonNegative(boundaryStepIndex, "boundaryStepIndex");
            if (expectedItemCount != null && expectedItemCount < 0) {
                throw new IllegalArgumentException("expectedItemCount must not be negative");
            }
        }
    }

    record Failed(String errorCode, String errorMessage) implements SegmentOutcome {
        public Failed {
            errorCode = ControlPlaneChecks.requireText(errorCode, "errorCode");
            errorMessage = errorMessage == null ? "" : errorMessage;
        }
    }
}
