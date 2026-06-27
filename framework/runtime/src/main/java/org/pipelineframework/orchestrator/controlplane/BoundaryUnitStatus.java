package org.pipelineframework.orchestrator.controlplane;

public enum BoundaryUnitStatus {
    OPEN,
    DISPATCH_COMPLETE,
    COMPLETED,
    TIMED_OUT,
    FAILED;

    public boolean terminal() {
        return this == COMPLETED || this == TIMED_OUT || this == FAILED;
    }
}
