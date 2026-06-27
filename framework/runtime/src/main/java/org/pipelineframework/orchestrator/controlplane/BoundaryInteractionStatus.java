package org.pipelineframework.orchestrator.controlplane;

public enum BoundaryInteractionStatus {
    DISPATCHED,
    COMPLETED,
    TIMED_OUT,
    FAILED;

    public boolean terminal() {
        return this == COMPLETED || this == TIMED_OUT || this == FAILED;
    }
}
