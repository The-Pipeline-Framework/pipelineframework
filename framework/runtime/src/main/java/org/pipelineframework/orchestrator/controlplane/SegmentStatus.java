package org.pipelineframework.orchestrator.controlplane;

public enum SegmentStatus {
    QUEUED,
    RUNNING,
    SUSPENDED,
    COMPLETED,
    FAILED;

    public boolean terminal() {
        return this == COMPLETED || this == FAILED;
    }
}
