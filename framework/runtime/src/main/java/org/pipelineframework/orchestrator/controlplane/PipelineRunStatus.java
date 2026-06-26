package org.pipelineframework.orchestrator.controlplane;

public enum PipelineRunStatus {
    ACCEPTED,
    RUNNING,
    WAITING_BOUNDARY,
    SUCCEEDED,
    FAILED,
    DLQ;

    public boolean terminal() {
        return this == SUCCEEDED || this == FAILED || this == DLQ;
    }
}
