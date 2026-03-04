package org.pipelineframework.orchestrator;

/**
 * Execution lifecycle status for async orchestrator runs.
 */
public enum ExecutionStatus {
    QUEUED,
    RUNNING,
    WAIT_RETRY,
    SUCCEEDED,
    FAILED,
    DLQ;

    /**
     * Indicates whether this execution status is terminal.
     *
     * @return `true` if the status is `SUCCEEDED`, `FAILED`, or `DLQ`, `false` otherwise.
     */
    public boolean terminal() {
        return this == SUCCEEDED || this == FAILED || this == DLQ;
    }
}
