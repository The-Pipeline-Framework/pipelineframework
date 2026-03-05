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
     * Whether the execution is terminal.
     *
     * @return true if the status is terminal
     */
    public boolean terminal() {
        return this == SUCCEEDED || this == FAILED || this == DLQ;
    }
}
