package org.pipelineframework.orchestrator;

/**
 * Execution lifecycle status for async orchestrator runs.
 */
public enum ExecutionStatus {
    /** Execution is accepted and waiting to be claimed by a queue worker. */
    QUEUED,
    /** Execution is currently leased and running in a queue worker. */
    RUNNING,
    /** Execution is paused at an await step until external completion arrives or its deadline expires. */
    WAITING_EXTERNAL,
    /** Execution failed transiently and is waiting for its retry due time. */
    WAIT_RETRY,
    /** Execution completed successfully and has a persisted result payload. */
    SUCCEEDED,
    /** Execution reached a terminal failure before dead-letter publication. */
    FAILED,
    /** Execution reached a terminal failure and was routed through the dead-letter path. */
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
