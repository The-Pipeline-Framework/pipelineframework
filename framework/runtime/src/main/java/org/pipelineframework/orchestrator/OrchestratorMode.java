package org.pipelineframework.orchestrator;

/**
 * Execution mode for orchestrator entry points.
 */
public enum OrchestratorMode {
    /**
     * Existing synchronous behavior.
     */
    SYNC,
    /**
     * Queue-backed async behavior.
     */
    QUEUE_ASYNC
}
