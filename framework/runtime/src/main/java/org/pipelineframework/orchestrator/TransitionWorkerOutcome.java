package org.pipelineframework.orchestrator;

/**
 * Explicit outcome returned by a transition worker.
 */
public enum TransitionWorkerOutcome {
    COMPLETED,
    WAITING_EXTERNAL,
    FAILED
}
