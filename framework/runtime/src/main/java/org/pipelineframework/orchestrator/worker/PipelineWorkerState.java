package org.pipelineframework.orchestrator.worker;

/**
 * Lifecycle state used to admit new hosted executions.
 */
public enum PipelineWorkerState {
    HEALTHY,
    STALE,
    DRAINING,
    UNAVAILABLE
}
