package org.pipelineframework.orchestrator;

/**
 * Execution strategy for admitted queue-async transitions.
 */
public enum TransitionWorkerExecutionMode {
    /** Preserve current in-process subscription behavior. */
    SAME_THREAD,
    /** Subscribe admitted transitions on a bounded virtual-thread offload point. */
    VIRTUAL_THREAD
}
