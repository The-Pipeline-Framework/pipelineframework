package org.pipelineframework.runtime.core.resilience;

/**
 * Transport-agnostic terminal outcome supplied by the invocation boundary.
 */
public enum CircuitOutcome {
    SUCCESS,
    HEALTH_FAILURE,
    NEUTRAL
}
