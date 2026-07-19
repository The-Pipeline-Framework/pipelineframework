package org.pipelineframework.runtime.core.resilience;

/**
 * State changes emitted by a circuit breaker implementation.
 */
public enum CircuitStateTransition {
    CLOSED_TO_OPEN,
    OPEN_TO_HALF_OPEN,
    HALF_OPEN_TO_CLOSED,
    HALF_OPEN_TO_OPEN
}
