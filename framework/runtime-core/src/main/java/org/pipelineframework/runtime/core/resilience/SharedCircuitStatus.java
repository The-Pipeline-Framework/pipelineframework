package org.pipelineframework.runtime.core.resilience;

/** Authoritative shared-circuit state. */
public enum SharedCircuitStatus {
    CLOSED,
    OPEN,
    HALF_OPEN
}
