package org.pipelineframework.runtime.core.resilience;

import java.util.Objects;

/**
 * Failure returned to an invocation when no dependency call was started.
 */
public final class CircuitOpenException extends RuntimeException {
    private final CircuitOpen circuitOpen;

    public CircuitOpenException(CircuitOpen circuitOpen) {
        super("Circuit is open for " + Objects.requireNonNull(circuitOpen, "circuitOpen must not be null").identity());
        this.circuitOpen = circuitOpen;
    }

    public CircuitOpen circuitOpen() {
        return circuitOpen;
    }
}
