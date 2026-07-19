package org.pipelineframework.runtime.core.resilience;

import java.util.Objects;

/**
 * Failure returned when a shared circuit must fail closed because its authority is unavailable.
 */
public final class CircuitProtectionUnavailableException extends RuntimeException {
    private final CircuitProtectionUnavailable protection;

    public CircuitProtectionUnavailableException(CircuitProtectionUnavailable protection) {
        super("Circuit protection is unavailable for "
            + Objects.requireNonNull(protection, "protection must not be null").identity());
        this.protection = protection;
    }

    public CircuitProtectionUnavailable protection() {
        return protection;
    }
}
