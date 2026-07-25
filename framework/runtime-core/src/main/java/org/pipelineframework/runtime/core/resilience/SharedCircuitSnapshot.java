package org.pipelineframework.runtime.core.resilience;

import java.time.Instant;
import java.util.Objects;

/** Immutable authoritative snapshot returned by a shared circuit store. */
public record SharedCircuitSnapshot(
    SharedCircuitStatus status,
    long epoch,
    long version,
    Instant openUntil
) {
    public SharedCircuitSnapshot {
        Objects.requireNonNull(status, "status must not be null");
        Objects.requireNonNull(openUntil, "openUntil must not be null");
        if (epoch < 0 || version < 0) {
            throw new IllegalArgumentException("epoch and version must not be negative");
        }
    }

    public static SharedCircuitSnapshot closed() {
        return new SharedCircuitSnapshot(SharedCircuitStatus.CLOSED, 0L, 0L, Instant.MIN);
    }
}
