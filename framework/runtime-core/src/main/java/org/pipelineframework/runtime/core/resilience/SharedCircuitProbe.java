package org.pipelineframework.runtime.core.resilience;

import java.time.Instant;
import java.util.Objects;

/** A token-guarded, expiring shared HALF_OPEN probe lease. */
public record SharedCircuitProbe(long epoch, int slot, String leaseToken, Instant expiresAt) {
    public SharedCircuitProbe {
        if (epoch < 0 || slot < 0) {
            throw new IllegalArgumentException("epoch and slot must not be negative");
        }
        Objects.requireNonNull(leaseToken, "leaseToken must not be null");
        Objects.requireNonNull(expiresAt, "expiresAt must not be null");
    }
}
