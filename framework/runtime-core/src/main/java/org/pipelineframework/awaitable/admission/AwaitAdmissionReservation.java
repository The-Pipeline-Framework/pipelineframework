package org.pipelineframework.awaitable.admission;

import java.util.Objects;

/**
 * Immutable claim of one pending-interaction slot.
 */
public record AwaitAdmissionReservation(
    AwaitAdmissionScope scope,
    AwaitAdmissionOwner owner,
    int slot,
    long expiresAtEpochMs,
    String leaseToken
) {
    public AwaitAdmissionReservation(AwaitAdmissionScope scope, AwaitAdmissionOwner owner, int slot, long expiresAtEpochMs) {
        this(scope, owner, slot, expiresAtEpochMs, java.util.UUID.randomUUID().toString());
    }

    public AwaitAdmissionReservation {
        Objects.requireNonNull(scope, "scope must not be null");
        Objects.requireNonNull(owner, "owner must not be null");
        if (slot < 0) {
            throw new IllegalArgumentException("slot must not be negative");
        }
        if (expiresAtEpochMs <= 0) {
            throw new IllegalArgumentException("expiresAtEpochMs must be positive");
        }
        if (leaseToken == null || leaseToken.isBlank()) {
            throw new IllegalArgumentException("leaseToken must not be blank");
        }
    }
}
