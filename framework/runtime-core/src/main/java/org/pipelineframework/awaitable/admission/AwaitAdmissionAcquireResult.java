package org.pipelineframework.awaitable.admission;

import java.util.Objects;
import java.util.Optional;

/**
 * Result of attempting to reserve a provider-facing await slot.
 */
public record AwaitAdmissionAcquireResult(
    Optional<AwaitAdmissionReservation> reservation,
    boolean reused,
    boolean reconciledExpired
) {
    public AwaitAdmissionAcquireResult {
        reservation = Objects.requireNonNull(reservation, "reservation must not be null");
        if (reused && reservation.isEmpty()) {
            throw new IllegalArgumentException("reused results must include a reservation");
        }
        if (reconciledExpired && reservation.isEmpty()) {
            throw new IllegalArgumentException("reconciled results must include a reservation");
        }
        if (reused && reconciledExpired) {
            throw new IllegalArgumentException("reused and reconciled results are mutually exclusive");
        }
    }

    public static AwaitAdmissionAcquireResult acquired(AwaitAdmissionReservation reservation) {
        return new AwaitAdmissionAcquireResult(Optional.of(reservation), false, false);
    }

    public static AwaitAdmissionAcquireResult acquiredAfterReconciliation(AwaitAdmissionReservation reservation) {
        return new AwaitAdmissionAcquireResult(Optional.of(reservation), false, true);
    }

    public static AwaitAdmissionAcquireResult reused(AwaitAdmissionReservation reservation) {
        return new AwaitAdmissionAcquireResult(Optional.of(reservation), true, false);
    }

    public static AwaitAdmissionAcquireResult unavailable() {
        return new AwaitAdmissionAcquireResult(Optional.empty(), false, false);
    }

    public boolean acquired() {
        return reservation.isPresent();
    }
}
