package org.pipelineframework.awaitable.admission;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.awaitable.admission.AwaitAdmissionAcquireResult;
import org.pipelineframework.awaitable.admission.AwaitAdmissionOwner;
import org.pipelineframework.awaitable.admission.AwaitAdmissionReservation;
import org.pipelineframework.awaitable.admission.AwaitAdmissionScope;
import org.pipelineframework.awaitable.admission.AwaitAdmissionStore;

/**
 * Local-only admission store for tests and single-process development.
 */
@ApplicationScoped
public class InMemoryAwaitAdmissionStore implements AwaitAdmissionStore {
    private final Map<String, AwaitAdmissionReservation> reservations = new HashMap<>();

    @Override
    public String providerName() {
        return "in-memory";
    }

    @Override
    public CompletionStage<AwaitAdmissionAcquireResult> acquire(
        AwaitAdmissionScope scope,
        AwaitAdmissionOwner owner,
        int capacity,
        long expiresAtEpochMs,
        long nowEpochMs
    ) {
        if (capacity < 1) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("capacity must be positive"));
        }
        synchronized (reservations) {
            Set<String> expiredKeys = new HashSet<>();
            reservations.entrySet().removeIf(entry -> {
                boolean expired = entry.getValue().expiresAtEpochMs() <= nowEpochMs;
                if (expired) {
                    expiredKeys.add(entry.getKey());
                }
                return expired;
            });
            for (AwaitAdmissionReservation reservation : reservations.values()) {
                if (reservation.scope().equals(scope) && reservation.owner().equals(owner)) {
                    return CompletableFuture.completedFuture(AwaitAdmissionAcquireResult.reused(reservation));
                }
            }
            for (int slot = 0; slot < capacity; slot++) {
                String key = slotKey(scope, slot);
                if (!reservations.containsKey(key)) {
                    AwaitAdmissionReservation reservation = new AwaitAdmissionReservation(scope, owner, slot, expiresAtEpochMs);
                    reservations.put(key, reservation);
                    return CompletableFuture.completedFuture(expiredKeys.contains(key)
                        ? AwaitAdmissionAcquireResult.acquiredAfterReconciliation(reservation)
                        : AwaitAdmissionAcquireResult.acquired(reservation));
                }
            }
            return CompletableFuture.completedFuture(AwaitAdmissionAcquireResult.unavailable());
        }
    }

    @Override
    public CompletionStage<Boolean> release(AwaitAdmissionReservation reservation) {
        synchronized (reservations) {
            String key = slotKey(reservation.scope(), reservation.slot());
            boolean removed = reservations.remove(key, reservation);
            return CompletableFuture.completedFuture(removed);
        }
    }

    private static String slotKey(AwaitAdmissionScope scope, int slot) {
        return scope.key() + "|" + slot;
    }
}
