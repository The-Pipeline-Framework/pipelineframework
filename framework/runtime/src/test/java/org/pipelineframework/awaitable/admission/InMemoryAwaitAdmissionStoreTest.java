package org.pipelineframework.awaitable.admission;

import java.util.Optional;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class InMemoryAwaitAdmissionStoreTest {

    @Test
    void sharesCapacityAcrossTenantsAndReusesTheSameOwnerReservation() {
        InMemoryAwaitAdmissionStore store = new InMemoryAwaitAdmissionStore();
        AwaitAdmissionScope scope = new AwaitAdmissionScope("payments", "provider-await", "kafka://payments.requests");
        AwaitAdmissionOwner first = new AwaitAdmissionOwner("unit-a:item-0");
        AwaitAdmissionOwner second = new AwaitAdmissionOwner("unit-b:item-0");

        AwaitAdmissionAcquireResult acquired = store.acquire(scope, first, 1, 10_000, 1_000)
            .toCompletableFuture().join();
        AwaitAdmissionAcquireResult reused = store.acquire(scope, first, 1, 10_000, 1_001)
            .toCompletableFuture().join();
        AwaitAdmissionAcquireResult unavailable = store.acquire(scope, second, 1, 10_000, 1_001)
            .toCompletableFuture().join();

        assertTrue(acquired.acquired());
        assertTrue(reused.acquired());
        assertTrue(reused.reused());
        assertFalse(unavailable.acquired());

        assertTrue(store.release(acquired.reservation().orElseThrow()).toCompletableFuture().join());
        assertTrue(store.acquire(scope, second, 1, 10_000, 1_002).toCompletableFuture().join().acquired());
    }

    @Test
    void expiresAnOrphanedReservationBeforeClaimingItsSlot() {
        InMemoryAwaitAdmissionStore store = new InMemoryAwaitAdmissionStore();
        AwaitAdmissionScope scope = new AwaitAdmissionScope("payments", "provider-await", "sqs://queue");

        store.acquire(scope, new AwaitAdmissionOwner("orphan"), 1, 1_500, 1_000).toCompletableFuture().join();

        AwaitAdmissionAcquireResult replacement = store.acquire(
            scope, new AwaitAdmissionOwner("replacement"), 1, 3_000, 1_501)
            .toCompletableFuture().join();

        assertTrue(replacement.acquired());
        assertTrue(replacement.reconciledExpired());
    }

    @Test
    void staleLeaseCannotReleaseAReacquiredSlot() {
        InMemoryAwaitAdmissionStore store = new InMemoryAwaitAdmissionStore();
        AwaitAdmissionScope scope = new AwaitAdmissionScope("payments", "provider-await", "kafka://requests");
        AwaitAdmissionOwner owner = new AwaitAdmissionOwner("tenant-a:unit:item");

        AwaitAdmissionReservation expired = store.acquire(scope, owner, 1, 1_000, 1)
            .toCompletableFuture().join().reservation().orElseThrow();
        AwaitAdmissionReservation replacement = store.acquire(scope, owner, 1, 3_000, 1_001)
            .toCompletableFuture().join().reservation().orElseThrow();

        assertFalse(store.release(expired).toCompletableFuture().join());
        assertTrue(store.release(replacement).toCompletableFuture().join());
    }

    @Test
    void rejectsAReusedResultWithoutAReservation() {
        assertThrows(IllegalArgumentException.class, () -> new AwaitAdmissionAcquireResult(Optional.empty(), true, false));
    }

    @Test
    void rejectsInconsistentReconciliationResults() {
        assertThrows(IllegalArgumentException.class, () -> new AwaitAdmissionAcquireResult(Optional.empty(), false, true));
        AwaitAdmissionReservation reservation = new AwaitAdmissionReservation(
            new AwaitAdmissionScope("payments", "provider-await", "kafka://requests"),
            new AwaitAdmissionOwner("tenant-a:unit:item"), 0, 1_000);
        assertThrows(IllegalArgumentException.class, () -> new AwaitAdmissionAcquireResult(Optional.of(reservation), true, true));
    }

    @Test
    void scopeKeyUsesUnambiguousComponentBoundaries() {
        AwaitAdmissionScope first = new AwaitAdmissionScope("payments:priority", "await", "kafka://requests");
        AwaitAdmissionScope second = new AwaitAdmissionScope("payments", "priority:await", "kafka://requests");

        assertFalse(first.key().equals(second.key()));
    }
}
