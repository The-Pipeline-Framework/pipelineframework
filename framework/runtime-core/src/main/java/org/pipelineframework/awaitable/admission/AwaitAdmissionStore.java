package org.pipelineframework.awaitable.admission;

import java.util.concurrent.CompletionStage;

/**
 * Portable durable store for provider-facing await admission reservations.
 */
public interface AwaitAdmissionStore {
    String providerName();

    CompletionStage<AwaitAdmissionAcquireResult> acquire(
        AwaitAdmissionScope scope,
        AwaitAdmissionOwner owner,
        int capacity,
        long expiresAtEpochMs,
        long nowEpochMs);

    CompletionStage<Boolean> release(AwaitAdmissionReservation reservation);

}
