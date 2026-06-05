package org.pipelineframework.orchestrator;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bounded nonce replay guard for signed transition-worker server protocols.
 */
final class TransitionWorkerNonceReplayGuard {

    private static final int MAX_ACCEPTED_NONCES = 10_000;
    private static final long PURGE_INTERVAL_MS = 30_000L;

    private final ConcurrentHashMap<String, Long> acceptedNonces = new ConcurrentHashMap<>();
    private final AtomicLong lastPurgeEpochMs = new AtomicLong();

    boolean accept(String nonce, long timestampEpochMs, long nowEpochMs, long toleranceMs) {
        if (nonce == null || nonce.isBlank()) {
            return false;
        }
        purgeExpired(nowEpochMs, toleranceMs);
        boolean accepted = acceptedNonces.putIfAbsent(nonce, timestampEpochMs) == null;
        if (accepted && acceptedNonces.size() > MAX_ACCEPTED_NONCES) {
            trimOldest();
        }
        return accepted;
    }

    private void purgeExpired(long nowEpochMs, long toleranceMs) {
        long previous = lastPurgeEpochMs.get();
        if (nowEpochMs - previous < PURGE_INTERVAL_MS
            || !lastPurgeEpochMs.compareAndSet(previous, nowEpochMs)) {
            return;
        }
        long oldestAccepted = nowEpochMs - Math.max(0L, toleranceMs);
        acceptedNonces.entrySet().removeIf(entry -> entry.getValue() < oldestAccepted);
    }

    private void trimOldest() {
        int overflow = acceptedNonces.size() - MAX_ACCEPTED_NONCES;
        if (overflow <= 0) {
            return;
        }
        acceptedNonces.entrySet().stream()
            .sorted(Comparator.comparingLong(Map.Entry::getValue))
            .limit(overflow)
            .map(Map.Entry::getKey)
            .forEach(acceptedNonces::remove);
    }
}
