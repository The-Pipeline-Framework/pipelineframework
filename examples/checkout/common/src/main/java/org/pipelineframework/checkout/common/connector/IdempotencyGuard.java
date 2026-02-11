package org.pipelineframework.checkout.common.connector;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Bounded in-memory idempotency guard for connector handoff keys.
 *
 * <p>This is intentionally process-local and best-effort: it prevents duplicate forwarding
 * within the current runtime instance while keeping memory bounded via simple LRU eviction.</p>
 */
public final class IdempotencyGuard {

    private final int maxKeys;
    private final LinkedHashMap<String, Boolean> seen;

    public IdempotencyGuard(int maxKeys) {
        if (maxKeys <= 0) {
            throw new IllegalArgumentException("maxKeys must be > 0");
        }
        this.maxKeys = maxKeys;
        this.seen = new LinkedHashMap<>(16, 0.75f, true);
    }

    /**
     * Marks the given key as seen and returns whether this is the first occurrence.
     *
     * @param key deterministic idempotency key
     * @return {@code true} if this key was not seen before in the current bounded window; {@code false} otherwise
     */
    public synchronized boolean markIfNew(String key) {
        Objects.requireNonNull(key, "key must not be null");
        if (key.isBlank()) {
            throw new IllegalArgumentException("key must not be blank");
        }
        if (seen.containsKey(key)) {
            // Touch the key for access-order LRU.
            seen.get(key);
            return false;
        }
        seen.put(key, Boolean.TRUE);
        evictIfNeeded();
        return true;
    }

    public synchronized boolean contains(String key) {
        Objects.requireNonNull(key, "key must not be null");
        return seen.containsKey(key);
    }

    private void evictIfNeeded() {
        while (seen.size() > maxKeys) {
            String eldest = seen.entrySet().iterator().next().getKey();
            seen.remove(eldest);
        }
    }

    synchronized int size() {
        return seen.size();
    }

    synchronized Map<String, Boolean> snapshot() {
        return Map.copyOf(seen);
    }
}
