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
        // Thread-safety invariant:
        // this map is mutated/read under synchronization by markIfNew/contains/size/snapshot.
        // removeEldestEntry therefore executes while markIfNew holds the same monitor.
        // Any future non-synchronized caller touching seen/maxKeys must either synchronize
        // on this instance or revisit the locking strategy.
        this.seen = new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                return size() > IdempotencyGuard.this.maxKeys;
            }
        };
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
        return true;
    }

    public synchronized boolean contains(String key) {
        Objects.requireNonNull(key, "key must not be null");
        if (key.isBlank()) {
            throw new IllegalArgumentException("key must not be blank");
        }
        return seen.containsKey(key);
    }

    synchronized int size() {
        return seen.size();
    }

    synchronized Map<String, Boolean> snapshot() {
        return Map.copyOf(seen);
    }
}
