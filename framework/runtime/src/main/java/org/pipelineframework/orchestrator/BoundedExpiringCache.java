package org.pipelineframework.orchestrator;

import java.time.Duration;
import java.util.Comparator;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * Small process-local cache for release-scoped runtime metadata.
 *
 * <p>The cache deliberately has no persistence semantics: pinned releases remain authoritative in the
 * release registry. Entries expire and the least-recently-used entry is discarded when the fixed capacity
 * is reached, preventing tenant/release churn from retaining release records indefinitely.</p>
 */
public final class BoundedExpiringCache<K, V> {
    private final ConcurrentHashMap<K, Entry<V>> entries = new ConcurrentHashMap<>();
    private final int maximumSize;
    private final long expiresAfterNanos;
    private final LongSupplier nanoTime;

    public BoundedExpiringCache(int maximumSize, Duration expiresAfter) {
        this(maximumSize, expiresAfter, System::nanoTime);
    }

    BoundedExpiringCache(int maximumSize, Duration expiresAfter, LongSupplier nanoTime) {
        if (maximumSize <= 0) {
            throw new IllegalArgumentException("maximumSize must be positive");
        }
        if (expiresAfter == null || expiresAfter.isZero() || expiresAfter.isNegative()) {
            throw new IllegalArgumentException("expiresAfter must be positive");
        }
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
        this.maximumSize = maximumSize;
        this.expiresAfterNanos = expiresAfter.toNanos();
    }

    public V getOrLoad(K key, Function<K, V> loader) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(loader, "loader");
        long now = nanoTime.getAsLong();
        removeExpired(now);
        if (!entries.containsKey(key)) {
            evictToSize(maximumSize - 1);
        }
        Entry<V> entry = entries.compute(key, (ignored, existing) -> {
            if (existing != null && !existing.expired(now, expiresAfterNanos)) {
                return existing.accessed(now);
            }
            return new Entry<>(Objects.requireNonNull(loader.apply(key), "loader result"), now);
        });
        evictToSize(maximumSize);
        return entry.value();
    }

    private void removeExpired(long now) {
        entries.entrySet().removeIf(entry -> entry.getValue().expired(now, expiresAfterNanos));
    }

    private void evictToSize(int maximumEntries) {
        while (entries.size() > maximumEntries) {
            entries.entrySet().stream()
                .min(Comparator.comparingLong(entry -> entry.getValue().lastAccessNanos()))
                .ifPresent(oldest -> entries.remove(oldest.getKey(), oldest.getValue()));
        }
    }

    private record Entry<V>(V value, long lastAccessNanos) {
        private boolean expired(long now, long expiration) {
            return now - lastAccessNanos >= expiration;
        }

        private Entry<V> accessed(long now) {
            return new Entry<>(value, now);
        }
    }
}
