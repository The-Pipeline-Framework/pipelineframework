package org.pipelineframework.orchestrator;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BoundedExpiringCacheTest {

    @Test
    void evictsTheLeastRecentlyUsedEntryAtItsBound() {
        BoundedExpiringCache<String, Integer> cache = new BoundedExpiringCache<>(1, Duration.ofMinutes(1));
        AtomicInteger loads = new AtomicInteger();

        assertEquals(1, cache.getOrLoad("first", ignored -> loads.incrementAndGet()));
        assertEquals(2, cache.getOrLoad("second", ignored -> loads.incrementAndGet()));
        assertEquals(3, cache.getOrLoad("first", ignored -> loads.incrementAndGet()));

        assertEquals(3, loads.get());
    }

    @Test
    void reloadsAnEntryAfterExpiry() {
        AtomicLong clock = new AtomicLong();
        BoundedExpiringCache<String, Integer> cache = new BoundedExpiringCache<>(2, Duration.ofNanos(10), clock::get);
        AtomicInteger loads = new AtomicInteger();

        assertEquals(1, cache.getOrLoad("release", ignored -> loads.incrementAndGet()));
        clock.set(10);

        assertEquals(2, cache.getOrLoad("release", ignored -> loads.incrementAndGet()));
        assertEquals(2, loads.get());
    }
}
