package org.pipelineframework.orchestrator;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    @Test
    void coalescesThirtyTwoConcurrentSameKeyLoads() throws Exception {
        BoundedExpiringCache<String, Integer> cache = new BoundedExpiringCache<>(2, Duration.ofMinutes(1));
        AtomicInteger loads = new AtomicInteger();
        CountDownLatch loaderStarted = new CountDownLatch(1);
        CountDownLatch releaseLoader = new CountDownLatch(1);
        ExecutorService callers = Executors.newFixedThreadPool(32);
        try {
            List<Future<Integer>> results = java.util.stream.IntStream.range(0, 32)
                .mapToObj(ignored -> callers.submit(() -> cache.getOrLoad("release", key -> {
                    loads.incrementAndGet();
                    loaderStarted.countDown();
                    await(releaseLoader);
                    return 42;
                })))
                .toList();

            assertTrue(loaderStarted.await(1, TimeUnit.SECONDS));
            assertEquals(1, loads.get());
            assertTrue(results.stream().noneMatch(Future::isDone));

            releaseLoader.countDown();
            for (Future<Integer> result : results) {
                assertEquals(42, result.get(1, TimeUnit.SECONDS));
            }
            assertEquals(1, loads.get());
        } finally {
            callers.shutdownNow();
        }
    }

    @Test
    void failedLoadDoesNotPoisonSubsequentSameKeyRetry() {
        BoundedExpiringCache<String, Integer> cache = new BoundedExpiringCache<>(2, Duration.ofMinutes(1));
        AtomicInteger loads = new AtomicInteger();

        assertThrows(IllegalStateException.class,
            () -> cache.getOrLoad("release", key -> {
                loads.incrementAndGet();
                throw new IllegalStateException("release lookup failed");
            }));

        assertEquals(7, cache.getOrLoad("release", key -> {
            loads.incrementAndGet();
            return 7;
        }));
        assertEquals(2, loads.get());
    }

    @Test
    void distinctKeysLoadIndependently() throws Exception {
        BoundedExpiringCache<String, Integer> cache = new BoundedExpiringCache<>(2, Duration.ofMinutes(1));
        CountDownLatch bothLoadersStarted = new CountDownLatch(2);
        CountDownLatch releaseLoaders = new CountDownLatch(1);
        ExecutorService callers = Executors.newFixedThreadPool(2);
        try {
            Future<Integer> first = callers.submit(() -> cache.getOrLoad("first", key -> {
                bothLoadersStarted.countDown();
                await(releaseLoaders);
                return 1;
            }));
            Future<Integer> second = callers.submit(() -> cache.getOrLoad("second", key -> {
                bothLoadersStarted.countDown();
                await(releaseLoaders);
                return 2;
            }));

            assertTrue(bothLoadersStarted.await(1, TimeUnit.SECONDS));
            releaseLoaders.countDown();
            assertEquals(1, first.get(1, TimeUnit.SECONDS));
            assertEquals(2, second.get(1, TimeUnit.SECONDS));
        } finally {
            callers.shutdownNow();
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(1, TimeUnit.SECONDS)) {
                throw new IllegalStateException("test loader was not released");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("test loader interrupted", interrupted);
        }
    }
}
