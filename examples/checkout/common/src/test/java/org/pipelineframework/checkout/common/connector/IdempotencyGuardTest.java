package org.pipelineframework.checkout.common.connector;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IdempotencyGuardTest {

    @Test
    void constructorRejectsZeroMaxKeys() {
        assertThrows(IllegalArgumentException.class, () -> new IdempotencyGuard(0));
    }

    @Test
    void constructorRejectsNegativeMaxKeys() {
        assertThrows(IllegalArgumentException.class, () -> new IdempotencyGuard(-1));
        assertThrows(IllegalArgumentException.class, () -> new IdempotencyGuard(-100));
    }

    @Test
    void markIfNewReturnsTrueForFirstOccurrence() {
        IdempotencyGuard guard = new IdempotencyGuard(10);
        assertTrue(guard.markIfNew("key1"));
    }

    @Test
    void markIfNewReturnsFalseForSecondOccurrence() {
        IdempotencyGuard guard = new IdempotencyGuard(10);
        guard.markIfNew("key1");
        assertFalse(guard.markIfNew("key1"));
    }

    @Test
    void markIfNewReturnsFalseForMultipleOccurrences() {
        IdempotencyGuard guard = new IdempotencyGuard(10);
        guard.markIfNew("key1");
        assertFalse(guard.markIfNew("key1"));
        assertFalse(guard.markIfNew("key1"));
        assertFalse(guard.markIfNew("key1"));
    }

    @Test
    void markIfNewRejectsNullKey() {
        IdempotencyGuard guard = new IdempotencyGuard(10);
        assertThrows(NullPointerException.class, () -> guard.markIfNew(null));
    }

    @Test
    void markIfNewRejectsBlankKey() {
        IdempotencyGuard guard = new IdempotencyGuard(10);
        assertThrows(IllegalArgumentException.class, () -> guard.markIfNew(""));
        assertThrows(IllegalArgumentException.class, () -> guard.markIfNew("   "));
    }

    @Test
    void containsReturnsTrueForMarkedKey() {
        IdempotencyGuard guard = new IdempotencyGuard(10);
        guard.markIfNew("key1");
        assertTrue(guard.contains("key1"));
    }

    @Test
    void containsReturnsFalseForUnmarkedKey() {
        IdempotencyGuard guard = new IdempotencyGuard(10);
        assertFalse(guard.contains("key1"));
    }

    @Test
    void containsRejectsNullKey() {
        IdempotencyGuard guard = new IdempotencyGuard(10);
        assertThrows(NullPointerException.class, () -> guard.contains(null));
    }

    @Test
    void containsRejectsBlankKey() {
        IdempotencyGuard guard = new IdempotencyGuard(10);
        assertThrows(IllegalArgumentException.class, () -> guard.contains(""));
        assertThrows(IllegalArgumentException.class, () -> guard.contains("   "));
    }

    @Test
    void sizeReturnsZeroInitially() {
        IdempotencyGuard guard = new IdempotencyGuard(10);
        assertEquals(0, guard.size());
    }

    @Test
    void sizeIncrementsWhenMarkingNewKeys() {
        IdempotencyGuard guard = new IdempotencyGuard(10);
        guard.markIfNew("key1");
        assertEquals(1, guard.size());
        guard.markIfNew("key2");
        assertEquals(2, guard.size());
        guard.markIfNew("key3");
        assertEquals(3, guard.size());
    }

    @Test
    void sizeDoesNotIncrementForDuplicateKeys() {
        IdempotencyGuard guard = new IdempotencyGuard(10);
        guard.markIfNew("key1");
        assertEquals(1, guard.size());
        guard.markIfNew("key1");
        assertEquals(1, guard.size());
        guard.markIfNew("key1");
        assertEquals(1, guard.size());
    }

    @Test
    void guardEnforcesMaxKeysLimitViaLruEviction() {
        IdempotencyGuard guard = new IdempotencyGuard(3);
        guard.markIfNew("key1");
        guard.markIfNew("key2");
        guard.markIfNew("key3");
        assertEquals(3, guard.size());

        guard.markIfNew("key4");
        assertEquals(3, guard.size());
        assertFalse(guard.contains("key1"), "Eldest entry (key1) should have been evicted");
        assertTrue(guard.contains("key2"));
        assertTrue(guard.contains("key3"));
        assertTrue(guard.contains("key4"));
    }

    @Test
    void guardEvictsLeastRecentlyAccessedEntry() {
        IdempotencyGuard guard = new IdempotencyGuard(3);
        guard.markIfNew("key1");
        guard.markIfNew("key2");
        guard.markIfNew("key3");

        guard.contains("key1");

        guard.markIfNew("key4");
        assertFalse(guard.contains("key2"), "key2 should have been evicted as least recently accessed");
        assertTrue(guard.contains("key1"), "key1 was accessed so should still be present");
        assertTrue(guard.contains("key3"));
        assertTrue(guard.contains("key4"));
    }

    @Test
    void markIfNewOnDuplicateTouchesEntryForLru() {
        IdempotencyGuard guard = new IdempotencyGuard(2);
        guard.markIfNew("key1");
        guard.markIfNew("key2");

        guard.markIfNew("key1");

        guard.markIfNew("key3");
        assertFalse(guard.contains("key2"), "key2 should have been evicted");
        assertTrue(guard.contains("key1"), "key1 was touched and should remain");
        assertTrue(guard.contains("key3"));
    }

    @Test
    void snapshotReturnsEmptyMapInitially() {
        IdempotencyGuard guard = new IdempotencyGuard(10);
        Map<String, Boolean> snapshot = guard.snapshot();
        assertTrue(snapshot.isEmpty());
    }

    @Test
    void snapshotReturnsAllMarkedKeys() {
        IdempotencyGuard guard = new IdempotencyGuard(10);
        guard.markIfNew("key1");
        guard.markIfNew("key2");
        guard.markIfNew("key3");

        Map<String, Boolean> snapshot = guard.snapshot();
        assertEquals(3, snapshot.size());
        assertEquals(Boolean.TRUE, snapshot.get("key1"));
        assertEquals(Boolean.TRUE, snapshot.get("key2"));
        assertEquals(Boolean.TRUE, snapshot.get("key3"));
    }

    @Test
    void snapshotReturnsImmutableCopy() {
        IdempotencyGuard guard = new IdempotencyGuard(10);
        guard.markIfNew("key1");
        Map<String, Boolean> snapshot = guard.snapshot();

        assertThrows(UnsupportedOperationException.class, () -> snapshot.put("key2", Boolean.TRUE));
    }

    @Test
    void snapshotDoesNotReflectSubsequentChanges() {
        IdempotencyGuard guard = new IdempotencyGuard(10);
        guard.markIfNew("key1");
        Map<String, Boolean> snapshot = guard.snapshot();

        guard.markIfNew("key2");
        assertEquals(1, snapshot.size());
        assertFalse(snapshot.containsKey("key2"));
    }

    @Test
    void guardIsThreadSafe() throws InterruptedException {
        IdempotencyGuard guard = new IdempotencyGuard(10000);
        int numThreads = 10;
        int keysPerThread = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);
        AtomicInteger firstOccurrences = new AtomicInteger(0);

        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < keysPerThread; i++) {
                        String key = "key-" + (threadId * keysPerThread + i);
                        if (guard.markIfNew(key)) {
                            firstOccurrences.incrementAndGet();
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

        assertEquals(numThreads * keysPerThread, firstOccurrences.get());
        assertEquals(numThreads * keysPerThread, guard.size());
    }

    @Test
    void guardHandlesConcurrentDuplicates() throws InterruptedException {
        IdempotencyGuard guard = new IdempotencyGuard(100);
        int numThreads = 20;
        int attemptsPerThread = 100;
        String sharedKey = "shared-key";
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int t = 0; t < numThreads; t++) {
            executor.submit(() -> {
                try {
                    for (int i = 0; i < attemptsPerThread; i++) {
                        if (guard.markIfNew(sharedKey)) {
                            successCount.incrementAndGet();
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

        assertEquals(1, successCount.get(), "Only one thread should have successfully marked the shared key as new");
        assertTrue(guard.contains(sharedKey));
    }

    @Test
    void guardHandlesLruEvictionUnderConcurrentLoad() throws InterruptedException {
        IdempotencyGuard guard = new IdempotencyGuard(100);
        int numThreads = 10;
        int keysPerThread = 50;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);

        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < keysPerThread; i++) {
                        guard.markIfNew("thread-" + threadId + "-key-" + i);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

        assertEquals(100, guard.size(), "Guard should maintain max capacity of 100 keys");
    }

    @Test
    void guardDistinguishesBetweenSimilarKeys() {
        IdempotencyGuard guard = new IdempotencyGuard(10);
        assertTrue(guard.markIfNew("key"));
        assertTrue(guard.markIfNew("key "));
        assertTrue(guard.markIfNew(" key"));
        assertTrue(guard.markIfNew("key1"));
        assertEquals(4, guard.size());
    }

    @Test
    void guardHandlesLargeMaxKeysValue() {
        IdempotencyGuard guard = new IdempotencyGuard(1000000);
        for (int i = 0; i < 1000; i++) {
            assertTrue(guard.markIfNew("key-" + i));
        }
        assertEquals(1000, guard.size());
    }
}