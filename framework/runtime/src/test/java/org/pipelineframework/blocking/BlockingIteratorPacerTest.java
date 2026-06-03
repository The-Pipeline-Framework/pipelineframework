package org.pipelineframework.blocking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class BlockingIteratorPacerTest {

    @AfterEach
    void clearInterruptFlag() {
        Thread.interrupted();
    }

    @Test
    void emitsFirstRowsPerPeriodWithoutDelay() {
        FakeClock clock = new FakeClock();
        RecordingSleeper sleeper = new RecordingSleeper(clock);
        BlockingIteratorPacer<String> pacer = new BlockingIteratorPacer<>(
            iteratorOf("a", "b", "c"),
            2,
            Duration.ofMillis(100),
            clock::nanoTime,
            sleeper::sleepNanos);

        assertEquals("a", pacer.next());
        assertEquals("b", pacer.next());

        assertTrue(sleeper.sleeps().isEmpty());
    }

    @Test
    void blocksBeforeFirstRecordBeyondConfiguredPeriod() {
        FakeClock clock = new FakeClock();
        RecordingSleeper sleeper = new RecordingSleeper(clock);
        BlockingIteratorPacer<String> pacer = new BlockingIteratorPacer<>(
            iteratorOf("a", "b", "c"),
            2,
            Duration.ofMillis(100),
            clock::nanoTime,
            sleeper::sleepNanos);

        assertEquals("a", pacer.next());
        assertEquals("b", pacer.next());
        assertEquals("c", pacer.next());

        assertEquals(List.of(TimeUnit.MILLISECONDS.toNanos(100)), sleeper.sleeps());
    }

    @Test
    void resetsPermitsAfterPeriod() {
        FakeClock clock = new FakeClock();
        RecordingSleeper sleeper = new RecordingSleeper(clock);
        BlockingIteratorPacer<String> pacer = new BlockingIteratorPacer<>(
            iteratorOf("a", "b"),
            1,
            Duration.ofMillis(100),
            clock::nanoTime,
            sleeper::sleepNanos);

        assertEquals("a", pacer.next());
        clock.advanceNanos(TimeUnit.MILLISECONDS.toNanos(100));
        assertEquals("b", pacer.next());

        assertTrue(sleeper.sleeps().isEmpty());
    }

    @Test
    void hasNextDelegatesWithoutPacing() {
        AtomicInteger hasNextCalls = new AtomicInteger();
        FakeClock clock = new FakeClock();
        RecordingSleeper sleeper = new RecordingSleeper(clock);
        CloseableIterator<String> delegate = new CloseableIterator<>() {
            @Override
            public boolean hasNext() {
                hasNextCalls.incrementAndGet();
                return true;
            }

            @Override
            public String next() {
                return "next";
            }

            @Override
            public void close() {
            }
        };
        BlockingIteratorPacer<String> pacer = new BlockingIteratorPacer<>(
            delegate,
            1,
            Duration.ofMillis(100),
            clock::nanoTime,
            sleeper::sleepNanos);

        assertTrue(pacer.hasNext());
        assertTrue(pacer.hasNext());

        assertEquals(2, hasNextCalls.get());
        assertTrue(sleeper.sleeps().isEmpty());
    }

    @Test
    void closesDelegate() throws Exception {
        AtomicBoolean closed = new AtomicBoolean(false);
        CloseableIterator<String> delegate = new CloseableIterator<>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public String next() {
                throw new IllegalStateException("no items");
            }

            @Override
            public void close() {
                closed.set(true);
            }
        };

        new BlockingIteratorPacer<>(delegate, 1, Duration.ofMillis(100)).close();

        assertTrue(closed.get());
    }

    @Test
    void propagatesCloseFailure() {
        Exception closeFailure = new Exception("close failed");
        CloseableIterator<String> delegate = new CloseableIterator<>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public String next() {
                throw new IllegalStateException("no items");
            }

            @Override
            public void close() throws Exception {
                throw closeFailure;
            }
        };
        BlockingIteratorPacer<String> pacer = new BlockingIteratorPacer<>(delegate, 1, Duration.ofMillis(100));

        Exception failure = assertThrows(Exception.class, pacer::close);

        assertSame(closeFailure, failure);
    }

    @Test
    void rejectsInvalidConfiguration() {
        CloseableIterator<String> delegate = iteratorOf("a");

        assertThrows(IllegalArgumentException.class, () -> new BlockingIteratorPacer<>(delegate, 0, Duration.ofMillis(100)));
        assertThrows(IllegalArgumentException.class, () -> new BlockingIteratorPacer<>(delegate, 1, Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> new BlockingIteratorPacer<>(delegate, 1, Duration.ofNanos(-1)));
        assertThrows(NullPointerException.class, () -> new BlockingIteratorPacer<>(null, 1, Duration.ofMillis(100)));
        assertThrows(NullPointerException.class, () -> new BlockingIteratorPacer<>(delegate, 1, null));
    }

    @Test
    void interruptionRestoresInterruptStatusAndFailsIteration() {
        FakeClock clock = new FakeClock();
        BlockingIteratorPacer.NanoSleeper sleeper = ignored -> {
            throw new InterruptedException("stop");
        };
        BlockingIteratorPacer<String> pacer = new BlockingIteratorPacer<>(
            iteratorOf("a", "b"),
            1,
            Duration.ofMillis(100),
            clock::nanoTime,
            sleeper);

        assertEquals("a", pacer.next());
        RuntimeException failure = assertThrows(RuntimeException.class, pacer::next);

        assertTrue(Thread.currentThread().isInterrupted());
        assertEquals("Interrupted while pacing blocking iterator", failure.getMessage());
        assertInstanceOf(InterruptedException.class, failure.getCause());
    }

    @SafeVarargs
    private static <T> CloseableIterator<T> iteratorOf(T... items) {
        Iterator<T> iterator = List.of(items).iterator();
        return new CloseableIterator<>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public T next() {
                return iterator.next();
            }

            @Override
            public void close() {
            }
        };
    }

    private static final class FakeClock {
        private long now;

        private long nanoTime() {
            return now;
        }

        private void advanceNanos(long nanos) {
            now += nanos;
        }
    }

    private static final class RecordingSleeper {
        private final FakeClock clock;
        private final List<Long> sleeps = new ArrayList<>();

        private RecordingSleeper(FakeClock clock) {
            this.clock = clock;
        }

        private void sleepNanos(long nanos) {
            assertFalse(Thread.currentThread().isInterrupted());
            sleeps.add(nanos);
            clock.advanceNanos(nanos);
        }

        private List<Long> sleeps() {
            return sleeps;
        }
    }
}
