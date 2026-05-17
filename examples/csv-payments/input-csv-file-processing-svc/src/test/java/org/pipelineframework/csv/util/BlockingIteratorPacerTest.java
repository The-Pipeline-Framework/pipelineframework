/*
 * Copyright (c) 2023-2026 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework.csv.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.blocking.CloseableIterator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BlockingIteratorPacerTest {

    @AfterEach
    void clearInterruptFlag() {
        Thread.interrupted();
    }

    @Test
    void emitsFirstRowsPerPeriodWithoutDelay() {
        FakeClock clock = new FakeClock();
        RecordingSleeper sleeper = new RecordingSleeper(clock);
        BlockingIteratorPacer<String> pacer =
                new BlockingIteratorPacer<>(iteratorOf("a", "b", "c"), 2, 100, clock::nanoTime, sleeper::sleepNanos);

        assertEquals("a", pacer.next());
        assertEquals("b", pacer.next());

        assertTrue(sleeper.sleeps().isEmpty());
    }

    @Test
    void blocksBeforeFirstRecordBeyondConfiguredPeriod() {
        FakeClock clock = new FakeClock();
        RecordingSleeper sleeper = new RecordingSleeper(clock);
        BlockingIteratorPacer<String> pacer =
                new BlockingIteratorPacer<>(iteratorOf("a", "b", "c"), 2, 100, clock::nanoTime, sleeper::sleepNanos);

        assertEquals("a", pacer.next());
        assertEquals("b", pacer.next());
        assertEquals("c", pacer.next());

        assertEquals(List.of(TimeUnit.MILLISECONDS.toNanos(100)), sleeper.sleeps());
    }

    @Test
    void resetsPermitsAfterPeriod() {
        FakeClock clock = new FakeClock();
        RecordingSleeper sleeper = new RecordingSleeper(clock);
        BlockingIteratorPacer<String> pacer =
                new BlockingIteratorPacer<>(iteratorOf("a", "b"), 1, 100, clock::nanoTime, sleeper::sleepNanos);

        assertEquals("a", pacer.next());
        clock.advanceNanos(TimeUnit.MILLISECONDS.toNanos(100));
        assertEquals("b", pacer.next());

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

        new BlockingIteratorPacer<>(delegate, 1, 100).close();

        assertTrue(closed.get());
    }

    @Test
    void rejectsInvalidConfiguration() {
        CloseableIterator<String> delegate = iteratorOf("a");

        assertThrows(IllegalArgumentException.class, () -> new BlockingIteratorPacer<>(delegate, 0, 100));
        assertThrows(IllegalArgumentException.class, () -> new BlockingIteratorPacer<>(delegate, 1, 0));
        assertThrows(IllegalArgumentException.class, () -> new BlockingIteratorPacer<>(null, 1, 100));
    }

    @Test
    void interruptionRestoresInterruptStatusAndFailsIteration() {
        FakeClock clock = new FakeClock();
        BlockingIteratorPacer.NanoSleeper sleeper = ignored -> {
            throw new InterruptedException("stop");
        };
        BlockingIteratorPacer<String> pacer =
                new BlockingIteratorPacer<>(iteratorOf("a", "b"), 1, 100, clock::nanoTime, sleeper);

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
