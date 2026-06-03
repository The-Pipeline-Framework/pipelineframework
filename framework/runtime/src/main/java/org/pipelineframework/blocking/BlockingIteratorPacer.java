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

package org.pipelineframework.blocking;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Pacing decorator for blocking iterators.
 *
 * <p>This limits how quickly a blocking iterator is consumed. It is not reactive backpressure; it deliberately blocks
 * the worker or virtual thread that is executing the iterator bridge.
 *
 * @param <T> item type
 */
public final class BlockingIteratorPacer<T> implements CloseableIterator<T> {

    @FunctionalInterface
    interface NanoClock {
        long nanoTime();
    }

    @FunctionalInterface
    interface NanoSleeper {
        void sleepNanos(long nanos) throws InterruptedException;
    }

    private final CloseableIterator<T> delegate;
    private final long rowsPerPeriod;
    private final long periodNanos;
    private final NanoClock clock;
    private final NanoSleeper sleeper;
    private boolean windowInitialised;
    private long windowStartNanos;
    private long emittedInWindow;

    /**
     * Creates a pacing wrapper.
     *
     * @param delegate iterator being paced
     * @param rowsPerPeriod maximum rows emitted during each period
     * @param period pacing period
     */
    public BlockingIteratorPacer(
        CloseableIterator<T> delegate,
        long rowsPerPeriod,
        Duration period) {
        this(delegate, rowsPerPeriod, period, System::nanoTime, TimeUnit.NANOSECONDS::sleep);
    }

    BlockingIteratorPacer(
        CloseableIterator<T> delegate,
        long rowsPerPeriod,
        Duration period,
        NanoClock clock,
        NanoSleeper sleeper) {
        if (rowsPerPeriod <= 0) {
            throw new IllegalArgumentException("rowsPerPeriod must be positive");
        }
        long nanos = periodNanos(period);
        this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.sleeper = Objects.requireNonNull(sleeper, "sleeper must not be null");
        this.rowsPerPeriod = rowsPerPeriod;
        this.periodNanos = nanos;
    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }

    @Override
    public T next() {
        awaitPermit();
        return delegate.next();
    }

    @Override
    public void close() throws Exception {
        delegate.close();
    }

    private void awaitPermit() {
        long now = clock.nanoTime();
        if (!windowInitialised) {
            windowInitialised = true;
            windowStartNanos = now;
        }

        if (now - windowStartNanos >= periodNanos) {
            resetWindow(now);
        }

        if (emittedInWindow >= rowsPerPeriod) {
            long waitNanos = periodNanos - (now - windowStartNanos);
            if (waitNanos > 0) {
                sleep(waitNanos);
                now = clock.nanoTime();
            }
            resetWindow(now);
        }

        emittedInWindow++;
    }

    private void resetWindow(long now) {
        windowStartNanos = now;
        emittedInWindow = 0;
    }

    private void sleep(long waitNanos) {
        try {
            sleeper.sleepNanos(waitNanos);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while pacing blocking iterator", e);
        }
    }

    private static long periodNanos(Duration period) {
        Objects.requireNonNull(period, "period must not be null");
        long nanos = period.toNanos();
        if (nanos <= 0) {
            throw new IllegalArgumentException("period must be positive");
        }
        return nanos;
    }
}
