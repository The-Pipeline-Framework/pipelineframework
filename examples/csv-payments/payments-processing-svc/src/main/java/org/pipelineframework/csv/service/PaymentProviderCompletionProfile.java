/*
 * Copyright (c) 2023-2025 Mariano Barcia
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

package org.pipelineframework.csv.service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Deterministic completion control for the CSV example's external provider mocks.
 *
 * <p>It keeps the provider-facing request acknowledgement independent from completion publication, so an
 * SQS poller can continue receiving requests while a configured completion burst is being assembled.
 */
final class PaymentProviderCompletionProfile<T> implements AutoCloseable {

  private final int burstSize;
  private final Duration flushDelay;
  private final ScheduledExecutorService flushExecutor;
  private final List<PendingCompletion<T>> pending = new ArrayList<>();
  private final AtomicInteger inFlight = new AtomicInteger();
  private final AtomicInteger maxInFlight = new AtomicInteger();
  private boolean flushScheduled;
  private long flushGeneration;

  PaymentProviderCompletionProfile(int burstSize, Duration flushDelay, String threadName) {
    this.burstSize = Math.max(1, burstSize);
    this.flushDelay = Objects.requireNonNull(flushDelay, "flushDelay must not be null");
    this.flushExecutor = Executors.newSingleThreadScheduledExecutor(runnable -> {
      Thread thread = new Thread(runnable, threadName);
      thread.setDaemon(true);
      return thread;
    });
  }

  CompletionStage<T> releaseWhenReady(T completion) {
    PendingCompletion<T> pendingCompletion = new PendingCompletion<>(completion);
    List<PendingCompletion<T>> toRelease = List.of();
    synchronized (this) {
      int active = inFlight.incrementAndGet();
      maxInFlight.accumulateAndGet(active, Math::max);
      pending.add(pendingCompletion);
      if (pending.size() >= burstSize) {
        toRelease = drainPending();
      } else if (!flushScheduled) {
        scheduleFlush();
      }
    }
    release(toRelease);
    return pendingCompletion.release();
  }

  void completionHandled() {
    inFlight.updateAndGet(active -> Math.max(0, active - 1));
  }

  CompletionObservation observation() {
    return new CompletionObservation(inFlight.get(), maxInFlight.get());
  }

  @Override
  public void close() {
    List<PendingCompletion<T>> toRelease;
    synchronized (this) {
      toRelease = drainPending();
    }
    release(toRelease);
    flushExecutor.shutdownNow();
  }

  private void scheduleFlush() {
    flushScheduled = true;
    long generation = ++flushGeneration;
    long delayMillis = Math.max(0L, flushDelay.toMillis());
    flushExecutor.schedule(() -> flushPending(generation), delayMillis, TimeUnit.MILLISECONDS);
  }

  private void flushPending(long generation) {
    List<PendingCompletion<T>> toRelease;
    synchronized (this) {
      if (!flushScheduled || generation != flushGeneration) {
        return;
      }
      toRelease = drainPending();
    }
    release(toRelease);
  }

  // Callers hold this instance's monitor while mutating the pending completion state.
  private List<PendingCompletion<T>> drainPending() {
    flushScheduled = false;
    flushGeneration++;
    if (pending.isEmpty()) {
      return List.of();
    }
    List<PendingCompletion<T>> drained = List.copyOf(pending);
    pending.clear();
    return drained;
  }

  private static <T> void release(List<PendingCompletion<T>> pendingCompletions) {
    pendingCompletions.forEach(pendingCompletion -> pendingCompletion.release().complete(pendingCompletion.completion()));
  }

  record CompletionObservation(int inFlight, int maxInFlight) {
  }

  private record PendingCompletion<T>(T completion, CompletableFuture<T> release) {
    private PendingCompletion(T completion) {
      this(completion, new CompletableFuture<>());
    }
  }
}
