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

package org.pipelineframework.telemetry;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

/** Runtime safety owner for retry amplification. It consumes runtime facts, not exported metrics. */
final class RetryAmplificationGuardRuntime {
    private final boolean enabled;
    private final Duration window;
    private final double slopeThreshold;
    private final int sustainSamples;
    private final Duration sampleInterval;
    private final ScheduledExecutorService scheduler;
    private final RetryAmplificationGuard evaluator = new RetryAmplificationGuard();
    private final ConcurrentMap<String, AtomicLong> inflightByStep = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, LongAdder> retriesByStep = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Monitor> monitors = new ConcurrentHashMap<>();

    RetryAmplificationGuardRuntime(TelemetryPolicy policy) {
        enabled = policy.retryAmplificationEnabled();
        window = policy.retryAmplificationWindow();
        slopeThreshold = policy.retryAmplificationInflightSlopeThreshold();
        sustainSamples = policy.retryAmplificationSustainSamples();
        sampleInterval = sampleInterval(window);
        scheduler = enabled ? Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "tpf-retry-amplification-guard");
            thread.setDaemon(true);
            return thread;
        }) : null;
    }

    boolean enabled() { return enabled; }
    Duration sampleInterval() { return sampleInterval; }

    void itemStarted(String step) {
        if (enabled && step != null) inflightByStep.computeIfAbsent(step, ignored -> new AtomicLong()).incrementAndGet();
    }

    void itemEnded(String step) {
        if (!enabled || step == null) return;
        AtomicLong count = inflightByStep.get(step);
        if (count != null) count.updateAndGet(value -> Math.max(0L, value - 1));
    }

    void retryRecorded(String step) {
        if (enabled && step != null) retriesByStep.computeIfAbsent(step, ignored -> new LongAdder()).increment();
    }

    void runStarted(String runId, Consumer<RetryAmplificationGuard.Trigger> triggerConsumer) {
        if (!enabled || scheduler == null || runId == null) return;
        Monitor monitor = new Monitor(runId, triggerConsumer);
        monitors.put(runId, monitor);
        monitor.start();
    }

    void runFinished(String runId) {
        Monitor monitor = runId == null ? null : monitors.remove(runId);
        if (monitor != null) monitor.stop();
    }

    Optional<RetryAmplificationGuard.Trigger> trigger(String runId) {
        Monitor monitor = runId == null ? null : monitors.get(runId);
        return monitor == null ? Optional.empty() : monitor.triggered();
    }

    Optional<RetryAmplificationGuard.Trigger> trigger() {
        return monitors.values().stream().map(Monitor::triggered).flatMap(Optional::stream).findFirst();
    }

    void shutdown() {
        monitors.values().forEach(Monitor::stop);
        monitors.clear();
        if (scheduler != null) scheduler.shutdownNow();
    }

    private static Duration sampleInterval(Duration window) {
        long millis = window == null ? 30_000L : Math.max(1_000L, window.toMillis());
        return Duration.ofMillis(Math.max(1_000L, Math.min(millis / 6, 5_000L)));
    }

    private final class Monitor {
        private final String runId;
        private final Consumer<RetryAmplificationGuard.Trigger> triggerConsumer;
        private final Deque<RetryAmplificationGuard.Sample> samples = new ArrayDeque<>();
        private final AtomicLong retryBaseline = new AtomicLong(-1L);
        private final AtomicReference<RetryAmplificationGuard.Trigger> trigger = new AtomicReference<>();
        private final AtomicInteger consecutiveBreaches = new AtomicInteger();
        private ScheduledFuture<?> future;

        private Monitor(String runId, Consumer<RetryAmplificationGuard.Trigger> triggerConsumer) {
            this.runId = runId;
            this.triggerConsumer = triggerConsumer;
        }
        void start() { future = scheduler.scheduleAtFixedRate(this::sample, 0L, sampleInterval.toMillis(), TimeUnit.MILLISECONDS); }
        void stop() { if (future != null) future.cancel(false); }
        Optional<RetryAmplificationGuard.Trigger> triggered() { return Optional.ofNullable(trigger.get()); }

        private void sample() {
            if (trigger.get() != null) return;
            long retries = retriesByStep.values().stream().mapToLong(LongAdder::sum).sum();
            long baseline = retryBaseline.updateAndGet(value -> value < 0 ? retries : value);
            long inflight = inflightByStep.values().stream().mapToLong(AtomicLong::get).sum();
            Deque<RetryAmplificationGuard.Sample> snapshot;
            synchronized (samples) {
                samples.addLast(new RetryAmplificationGuard.Sample(System.nanoTime(), inflight, Math.max(0L, retries - baseline)));
                int maximum = Math.max(2, (int) (window.toMillis() / Math.max(1L, sampleInterval.toMillis())) + 2);
                while (samples.size() > maximum) samples.removeFirst();
                snapshot = new ArrayDeque<>(samples);
            }
            evaluator.evaluate("global", snapshot, window, slopeThreshold, sustainSamples).ifPresent(candidate -> {
                if (consecutiveBreaches.incrementAndGet() >= sustainSamples && trigger.compareAndSet(null, candidate)) {
                    stop();
                    triggerConsumer.accept(candidate);
                }
            });
        }
    }
}
