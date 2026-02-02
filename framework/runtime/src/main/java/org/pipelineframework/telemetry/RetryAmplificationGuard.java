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
import java.util.Deque;
import java.util.Optional;

/**
 * Evaluates retry amplification conditions based on sampled metrics.
 */
public final class RetryAmplificationGuard {

    /**
     * Sampled values for a step at a point in time.
     *
     * @param timestampNanos sample timestamp in nanoseconds
     * @param inflight in-flight items
     * @param retries retry count
     */
    public record Sample(long timestampNanos, long inflight, long retries) {
    }

    /**
     * Metadata describing a guard trigger.
     *
     * @param step step class name
     * @param inflightSlope computed inflight slope (items/sec)
     * @param retryRate computed retry rate (retries/sec)
     * @param inflightSlopeThreshold inflight slope threshold
     * @param window evaluation window
     * @param sustainSamples consecutive samples required to trigger
     */
    public record Trigger(
        String step,
        double inflightSlope,
        double retryRate,
        double inflightSlopeThreshold,
        Duration window,
        int sustainSamples) {
    }

    /**
     * Evaluate inflight growth for a step based on sampled data.
     *
     * @param step step class name
     * @param samples sampled values (oldest to newest)
     * @param window evaluation window
     * @param inflightSlopeThreshold inflight slope threshold
     * @return trigger metadata when thresholds are exceeded
     */
    public Optional<Trigger> evaluate(
        String step,
        Deque<Sample> samples,
        Duration window,
        double inflightSlopeThreshold,
        int sustainSamples) {
        if (samples == null || samples.size() < 2 || window == null) {
            return Optional.empty();
        }
        Sample last = samples.peekLast();
        if (last == null) {
            return Optional.empty();
        }
        long windowNanos = Math.max(0L, window.toNanos());
        Sample first = findWindowStart(samples, last.timestampNanos() - windowNanos);
        if (first == null) {
            return Optional.empty();
        }
        long durationNanos = last.timestampNanos() - first.timestampNanos();
        if (durationNanos <= 0L) {
            return Optional.empty();
        }
        double seconds = durationNanos / 1_000_000_000d;
        double inflightSlope = (last.inflight() - first.inflight()) / seconds;
        double retryRate = (last.retries() - first.retries()) / seconds;
        if (inflightSlope > inflightSlopeThreshold) {
            return Optional.of(new Trigger(
                step,
                inflightSlope,
                retryRate,
                inflightSlopeThreshold,
                window,
                sustainSamples));
        }
        return Optional.empty();
    }

    private Sample findWindowStart(Deque<Sample> samples, long cutoffNanos) {
        Sample candidate = null;
        for (Sample sample : samples) {
            if (sample.timestampNanos() <= cutoffNanos) {
                candidate = sample;
            } else {
                break;
            }
        }
        return candidate;
    }
}
