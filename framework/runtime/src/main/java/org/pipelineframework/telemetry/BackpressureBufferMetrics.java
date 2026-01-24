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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import io.smallrye.mutiny.Multi;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Emits backpressure buffer depth metrics around Mutiny overflow buffers.
 */
public final class BackpressureBufferMetrics {

    private static final AttributeKey<String> STEP_CLASS = AttributeKey.stringKey("tpf.step.class");
    private static final String TELEMETRY_ENABLED_KEY = "pipeline.telemetry.enabled";
    private static final String METRICS_ENABLED_KEY = "pipeline.telemetry.metrics.enabled";
    private static final ConcurrentMap<String, AtomicLong> QUEUED_BY_STEP = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, AtomicLong> CAPACITY_BY_STEP = new ConcurrentHashMap<>();
    private static final AtomicBoolean GAUGES_REGISTERED = new AtomicBoolean(false);

    private BackpressureBufferMetrics() {
    }

    /**
     * Apply a buffering overflow strategy and track queued items per step.
     *
     * @param input the upstream stream
     * @param stepClass the step class owning the buffer
     * @param capacity buffer capacity
     * @param <T> item type
     * @return instrumented Multi
     */
    public static <T> Multi<T> buffer(Multi<T> input, Class<?> stepClass, int capacity) {
        int normalized = Math.max(1, capacity);
        if (!metricsEnabled() || stepClass == null) {
            return input.onOverflow().buffer(normalized);
        }

        registerGauges();

        String stepName = stepClass.getName();
        AtomicLong totalQueued = QUEUED_BY_STEP.computeIfAbsent(stepName, key -> new AtomicLong());
        CAPACITY_BY_STEP.compute(stepName, (key, value) -> {
            if (value == null) {
                return new AtomicLong(normalized);
            }
            value.set(normalized);
            return value;
        });

        AtomicLong localQueued = new AtomicLong();
        return input
            .onItem().invoke(item -> {
                localQueued.incrementAndGet();
                totalQueued.incrementAndGet();
            })
            .onOverflow().buffer(normalized)
            .onItem().invoke(item -> {
                decrement(localQueued, 1);
                decrement(totalQueued, 1);
            })
            .onTermination().invoke(() -> {
                long remaining = localQueued.getAndSet(0);
                if (remaining > 0) {
                    decrement(totalQueued, remaining);
                }
            });
    }

    private static void registerGauges() {
        if (!GAUGES_REGISTERED.compareAndSet(false, true)) {
            return;
        }
        Meter meter = GlobalOpenTelemetry.getMeter("org.pipelineframework");
        meter.gaugeBuilder("tpf.step.buffer.queued")
            .setDescription("Queued items in the backpressure buffer per step")
            .setUnit("items")
            .ofLongs()
            .buildWithCallback(BackpressureBufferMetrics::recordQueuedGauge);
        meter.gaugeBuilder("tpf.step.buffer.capacity")
            .setDescription("Configured backpressure buffer capacity per step")
            .setUnit("items")
            .ofLongs()
            .buildWithCallback(BackpressureBufferMetrics::recordCapacityGauge);
    }

    private static void recordQueuedGauge(ObservableLongMeasurement measurement) {
        QUEUED_BY_STEP.forEach((step, count) -> {
            measurement.record(count.get(), Attributes.of(STEP_CLASS, step));
        });
    }

    private static void recordCapacityGauge(ObservableLongMeasurement measurement) {
        CAPACITY_BY_STEP.forEach((step, count) -> {
            measurement.record(count.get(), Attributes.of(STEP_CLASS, step));
        });
    }

    private static boolean metricsEnabled() {
        try {
            boolean enabled = ConfigProvider.getConfig()
                .getOptionalValue(TELEMETRY_ENABLED_KEY, Boolean.class)
                .orElse(false);
            boolean metrics = ConfigProvider.getConfig()
                .getOptionalValue(METRICS_ENABLED_KEY, Boolean.class)
                .orElse(false);
            return enabled && metrics;
        } catch (Exception ignored) {
            boolean enabled = Boolean.parseBoolean(System.getProperty(TELEMETRY_ENABLED_KEY, "false"));
            boolean metrics = Boolean.parseBoolean(System.getProperty(METRICS_ENABLED_KEY, "false"));
            return enabled && metrics;
        }
    }

    private static void decrement(AtomicLong counter, long delta) {
        counter.updateAndGet(value -> {
            long updated = value - delta;
            return updated < 0 ? 0 : updated;
        });
    }
}
