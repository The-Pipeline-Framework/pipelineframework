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

import jakarta.inject.Singleton;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import io.smallrye.mutiny.Multi;
import org.eclipse.microprofile.config.ConfigProvider;
import org.pipelineframework.config.pipeline.PipelineTelemetryResourceLoader;

/**
 * Emits backpressure buffer depth metrics around Mutiny overflow buffers.
 */
@Singleton
final class BackpressureBufferMetricsRecorder {

    private final AttributeKey<String> STEP_CLASS = AttributeKey.stringKey("tpf.step.class");
    private final AttributeKey<String> STEP_PARENT = AttributeKey.stringKey("tpf.step.parent");
    private final AttributeKey<String> PIPELINE = AttributeKey.stringKey("tpf.pipeline");
    private final AttributeKey<String> STEP = AttributeKey.stringKey("tpf.step");
    private final AttributeKey<String> SERVICE = AttributeKey.stringKey("tpf.service");
    private final AttributeKey<String> CARDINALITY = AttributeKey.stringKey("tpf.cardinality");
    private final String TELEMETRY_ENABLED_KEY = "pipeline.telemetry.enabled";
    private final String METRICS_ENABLED_KEY = "pipeline.telemetry.metrics.enabled";
    private final ConcurrentMap<String, AtomicLong> QUEUED_BY_STEP = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, AtomicLong> CAPACITY_BY_STEP = new ConcurrentHashMap<>();
    private final AtomicBoolean GAUGES_REGISTERED = new AtomicBoolean(false);
    private volatile Map<String, String> STEP_PARENTS;
    private volatile Map<String, PipelineReplayTopology.Step> TOPOLOGY_STEPS;
    private volatile String PIPELINE_NAME;

    BackpressureBufferMetricsRecorder() {
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
    public <T> Multi<T> buffer(Multi<T> input, Class<?> stepClass, int capacity) {
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

    private void registerGauges() {
        if (!GAUGES_REGISTERED.compareAndSet(false, true)) {
            return;
        }
        Meter meter = TelemetryCompatibilityAccess.metricsRuntime().meter("org.pipelineframework");
        meter.gaugeBuilder("tpf.step.buffer.queued")
            .setDescription("Queued items in the backpressure buffer per step")
            .setUnit("items")
            .ofLongs()
            .buildWithCallback(this::recordQueuedGauge);
        meter.gaugeBuilder("tpf.step.buffer.capacity")
            .setDescription("Configured backpressure buffer capacity per step")
            .setUnit("items")
            .ofLongs()
            .buildWithCallback(this::recordCapacityGauge);
    }

    private void recordQueuedGauge(ObservableLongMeasurement measurement) {
        QUEUED_BY_STEP.forEach((step, count) -> {
            measurement.record(count.get(), attributes(step));
        });
    }

    private void recordCapacityGauge(ObservableLongMeasurement measurement) {
        CAPACITY_BY_STEP.forEach((step, count) -> {
            measurement.record(count.get(), attributes(step));
        });
    }

    private Attributes attributes(String stepClassName) {
        String normalizedStepClass = normalizeStepClassName(stepClassName);
        AttributesBuilder builder = Attributes.builder()
            .put(STEP_CLASS, normalizedStepClass)
            .put(STEP_PARENT, resolveStepParent(normalizedStepClass));
        PipelineReplayTopology.Step descriptor = resolveTopologySteps().get(normalizedStepClass);
        if (descriptor != null) {
            String pipeline = resolvePipelineName();
            if (pipeline != null && !pipeline.isBlank()) {
                builder.put(PIPELINE, pipeline);
            }
            if (descriptor.step() != null) {
                builder.put(STEP, descriptor.step());
            }
            if (descriptor.service() != null) {
                builder.put(SERVICE, descriptor.service());
            }
            if (descriptor.cardinality() != null) {
                builder.put(CARDINALITY, descriptor.cardinality());
            }
        }
        return builder.build();
    }

    private String resolveStepParent(String stepClassName) {
        Map<String, String> parents = STEP_PARENTS;
        if (parents == null) {
            synchronized (BackpressureBufferMetrics.class) {
                if (STEP_PARENTS == null) {
                    STEP_PARENTS = PipelineTelemetryResourceLoader.loadItemBoundary()
                        .map(PipelineTelemetryResourceLoader.ItemBoundary::stepParents)
                        .orElse(Map.of());
                }
                parents = STEP_PARENTS;
            }
        }
        return parents.getOrDefault(stepClassName, stepClassName);
    }

    private String normalizeStepClassName(String stepClassName) {
        if (stepClassName == null || stepClassName.isBlank()) {
            return stepClassName;
        }
        if ((stepClassName.contains("_Subclass") || stepClassName.contains("$$") || stepClassName.contains("_ClientProxy"))
            && stepClassName.contains(".")) {
            int proxyIndex = stepClassName.indexOf("$$");
            if (proxyIndex < 0) {
                proxyIndex = stepClassName.indexOf("_Subclass");
            }
            if (proxyIndex < 0) {
                proxyIndex = stepClassName.indexOf("_ClientProxy");
            }
            if (proxyIndex > 0) {
                return stepClassName.substring(0, proxyIndex);
            }
        }
        return stepClassName;
    }

    private Map<String, PipelineReplayTopology.Step> resolveTopologySteps() {
        Map<String, PipelineReplayTopology.Step> steps = TOPOLOGY_STEPS;
        if (steps == null) {
            synchronized (BackpressureBufferMetrics.class) {
                if (TOPOLOGY_STEPS == null) {
                    PipelineReplayTopology topology = PipelineReplayTopologyLoader.load().orElse(null);
                    TOPOLOGY_STEPS = topology == null ? Map.of() : topology.stepsByRuntimeClass();
                    PIPELINE_NAME = topology == null ? null : topology.pipeline();
                }
                steps = TOPOLOGY_STEPS;
            }
        }
        return steps;
    }

    private String resolvePipelineName() {
        resolveTopologySteps();
        return PIPELINE_NAME;
    }

    private boolean metricsEnabled() {
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

    private void decrement(AtomicLong counter, long delta) {
        counter.updateAndGet(value -> {
            long updated = value - delta;
            return updated < 0 ? 0 : updated;
        });
    }
}
