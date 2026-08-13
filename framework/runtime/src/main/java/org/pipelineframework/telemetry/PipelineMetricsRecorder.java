/*
 * Copyright (c) 2026 Mariano Barcia
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

import org.pipelineframework.telemetry.PipelineRunContext;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.pipelineframework.config.pipeline.PipelineTelemetryResourceLoader;

/** Imperative metric adapter for pipeline observations. It owns no spans or replay state. */
final class PipelineMetricsRecorder {
    private final boolean enabled;
    private final PipelineMetricAttributes attributes;
    private final ConcurrentMap<String, AtomicLong> inflightByStep = new ConcurrentHashMap<>();
    private final AtomicLong maxConcurrency = new AtomicLong();
    private final LongCounter pipelineRunCounter;
    private final LongCounter pipelineRunErrorCounter;
    private final LongCounter itemProducedCounter;
    private final LongCounter itemConsumedCounter;
    private final LongCounter sloItemThroughputTotal;
    private final LongCounter sloItemThroughputGood;
    private final LongCounter sloItemSuccessTotal;
    private final LongCounter sloItemSuccessGood;
    private final LongCounter stepErrorCounter;
    private final LongCounter stepRetryCounter;
    private final LongCounter transitionCounter;
    private final LongCounter killSwitchCounter;
    private final DoubleHistogram pipelineRunDuration;
    private final DoubleHistogram stepDuration;
    private final DoubleHistogram transitionLatency;

    PipelineMetricsRecorder(
        TelemetryPolicy policy,
        TelemetryRuntime telemetryRuntime,
        Optional<PipelineReplayTopology> replayTopology
    ) {
        enabled = policy.metricsEnabled();
        attributes = new PipelineMetricAttributes(replayTopology, PipelineTelemetryResourceLoader.loadItemBoundary());
        if (!enabled) {
            pipelineRunCounter = null;
            pipelineRunErrorCounter = null;
            itemProducedCounter = null;
            itemConsumedCounter = null;
            sloItemThroughputTotal = null;
            sloItemThroughputGood = null;
            sloItemSuccessTotal = null;
            sloItemSuccessGood = null;
            stepErrorCounter = null;
            stepRetryCounter = null;
            transitionCounter = null;
            killSwitchCounter = null;
            pipelineRunDuration = null;
            stepDuration = null;
            transitionLatency = null;
            return;
        }
        Meter meter = telemetryRuntime.meter("org.pipelineframework");
        pipelineRunCounter = counter(meter, "tpf.pipeline.run.count", "Pipeline runs", "1");
        pipelineRunErrorCounter = counter(meter, "tpf.pipeline.run.errors", "Pipeline run errors", "1");
        itemProducedCounter = counter(meter, "tpf.item.produced", "Items produced at the configured item boundary", "items");
        itemConsumedCounter = counter(meter, "tpf.item.consumed", "Items consumed at the configured item boundary", "items");
        sloItemThroughputTotal = counter(meter, "tpf.slo.item.throughput.total", "Total item throughput evaluations", "1");
        sloItemThroughputGood = counter(meter, "tpf.slo.item.throughput.good", "Item throughput evaluations meeting the threshold", "1");
        sloItemSuccessTotal = counter(meter, "tpf.slo.item.success.total", "Items evaluated for success at the configured boundary", "items");
        sloItemSuccessGood = counter(meter, "tpf.slo.item.success.good", "Items successfully produced at the configured boundary", "items");
        stepErrorCounter = counter(meter, "tpf.step.errors", "Pipeline step errors", "1");
        stepRetryCounter = counter(meter, "tpf.step.retry.count", "Pipeline step retries", "1");
        transitionCounter = counter(meter, "tpf.transition.count", "Pipeline transition emissions", "1");
        killSwitchCounter = counter(meter, "tpf.pipeline.kill_switch.triggered", "Pipeline kill switch triggers", "1");
        pipelineRunDuration = histogram(meter, "tpf.pipeline.run.duration", "Pipeline run duration");
        stepDuration = histogram(meter, "tpf.step.duration", "Pipeline step duration");
        transitionLatency = histogram(meter, "tpf.transition.latency", "Pipeline transition latency");
        meter.gaugeBuilder("tpf.step.inflight").setDescription("In-flight items per step").setUnit("items")
            .ofLongs().buildWithCallback(this::recordInflightGauge);
        meter.gaugeBuilder("tpf.pipeline.max_concurrency").setDescription("Configured max concurrency for the pipeline run")
            .setUnit("items").ofLongs().buildWithCallback(this::recordMaxConcurrencyGauge);
    }

    Attributes runAttributes(String inputKind) {
        return sdk(attributes.run(inputKind));
    }

    void runStarted(String inputKind, int configuredMaxConcurrency) {
        if (!enabled) {
            return;
        }
        pipelineRunCounter.add(1, runAttributes(inputKind));
        maxConcurrency.set(Math.max(1, configuredMaxConcurrency));
    }

    <T> Multi<T> instrumentConsumed(Class<?> stepClass, PipelineRunContext runContext, Multi<T> input) {
        if (!enabled || !isConsumer(stepClass)) {
            return input;
        }
        return input.onItem().invoke(item -> consumed(stepClass, runContext));
    }

    <T> Uni<T> instrumentConsumed(Class<?> stepClass, PipelineRunContext runContext, Uni<T> input) {
        if (!enabled || !isConsumer(stepClass)) {
            return input;
        }
        return input.onItem().invoke(item -> consumed(stepClass, runContext));
    }

    <T> Multi<T> instrumentProduced(Class<?> stepClass, PipelineRunContext runContext, Multi<T> output) {
        if (!enabled || !isProducer(stepClass)) {
            return output;
        }
        return output.onItem().invoke(item -> produced(stepClass, runContext));
    }

    <T> Uni<T> instrumentProduced(Class<?> stepClass, PipelineRunContext runContext, Uni<T> output) {
        if (!enabled || !isProducer(stepClass)) {
            return output;
        }
        return output.onItem().invoke(item -> produced(stepClass, runContext));
    }

    void stepStarted(Class<?> stepClass, PipelineRunContext runContext) {
        if (runContext == null || !runContext.enabled()) {
            return;
        }
        long current = runContext.inflightCurrent().incrementAndGet();
        runContext.inflightSamples().increment();
        runContext.inflightSum().add(current);
        runContext.inflightMax().accumulateAndGet(current, Math::max);
        if (enabled) {
            inflightByStep.computeIfAbsent(PipelineMetricAttributes.resolveStepClassName(stepClass), ignored -> new AtomicLong())
                .incrementAndGet();
        }
    }

    void stepFinished(Class<?> stepClass, PipelineRunContext runContext, long startNanos, Throwable failure, boolean cancelled) {
        if (runContext == null || !runContext.enabled()) {
            return;
        }
        long current = runContext.inflightCurrent().decrementAndGet();
        runContext.inflightSamples().increment();
        runContext.inflightSum().add(Math.max(current, 0));
        if (enabled) {
            AtomicLong stepInflight = inflightByStep.get(PipelineMetricAttributes.resolveStepClassName(stepClass));
            if (stepInflight != null) {
                stepInflight.updateAndGet(value -> Math.max(0, value - 1));
            }
            Attributes metricAttributes = sdk(attributes.step(stepClass));
            stepDuration.record(elapsedMillis(startNanos), metricAttributes);
            if (failure != null && !cancelled) {
                stepErrorCounter.add(1, metricAttributes);
            }
        }
    }

    void runFinished(PipelineRunContext runContext, Throwable failure) {
        if (!enabled) {
            return;
        }
        double duration = elapsedMillis(runContext.startNanos());
        pipelineRunDuration.record(duration, runContext.attributes());
        if (failure != null) {
            pipelineRunErrorCounter.add(1, runContext.attributes());
        }
        recordThroughputSlo(runContext, duration);
        recordItemSuccessSlo(runContext);
    }

    void retryRecorded(Class<?> stepClass) {
        if (enabled) {
            stepRetryCounter.add(1, sdk(attributes.step(stepClass)));
        }
    }

    void killSwitchTriggered(RetryAmplificationGuard.Trigger trigger) {
        if (enabled) {
            killSwitchCounter.add(1, sdk(java.util.Map.of(
                "tpf.kill_switch.reason", "retry_amplification",
                "tpf.kill_switch.step", trigger.step())));
        }
    }

    LongCounter transitionCounter() { return transitionCounter; }

    DoubleHistogram transitionLatency() { return transitionLatency; }

    private void consumed(Class<?> stepClass, PipelineRunContext runContext) {
        itemConsumedCounter.add(1, sdk(attributes.boundary(stepClass, true)));
        if (runContext != null && runContext.enabled()) {
            runContext.itemsConsumed().add(1);
        }
    }

    private void produced(Class<?> stepClass, PipelineRunContext runContext) {
        itemProducedCounter.add(1, sdk(attributes.boundary(stepClass, false)));
        if (runContext != null && runContext.enabled()) {
            runContext.itemsProduced().add(1);
        }
    }

    private boolean isProducer(Class<?> stepClass) {
        return stepClass != null && attributes.itemBoundary().map(boundary ->
            PipelineMetricAttributes.resolveStepClassName(stepClass).equals(boundary.producerStep())).orElse(false);
    }

    private boolean isConsumer(Class<?> stepClass) {
        return stepClass != null && attributes.itemBoundary().map(boundary ->
            PipelineMetricAttributes.resolveStepClassName(stepClass).equals(boundary.consumerStep())).orElse(false);
    }

    private void recordInflightGauge(ObservableLongMeasurement measurement) {
        inflightByStep.forEach((step, count) -> measurement.record(count.get(), sdk(attributes.step(step))));
    }

    private void recordMaxConcurrencyGauge(ObservableLongMeasurement measurement) {
        measurement.record(maxConcurrency.get());
    }

    private void recordThroughputSlo(PipelineRunContext runContext, double durationMs) {
        attributes.itemBoundary().ifPresent(boundary -> {
            if (durationMs <= 0d || boundary.consumerStep() == null || boundary.consumerStep().isBlank()
                || boundary.itemInputType() == null || boundary.itemInputType().isBlank()) {
                return;
            }
            long consumed = runContext.itemsConsumed().sum();
            double itemsPerMinute = consumed / (durationMs / 60_000d);
            Attributes metricAttributes = sdk(attributes.boundary(boundary.consumerStep(), boundary.itemInputType()));
            sloItemThroughputTotal.add(1, metricAttributes);
            if (itemsPerMinute >= TelemetrySloConfig.itemThroughputPerMinute()) {
                sloItemThroughputGood.add(1, metricAttributes);
            }
        });
    }

    private void recordItemSuccessSlo(PipelineRunContext runContext) {
        attributes.itemBoundary().ifPresent(boundary -> {
            if (boundary.consumerStep() == null || boundary.consumerStep().isBlank()
                || boundary.itemInputType() == null || boundary.itemInputType().isBlank()) {
                return;
            }
            long consumed = runContext.itemsConsumed().sum();
            if (consumed <= 0) {
                return;
            }
            long goodCount = Math.min(consumed, runContext.itemsProduced().sum());
            Attributes metricAttributes = sdk(attributes.boundary(boundary.consumerStep(), boundary.itemInputType()));
            sloItemSuccessTotal.add(consumed, metricAttributes);
            if (goodCount > 0) {
                sloItemSuccessGood.add(goodCount, metricAttributes);
            }
        });
    }

    private static LongCounter counter(Meter meter, String name, String description, String unit) {
        return meter.counterBuilder(name).setDescription(description).setUnit(unit).build();
    }

    private static DoubleHistogram histogram(Meter meter, String name, String description) {
        return meter.histogramBuilder(name).setDescription(description).setUnit("ms").build();
    }

    private static Attributes sdk(java.util.Map<String, String> values) {
        return TelemetrySdkAttributes.from(values);
    }

    private static double elapsedMillis(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000d;
    }
}
