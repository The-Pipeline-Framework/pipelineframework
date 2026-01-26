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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.quarkus.arc.Unremovable;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.config.ParallelismPolicy;
import org.pipelineframework.config.PipelineStepConfig;
import org.pipelineframework.config.pipeline.PipelineTelemetryResourceLoader;

/**
 * Records pipeline-level spans and metrics for step execution.
 */
@ApplicationScoped
@Unremovable
public class PipelineTelemetry {

    private static final AttributeKey<String> INPUT_KIND = AttributeKey.stringKey("tpf.input");
    private static final AttributeKey<String> STEP_CLASS = AttributeKey.stringKey("tpf.step.class");
    private static final AttributeKey<String> STEP_PARENT = AttributeKey.stringKey("tpf.step.parent");
    private static final AttributeKey<String> ITEM_TYPE = AttributeKey.stringKey("tpf.item.type");
    private static final AttributeKey<Long> PARALLEL_MAX_IN_FLIGHT =
        AttributeKey.longKey("tpf.parallel.max_in_flight");
    private static final AttributeKey<Double> PARALLEL_AVG_IN_FLIGHT =
        AttributeKey.doubleKey("tpf.parallel.avg_in_flight");

    private final boolean enabled;
    private final boolean metricsEnabled;
    private final boolean tracingEnabled;
    private final boolean perItemSpans;
    private final Tracer tracer;
    private final Meter meter;
    private final LongCounter pipelineRunCounter;
    private final LongCounter pipelineRunErrorCounter;
    private final LongCounter itemProducedCounter;
    private final LongCounter itemConsumedCounter;
    private final LongCounter sloItemThroughputTotal;
    private final LongCounter sloItemThroughputGood;
    private final LongCounter stepErrorCounter;
    private final DoubleHistogram pipelineRunDuration;
    private final DoubleHistogram stepDuration;
    private final ConcurrentMap<String, AtomicLong> inflightByStep;
    private final AtomicLong maxConcurrency;
    private final PipelineTelemetryResourceLoader.ItemBoundary itemBoundary;
    private final Map<String, String> stepParents;

    /**
     * Create a telemetry helper from the configured pipeline settings.
     *
     * @param stepConfig pipeline configuration mapping
     */
    @Inject
    public PipelineTelemetry(PipelineStepConfig stepConfig) {
        PipelineStepConfig.TelemetryConfig telemetry = stepConfig.telemetry();
        this.enabled = telemetry != null && Boolean.TRUE.equals(telemetry.enabled());
        this.tracingEnabled = enabled && Boolean.TRUE.equals(telemetry.tracing().enabled());
        this.perItemSpans = tracingEnabled && Boolean.TRUE.equals(telemetry.tracing().perItem());
        this.metricsEnabled = enabled && Boolean.TRUE.equals(telemetry.metrics().enabled());
        this.tracer = GlobalOpenTelemetry.getTracer("org.pipelineframework");
        this.meter = GlobalOpenTelemetry.getMeter("org.pipelineframework");
        this.inflightByStep = new ConcurrentHashMap<>();
        this.maxConcurrency = new AtomicLong();
        this.itemBoundary = PipelineTelemetryResourceLoader.loadItemBoundary().orElse(null);
        this.stepParents = itemBoundary != null ? itemBoundary.stepParents() : Map.of();
        if (metricsEnabled) {
            this.pipelineRunCounter = meter.counterBuilder("tpf.pipeline.run.count")
                .setDescription("Pipeline runs")
                .setUnit("1")
                .build();
            this.pipelineRunErrorCounter = meter.counterBuilder("tpf.pipeline.run.errors")
                .setDescription("Pipeline run errors")
                .setUnit("1")
                .build();
            this.itemProducedCounter = meter.counterBuilder("tpf.item.produced")
                .setDescription("Items produced at the configured item boundary")
                .setUnit("items")
                .build();
            this.itemConsumedCounter = meter.counterBuilder("tpf.item.consumed")
                .setDescription("Items consumed at the configured item boundary")
                .setUnit("items")
                .build();
            this.sloItemThroughputTotal = meter.counterBuilder("tpf.slo.item.throughput.total")
                .setDescription("Total item throughput evaluations")
                .setUnit("1")
                .build();
            this.sloItemThroughputGood = meter.counterBuilder("tpf.slo.item.throughput.good")
                .setDescription("Item throughput evaluations meeting the threshold")
                .setUnit("1")
                .build();
            this.stepErrorCounter = meter.counterBuilder("tpf.step.errors")
                .setDescription("Pipeline step errors")
                .setUnit("1")
                .build();
            this.pipelineRunDuration = meter.histogramBuilder("tpf.pipeline.run.duration")
                .setDescription("Pipeline run duration")
                .setUnit("ms")
                .build();
            this.stepDuration = meter.histogramBuilder("tpf.step.duration")
                .setDescription("Pipeline step duration")
                .setUnit("ms")
                .build();
            meter.gaugeBuilder("tpf.step.inflight")
                .setDescription("In-flight items per step")
                .setUnit("items")
                .ofLongs()
                .buildWithCallback(this::recordInflightGauge);
            meter.gaugeBuilder("tpf.pipeline.max_concurrency")
                .setDescription("Configured max concurrency for the pipeline run")
                .setUnit("items")
                .ofLongs()
                .buildWithCallback(this::recordMaxConcurrencyGauge);
        } else {
            this.pipelineRunCounter = null;
            this.pipelineRunErrorCounter = null;
            this.itemProducedCounter = null;
            this.itemConsumedCounter = null;
            this.sloItemThroughputTotal = null;
            this.sloItemThroughputGood = null;
            this.stepErrorCounter = null;
            this.pipelineRunDuration = null;
            this.stepDuration = null;
        }
    }

    /**
     * Start a pipeline run and return the telemetry context.
     *
     * @param input pipeline input
     * @param stepCount number of steps
     * @param policy parallelism policy
     * @param maxConcurrency max concurrency
     * @return telemetry run context
     */
    public RunContext startRun(Object input, int stepCount, ParallelismPolicy policy, int maxConcurrency) {
        if (!enabled) {
            return RunContext.disabled();
        }
        boolean multiInput = input instanceof Multi<?>;
        Attributes attributes = Attributes.of(INPUT_KIND, multiInput ? "multi" : "uni");
        if (metricsEnabled) {
            pipelineRunCounter.add(1, attributes);
            this.maxConcurrency.set(Math.max(1, maxConcurrency));
        }
        Span span = null;
        Context context = Context.current();
        if (tracingEnabled) {
            span = tracer.spanBuilder("tpf.pipeline.run")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("tpf.steps.count", stepCount)
                .setAttribute("tpf.parallelism", policy == null ? "AUTO" : policy.name())
                .setAttribute("tpf.max_concurrency", maxConcurrency)
                .setAttribute("tpf.input", multiInput ? "multi" : "uni")
                .startSpan();
            context = context.with(span);
        }
        return new RunContext(
            context,
            span,
            System.nanoTime(),
            attributes,
            enabled,
            new AtomicLong(),
            new AtomicLong(),
            new LongAdder(),
            new LongAdder(),
            new LongAdder(),
            new LongAdder());
    }

    /**
     * Instrument pipeline input.
     *
     * @param input input Uni or Multi
     * @param runContext telemetry context
     * @return instrumented input
     */
    public Object instrumentInput(Object input, RunContext runContext) {
        return input;
    }

    /**
     * Instrument a consumer step to count items entering the configured item boundary.
     *
     * @param stepClass step class
     * @param input step input
     * @param <T> item type
     * @return instrumented input
     */
    public <T> Multi<T> instrumentItemConsumed(Class<?> stepClass, Multi<T> input) {
        return instrumentItemConsumed(stepClass, null, input);
    }

    /**
     * Instrument a consumer step to count items consumed at the configured item boundary.
     *
     * @param stepClass step class
     * @param runContext run context
     * @param input step input
     * @param <T> item type
     * @return instrumented input
     */
    public <T> Multi<T> instrumentItemConsumed(
        Class<?> stepClass,
        RunContext runContext,
        Multi<T> input) {
        if (!metricsEnabled || !isConsumer(stepClass)) {
            return input;
        }
        return input.onItem().invoke(item -> {
            itemConsumedCounter.add(1, boundaryAttributes(stepClass, true));
            if (runContext != null && runContext.enabled()) {
                runContext.itemsConsumed().add(1);
            }
        });
    }

    /**
     * Instrument a consumer step to count items entering the configured item boundary.
     *
     * @param stepClass step class
     * @param input step input
     * @param <T> item type
     * @return instrumented input
     */
    public <T> Uni<T> instrumentItemConsumed(Class<?> stepClass, Uni<T> input) {
        return instrumentItemConsumed(stepClass, null, input);
    }

    /**
     * Instrument a consumer step to count items consumed at the configured item boundary.
     *
     * @param stepClass step class
     * @param runContext run context
     * @param input step input
     * @param <T> item type
     * @return instrumented input
     */
    public <T> Uni<T> instrumentItemConsumed(
        Class<?> stepClass,
        RunContext runContext,
        Uni<T> input) {
        if (!metricsEnabled || !isConsumer(stepClass)) {
            return input;
        }
        return input.onItem().invoke(item -> {
            itemConsumedCounter.add(1, boundaryAttributes(stepClass, true));
            if (runContext != null && runContext.enabled()) {
                runContext.itemsConsumed().add(1);
            }
        });
    }

    /**
     * Instrument a producer step to count items emitted at the configured item boundary.
     *
     * @param stepClass step class
     * @param output step output
     * @param <T> item type
     * @return instrumented output
     */
    public <T> Multi<T> instrumentItemProduced(Class<?> stepClass, Multi<T> output) {
        return instrumentItemProduced(stepClass, null, output);
    }

    /**
     * Instrument a producer step to count items emitted at the configured item boundary.
     *
     * @param stepClass step class
     * @param runContext run context
     * @param output step output
     * @param <T> item type
     * @return instrumented output
     */
    public <T> Multi<T> instrumentItemProduced(
        Class<?> stepClass,
        RunContext runContext,
        Multi<T> output) {
        if (!metricsEnabled || !isProducer(stepClass)) {
            return output;
        }
        return output.onItem().invoke(item -> {
            itemProducedCounter.add(1, boundaryAttributes(stepClass, false));
            if (runContext != null && runContext.enabled()) {
                runContext.itemsProduced().add(1);
            }
        });
    }

    /**
     * Instrument a producer step to count items emitted at the configured item boundary.
     *
     * @param stepClass step class
     * @param output step output
     * @param <T> item type
     * @return instrumented output
     */
    public <T> Uni<T> instrumentItemProduced(Class<?> stepClass, Uni<T> output) {
        return instrumentItemProduced(stepClass, null, output);
    }

    /**
     * Instrument a producer step to count items emitted at the configured item boundary.
     *
     * @param stepClass step class
     * @param runContext run context
     * @param output step output
     * @param <T> item type
     * @return instrumented output
     */
    public <T> Uni<T> instrumentItemProduced(
        Class<?> stepClass,
        RunContext runContext,
        Uni<T> output) {
        if (!metricsEnabled || !isProducer(stepClass)) {
            return output;
        }
        return output.onItem().invoke(item -> {
            itemProducedCounter.add(1, boundaryAttributes(stepClass, false));
            if (runContext != null && runContext.enabled()) {
                runContext.itemsProduced().add(1);
            }
        });
    }

    /**
     * Attach completion hooks to a Uni or Multi to close the run.
     *
     * @param current Uni or Multi
     * @param runContext telemetry context
     * @return instrumented publisher
     */
    public Object instrumentRunCompletion(Object current, RunContext runContext) {
        if (runContext == null || !runContext.enabled()) {
            return current;
        }
        if (current instanceof Uni<?> uni) {
            return uni.onItemOrFailure().invoke((item, failure) -> endRun(runContext, failure));
        }
        if (current instanceof Multi<?> multi) {
            AtomicReference<Throwable> failureRef = new AtomicReference<>();
            return multi.onFailure().invoke(failureRef::set)
                .onTermination().invoke(() -> endRun(runContext, failureRef.get()));
        }
        return current;
    }

    /**
     * Instrument a step execution that returns a Uni.
     *
     * @param stepClass step class
     * @param uni step result
     * @param runContext telemetry context
     * @param perItemOperation true when called per item
     * @param <T> output type
     * @return instrumented Uni
     */
    public <T> Uni<T> instrumentStepUni(
        Class<?> stepClass,
        Uni<T> uni,
        RunContext runContext,
        boolean perItemOperation) {
        if (runContext == null || !runContext.enabled()) {
            return uni;
        }
        Span span = startStepSpan(stepClass, runContext, perItemOperation);
        long startNanos = System.nanoTime();
            onItemStart(stepClass, runContext);
            return uni.onItemOrFailure().invoke((item, failure) -> {
                recordStepOutcome(stepClass, startNanos, failure);
                onItemEnd(stepClass, runContext);
                endSpan(span, failure);
            });
    }

    /**
     * Instrument a step execution that returns a Multi.
     *
     * @param stepClass step class
     * @param multi step result
     * @param runContext telemetry context
     * @param perItemOperation true when called per item
     * @param <T> output type
     * @return instrumented Multi
     */
    public <T> Multi<T> instrumentStepMulti(
        Class<?> stepClass,
        Multi<T> multi,
        RunContext runContext,
        boolean perItemOperation) {
        if (runContext == null || !runContext.enabled()) {
            return multi;
        }
        Span span = startStepSpan(stepClass, runContext, perItemOperation);
        long startNanos = System.nanoTime();
        AtomicReference<Throwable> failureRef = new AtomicReference<>();
        onItemStart(stepClass, runContext);
        return multi.onFailure().invoke(failureRef::set)
            .onTermination().invoke(() -> {
                recordStepOutcome(stepClass, startNanos, failureRef.get());
                onItemEnd(stepClass, runContext);
                endSpan(span, failureRef.get());
            });
    }

    private Span startStepSpan(Class<?> stepClass, RunContext runContext, boolean perItemOperation) {
        if (!tracingEnabled || runContext == null || !runContext.enabled()) {
            return null;
        }
        if (perItemOperation && !perItemSpans) {
            return null;
        }
        return tracer.spanBuilder("tpf.step")
            .setParent(runContext.context())
            .setSpanKind(SpanKind.INTERNAL)
            .setAttribute("tpf.step.class", stepClass.getName())
            .startSpan();
    }

    private void recordStepOutcome(Class<?> stepClass, long startNanos, Throwable failure) {
        if (!metricsEnabled) {
            return;
        }
        double durationMs = nanosToMillis(startNanos);
        Attributes attributes = stepAttributes(stepClass);
        stepDuration.record(durationMs, attributes);
        if (failure != null) {
            stepErrorCounter.add(1, attributes);
        }
    }

    private void endRun(RunContext runContext, Throwable failure) {
        if (!runContext.enabled()) {
            return;
        }
        if (metricsEnabled) {
            double durationMs = nanosToMillis(runContext.startNanos());
            pipelineRunDuration.record(durationMs, runContext.attributes());
            if (failure != null) {
                pipelineRunErrorCounter.add(1, runContext.attributes());
            }
            recordThroughputSlo(runContext, durationMs);
        }
        if (tracingEnabled && runContext.span() != null) {
            long samples = runContext.inflightSamples().sum();
            double inflightAvg = samples > 0 ? runContext.inflightSum().sum() / (double) samples : 0.0;
            runContext.span().setAttribute(PARALLEL_MAX_IN_FLIGHT, runContext.inflightMax().get());
            runContext.span().setAttribute(PARALLEL_AVG_IN_FLIGHT, inflightAvg);
        }
        endSpan(runContext.span(), failure);
    }

    private void onItemStart(Class<?> stepClass, RunContext runContext) {
        if (runContext == null || !runContext.enabled()) {
            return;
        }
        long current = runContext.inflightCurrent().incrementAndGet();
        runContext.inflightSamples().increment();
        runContext.inflightSum().add(current);
        runContext.inflightMax().accumulateAndGet(current, Math::max);
        if (metricsEnabled) {
            inflightByStep
                .computeIfAbsent(stepClass.getName(), key -> new AtomicLong())
                .incrementAndGet();
        }
    }

    private void onItemEnd(Class<?> stepClass, RunContext runContext) {
        if (runContext == null || !runContext.enabled()) {
            return;
        }
        long current = runContext.inflightCurrent().decrementAndGet();
        runContext.inflightSamples().increment();
        runContext.inflightSum().add(Math.max(current, 0));
        if (metricsEnabled) {
            AtomicLong currentStep = inflightByStep.get(stepClass.getName());
            if (currentStep != null) {
                currentStep.updateAndGet(value -> Math.max(0, value - 1));
            }
        }
    }

    private void recordInflightGauge(ObservableLongMeasurement measurement) {
        inflightByStep.forEach((step, count) -> {
            measurement.record(count.get(), stepAttributes(step));
        });
    }

    private void recordMaxConcurrencyGauge(ObservableLongMeasurement measurement) {
        measurement.record(maxConcurrency.get());
    }

    private void recordThroughputSlo(RunContext runContext, double durationMs) {
        if (sloItemThroughputTotal == null || sloItemThroughputGood == null || itemBoundary == null) {
            return;
        }
        String consumerStep = itemBoundary.consumerStep();
        String itemType = itemBoundary.itemInputType();
        if (consumerStep == null || consumerStep.isBlank() || itemType == null || itemType.isBlank()) {
            return;
        }
        if (durationMs <= 0) {
            return;
        }
        long consumed = runContext.itemsConsumed().sum();
        double itemsPerMinute = consumed / (durationMs / 60_000d);
        Attributes attributes = boundaryAttributes(consumerStep, itemType);
        sloItemThroughputTotal.add(1, attributes);
        if (itemsPerMinute >= TelemetrySloConfig.itemThroughputPerMinute()) {
            sloItemThroughputGood.add(1, attributes);
        }
    }

    private boolean isProducer(Class<?> stepClass) {
        return itemBoundary != null
            && stepClass != null
            && resolveStepClassName(stepClass).equals(itemBoundary.producerStep());
    }

    private boolean isConsumer(Class<?> stepClass) {
        return itemBoundary != null
            && stepClass != null
            && resolveStepClassName(stepClass).equals(itemBoundary.consumerStep());
    }

    private Attributes boundaryAttributes(Class<?> stepClass, boolean consumed) {
        if (itemBoundary == null) {
            return stepAttributes(stepClass);
        }
        String stepName = resolveStepClassName(stepClass);
        String itemType = consumed ? itemBoundary.itemInputType() : itemBoundary.itemOutputType();
        return Attributes.of(
            STEP_CLASS, stepName,
            STEP_PARENT, resolveStepParent(stepName),
            ITEM_TYPE, itemType);
    }

    private Attributes boundaryAttributes(String stepClassName, String itemType) {
        return Attributes.of(
            STEP_CLASS, stepClassName,
            STEP_PARENT, resolveStepParent(stepClassName),
            ITEM_TYPE, itemType);
    }

    private String resolveStepClassName(Class<?> stepClass) {
        if (stepClass == null) {
            return null;
        }
        String name = stepClass.getName();
        if ((name.contains("_Subclass") || name.contains("$$")) && stepClass.getSuperclass() != null) {
            return stepClass.getSuperclass().getName();
        }
        return name;
    }

    private Attributes stepAttributes(Class<?> stepClass) {
        return stepClass == null
            ? Attributes.empty()
            : Attributes.of(
                STEP_CLASS, stepClass.getName(),
                STEP_PARENT, resolveStepParent(stepClass.getName()));
    }

    private Attributes stepAttributes(String stepClassName) {
        return Attributes.of(
            STEP_CLASS, stepClassName,
            STEP_PARENT, resolveStepParent(stepClassName));
    }

    private String resolveStepParent(String stepClassName) {
        return stepParents.getOrDefault(stepClassName, stepClassName);
    }

    private void endSpan(Span span, Throwable failure) {
        if (span == null) {
            return;
        }
        if (failure != null) {
            span.recordException(failure);
            span.setStatus(StatusCode.ERROR, failure.getMessage());
        }
        span.end();
    }

    private double nanosToMillis(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000d;
    }

    /**
     * Immutable run context used for pipeline telemetry.
     *
     * @param context parent context
     * @param span run span
     * @param startNanos start time
     * @param attributes run attributes
     * @param enabled whether telemetry is enabled
     * @param inflightCurrent current in-flight item count
     * @param inflightMax max in-flight item count
     * @param inflightSamples number of in-flight samples taken
     * @param inflightSum sum of in-flight samples
     */
    public record RunContext(
        Context context,
        Span span,
        long startNanos,
        Attributes attributes,
        boolean enabled,
        AtomicLong inflightCurrent,
        AtomicLong inflightMax,
        LongAdder inflightSamples,
        LongAdder inflightSum,
        LongAdder itemsConsumed,
        LongAdder itemsProduced) {

        static RunContext disabled() {
            return new RunContext(
                Context.current(),
                null,
                0L,
                Attributes.empty(),
                false,
                new AtomicLong(),
                new AtomicLong(),
                new LongAdder(),
                new LongAdder(),
                new LongAdder(),
                new LongAdder());
        }
    }
}
