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
import jakarta.annotation.PreDestroy;
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
    private static final AttributeKey<Boolean> KILL_SWITCH_TRIGGERED =
        AttributeKey.booleanKey("tpf.kill_switch.triggered");
    private static final AttributeKey<String> KILL_SWITCH_REASON =
        AttributeKey.stringKey("tpf.kill_switch.reason");
    private static final AttributeKey<String> KILL_SWITCH_STEP =
        AttributeKey.stringKey("tpf.kill_switch.step");
    private static final AttributeKey<Double> KILL_SWITCH_INFLIGHT_SLOPE =
        AttributeKey.doubleKey("tpf.kill_switch.inflight_slope");
    private static final AttributeKey<Double> KILL_SWITCH_RETRY_RATE =
        AttributeKey.doubleKey("tpf.kill_switch.retry_rate");
    private static final AttributeKey<Double> KILL_SWITCH_INFLIGHT_SLOPE_THRESHOLD =
        AttributeKey.doubleKey("tpf.kill_switch.inflight_slope_threshold");
    private static final AttributeKey<Long> KILL_SWITCH_SUSTAIN_SAMPLES =
        AttributeKey.longKey("tpf.kill_switch.sustain_samples");
    private static final String RETRY_AMPLIFICATION_REASON = "retry_amplification";

    private final boolean enabled;
    private final boolean metricsEnabled;
    private final boolean tracingEnabled;
    private final boolean perItemSpans;
    private final boolean retryAmplificationEnabled;
    private final Duration retryAmplificationWindow;
    private final double inflightSlopeThreshold;
    private final int retryAmplificationSustainSamples;
    private final RetryAmplificationGuardMode retryAmplificationMode;
    private final Duration retryAmplificationSampleInterval;
    private final ScheduledExecutorService retryAmplificationScheduler;
    private final Tracer tracer;
    private final Meter meter;
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
    private final LongCounter killSwitchCounter;
    private final DoubleHistogram pipelineRunDuration;
    private final DoubleHistogram stepDuration;
    private final ConcurrentMap<String, AtomicLong> inflightByStep;
    private final ConcurrentMap<String, LongAdder> retryByStep;
    private final AtomicLong maxConcurrency;
    private final PipelineTelemetryResourceLoader.ItemBoundary itemBoundary;
    private final Map<String, String> stepParents;
    private final RetryAmplificationGuard retryAmplificationGuard;
    private final AtomicReference<RetryAmplificationMonitor> activeRetryAmplificationMonitor;
    private static final AtomicReference<PipelineTelemetry> ACTIVE = new AtomicReference<>();

    /**
     * Create a telemetry helper from the configured pipeline settings.
     *
     * @param stepConfig pipeline configuration mapping
     */
    @Inject
    public PipelineTelemetry(PipelineStepConfig stepConfig) {
        PipelineStepConfig.TelemetryConfig telemetry = stepConfig.telemetry();
        PipelineStepConfig.RetryAmplificationGuardConfig guardConfig = null;
        PipelineStepConfig.KillSwitchConfig killSwitchConfig = stepConfig.killSwitch();
        if (killSwitchConfig != null) {
            guardConfig = killSwitchConfig.retryAmplification();
        }
        this.enabled = telemetry != null && Boolean.TRUE.equals(telemetry.enabled());
        this.tracingEnabled = enabled && Boolean.TRUE.equals(telemetry.tracing().enabled());
        this.perItemSpans = tracingEnabled && Boolean.TRUE.equals(telemetry.tracing().perItem());
        this.metricsEnabled = enabled && Boolean.TRUE.equals(telemetry.metrics().enabled());
        this.retryAmplificationEnabled = guardConfig != null && Boolean.TRUE.equals(guardConfig.enabled());
        this.retryAmplificationWindow = guardConfig != null && guardConfig.window() != null
            ? guardConfig.window()
            : Duration.ofSeconds(30);
        this.inflightSlopeThreshold = guardConfig != null && guardConfig.inflightSlopeThreshold() != null
            ? guardConfig.inflightSlopeThreshold()
            : 10d;
        this.retryAmplificationSustainSamples = guardConfig != null && guardConfig.sustainSamples() != null
            ? Math.max(1, guardConfig.sustainSamples())
            : 3;
        this.retryAmplificationMode = guardConfig != null && guardConfig.mode() != null
            ? guardConfig.mode()
            : RetryAmplificationGuardMode.FAIL_FAST;
        this.retryAmplificationSampleInterval = resolveSampleInterval(this.retryAmplificationWindow);
        this.retryAmplificationScheduler = retryAmplificationEnabled
            ? Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "tpf-retry-amplification-guard");
                thread.setDaemon(true);
                return thread;
            })
            : null;
        this.tracer = GlobalOpenTelemetry.getTracer("org.pipelineframework");
        this.meter = GlobalOpenTelemetry.getMeter("org.pipelineframework");
        this.inflightByStep = new ConcurrentHashMap<>();
        this.retryByStep = new ConcurrentHashMap<>();
        this.maxConcurrency = new AtomicLong();
        this.itemBoundary = PipelineTelemetryResourceLoader.loadItemBoundary().orElse(null);
        this.stepParents = itemBoundary != null ? itemBoundary.stepParents() : Map.of();
        this.retryAmplificationGuard = new RetryAmplificationGuard();
        this.activeRetryAmplificationMonitor = new AtomicReference<>();
        ACTIVE.set(this);
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
            this.sloItemSuccessTotal = meter.counterBuilder("tpf.slo.item.success.total")
                .setDescription("Items evaluated for success at the configured boundary")
                .setUnit("items")
                .build();
            this.sloItemSuccessGood = meter.counterBuilder("tpf.slo.item.success.good")
                .setDescription("Items successfully produced at the configured boundary")
                .setUnit("items")
                .build();
            this.stepErrorCounter = meter.counterBuilder("tpf.step.errors")
                .setDescription("Pipeline step errors")
                .setUnit("1")
                .build();
            this.stepRetryCounter = meter.counterBuilder("tpf.step.retry.count")
                .setDescription("Pipeline step retries")
                .setUnit("1")
                .build();
            this.killSwitchCounter = meter.counterBuilder("tpf.pipeline.kill_switch.triggered")
                .setDescription("Pipeline kill switch triggers")
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
            this.sloItemSuccessTotal = null;
            this.sloItemSuccessGood = null;
            this.stepErrorCounter = null;
            this.stepRetryCounter = null;
            this.killSwitchCounter = null;
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
        if (!enabled && !retryAmplificationEnabled) {
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
        RunContext runContext = new RunContext(
            context,
            span,
            System.nanoTime(),
            attributes,
            enabled || retryAmplificationEnabled,
            new AtomicLong(),
            new AtomicLong(),
            new LongAdder(),
            new LongAdder(),
            new LongAdder(),
            new LongAdder());
        startRetryAmplificationMonitor(runContext);
        return runContext;
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
        stopRetryAmplificationMonitor();
        if (metricsEnabled) {
            double durationMs = nanosToMillis(runContext.startNanos());
            pipelineRunDuration.record(durationMs, runContext.attributes());
            if (failure != null) {
                pipelineRunErrorCounter.add(1, runContext.attributes());
            }
            recordThroughputSlo(runContext, durationMs);
            recordItemSuccessSlo(runContext);
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
        if (metricsEnabled || retryAmplificationEnabled) {
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
        if (metricsEnabled || retryAmplificationEnabled) {
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

    private void recordItemSuccessSlo(RunContext runContext) {
        if (sloItemSuccessTotal == null || sloItemSuccessGood == null || itemBoundary == null) {
            return;
        }
        String consumerStep = itemBoundary.consumerStep();
        String itemType = itemBoundary.itemInputType();
        if (consumerStep == null || consumerStep.isBlank() || itemType == null || itemType.isBlank()) {
            return;
        }
        long consumed = runContext.itemsConsumed().sum();
        if (consumed <= 0) {
            return;
        }
        long produced = runContext.itemsProduced().sum();
        long goodCount = Math.min(consumed, produced);
        Attributes attributes = boundaryAttributes(consumerStep, itemType);
        sloItemSuccessTotal.add(consumed, attributes);
        if (goodCount > 0) {
            sloItemSuccessGood.add(goodCount, attributes);
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
        if ((name.contains("_Subclass") || name.contains("$$") || name.contains("_ClientProxy"))
            && stepClass.getSuperclass() != null) {
            return stepClass.getSuperclass().getName();
        }
        return name;
    }

    private Attributes stepAttributes(Class<?> stepClass) {
        if (stepClass == null) {
            return Attributes.empty();
        }
        String resolved = resolveStepClassName(stepClass);
        return Attributes.of(
            STEP_CLASS, resolved,
            STEP_PARENT, resolveStepParent(resolved));
    }

    private Attributes stepAttributes(String stepClassName) {
        if (stepClassName == null) {
            return Attributes.empty();
        }
        return Attributes.of(
            STEP_CLASS, stepClassName,
            STEP_PARENT, resolveStepParent(stepClassName));
    }

    private String resolveStepParent(String stepClassName) {
        return stepParents.getOrDefault(stepClassName, stepClassName);
    }

    /**
     * Record a retry for a step.
     *
     * @param stepClass step class
     */
    public static void recordRetry(Class<?> stepClass) {
        PipelineTelemetry telemetry = ACTIVE.get();
        if (telemetry != null) {
            telemetry.recordRetryInternal(stepClass);
        }
    }

    public boolean retryAmplificationGuardEnabled() {
        return retryAmplificationEnabled;
    }

    public RetryAmplificationGuardMode retryAmplificationMode() {
        return retryAmplificationMode;
    }

    public Duration retryAmplificationCheckInterval() {
        return retryAmplificationSampleInterval;
    }

    public java.util.Optional<RetryAmplificationGuard.Trigger> retryAmplificationTrigger() {
        RetryAmplificationMonitor monitor = activeRetryAmplificationMonitor.get();
        if (monitor == null) {
            return java.util.Optional.empty();
        }
        return monitor.triggered();
    }

    private void recordRetryInternal(Class<?> stepClass) {
        if (stepClass == null || !(metricsEnabled || retryAmplificationEnabled)) {
            return;
        }
        String step = resolveStepClassName(stepClass);
        if (step == null) {
            return;
        }
        retryByStep.computeIfAbsent(step, key -> new LongAdder()).increment();
        if (metricsEnabled) {
            stepRetryCounter.add(1, stepAttributes(step));
        }
    }

    private void startRetryAmplificationMonitor(RunContext runContext) {
        if (!retryAmplificationEnabled || retryAmplificationScheduler == null || runContext == null) {
            return;
        }
        RetryAmplificationMonitor monitor = new RetryAmplificationMonitor(runContext);
        RetryAmplificationMonitor previous = activeRetryAmplificationMonitor.getAndSet(monitor);
        if (previous != null) {
            previous.stop();
        }
        monitor.start();
    }

    private void stopRetryAmplificationMonitor() {
        RetryAmplificationMonitor monitor = activeRetryAmplificationMonitor.getAndSet(null);
        if (monitor != null) {
            monitor.stop();
        }
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

    @PreDestroy
    void shutdownRetryAmplificationScheduler() {
        if (retryAmplificationScheduler != null) {
            retryAmplificationScheduler.shutdownNow();
        }
    }

    private Duration resolveSampleInterval(Duration window) {
        long windowMillis = window == null ? 30_000L : Math.max(1_000L, window.toMillis());
        long intervalMillis = Math.max(1_000L, Math.min(windowMillis / 6, 5_000L));
        return Duration.ofMillis(intervalMillis);
    }

    private void recordKillSwitchTriggered(RunContext runContext, RetryAmplificationGuard.Trigger trigger) {
        if (metricsEnabled && killSwitchCounter != null) {
            killSwitchCounter.add(1, Attributes.of(
                KILL_SWITCH_REASON, RETRY_AMPLIFICATION_REASON,
                KILL_SWITCH_STEP, trigger.step()));
        }
        if (tracingEnabled && runContext != null && runContext.span() != null) {
            runContext.span().addEvent(
                "tpf.kill_switch.triggered",
                Attributes.builder()
                    .put(KILL_SWITCH_TRIGGERED, true)
                    .put(KILL_SWITCH_REASON, RETRY_AMPLIFICATION_REASON)
                    .put(KILL_SWITCH_STEP, trigger.step())
                    .put(KILL_SWITCH_INFLIGHT_SLOPE, trigger.inflightSlope())
                    .put(KILL_SWITCH_RETRY_RATE, trigger.retryRate())
                    .put(KILL_SWITCH_INFLIGHT_SLOPE_THRESHOLD, trigger.inflightSlopeThreshold())
                    .put(KILL_SWITCH_SUSTAIN_SAMPLES, (long) trigger.sustainSamples())
                    .build());
        }
    }

    private final class RetryAmplificationMonitor {
        private final RunContext runContext;
        private final Deque<RetryAmplificationGuard.Sample> samples;
        private final AtomicLong retryBaseline;
        private final AtomicReference<RetryAmplificationGuard.Trigger> trigger;
        private final AtomicInteger consecutiveBreaches;
        private ScheduledFuture<?> future;

        private RetryAmplificationMonitor(RunContext runContext) {
            this.runContext = runContext;
            this.samples = new ArrayDeque<>();
            this.retryBaseline = new AtomicLong(-1L);
            this.trigger = new AtomicReference<>();
            this.consecutiveBreaches = new AtomicInteger();
        }

        void start() {
            future = retryAmplificationScheduler.scheduleAtFixedRate(
                this::sample,
                0L,
                retryAmplificationSampleInterval.toMillis(),
                TimeUnit.MILLISECONDS);
        }

        void stop() {
            if (future != null) {
                future.cancel(false);
            }
        }

        java.util.Optional<RetryAmplificationGuard.Trigger> triggered() {
            return java.util.Optional.ofNullable(trigger.get());
        }

        private void sample() {
            if (trigger.get() != null) {
                return;
            }
            long now = System.nanoTime();
            long inflight = 0L;
            for (AtomicLong inflightCount : inflightByStep.values()) {
                inflight += inflightCount.get();
            }
            long retries = 0L;
            for (LongAdder retryCount : retryByStep.values()) {
                retries += retryCount.sum();
            }
            long baseline = retryBaseline.get();
            if (baseline < 0L) {
                retryBaseline.compareAndSet(-1L, retries);
                baseline = retryBaseline.get();
            }
            long relativeRetries = Math.max(0L, retries - baseline);

            Deque<RetryAmplificationGuard.Sample> snapshot;
            synchronized (samples) {
                samples.addLast(new RetryAmplificationGuard.Sample(now, inflight, relativeRetries));
                trimSamples(samples);
                snapshot = new ArrayDeque<>(samples);
            }
            java.util.Optional<RetryAmplificationGuard.Trigger> candidate = retryAmplificationGuard
                .evaluate("global", snapshot, retryAmplificationWindow, inflightSlopeThreshold,
                    retryAmplificationSustainSamples);
            if (candidate.isPresent()) {
                int streak = consecutiveBreaches.incrementAndGet();
                if (streak >= retryAmplificationSustainSamples) {
                    trigger(candidate.get());
                }
            } else {
                consecutiveBreaches.set(0);
            }
        }

        private void trigger(RetryAmplificationGuard.Trigger triggerInfo) {
            if (!trigger.compareAndSet(null, triggerInfo)) {
                return;
            }
            recordKillSwitchTriggered(runContext, triggerInfo);
            stop();
        }

        private void trimSamples(Deque<RetryAmplificationGuard.Sample> deque) {
            int maxSamples = Math.max(2, (int) (retryAmplificationWindow.toMillis()
                / Math.max(1L, retryAmplificationSampleInterval.toMillis())) + 2);
            while (deque.size() > maxSamples) {
                deque.removeFirst();
            }
        }
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
     * @param itemsConsumed number of items consumed at the boundary
     * @param itemsProduced number of items produced at the boundary
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
