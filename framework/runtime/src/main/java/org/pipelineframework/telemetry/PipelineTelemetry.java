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

import java.util.concurrent.atomic.AtomicReference;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.quarkus.arc.Unremovable;
import io.smallrye.config.ConfigProvider;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.config.ParallelismPolicy;
import org.pipelineframework.config.PipelineStepConfig;

@ApplicationScoped
@Unremovable
public class PipelineTelemetry {

    private static final AttributeKey<String> INPUT_KIND = AttributeKey.stringKey("tpf.input");
    private static final AttributeKey<String> STEP_CLASS = AttributeKey.stringKey("tpf.step.class");
    private static final AttributeKey<String> PIPELINE_NAME = AttributeKey.stringKey("tpf.pipeline.name");

    private final boolean enabled;
    private final boolean metricsEnabled;
    private final boolean tracingEnabled;
    private final boolean perItemSpans;
    private final Tracer tracer;
    private final Meter meter;
    private final String pipelineName;
    private final LongCounter pipelineRunCounter;
    private final LongCounter pipelineRunErrorCounter;
    private final LongCounter itemCounter;
    private final LongCounter stepErrorCounter;
    private final DoubleHistogram pipelineRunDuration;
    private final DoubleHistogram stepDuration;

    @Inject
    public PipelineTelemetry(PipelineStepConfig stepConfig) {
        PipelineStepConfig.TelemetryConfig telemetry = stepConfig.telemetry();
        this.enabled = telemetry != null && Boolean.TRUE.equals(telemetry.enabled());
        this.tracingEnabled = enabled && Boolean.TRUE.equals(telemetry.tracing().enabled());
        this.perItemSpans = tracingEnabled && Boolean.TRUE.equals(telemetry.tracing().perItem());
        this.metricsEnabled = enabled && Boolean.TRUE.equals(telemetry.metrics().enabled());
        this.pipelineName = ConfigProvider.getConfig()
            .getOptionalValue("quarkus.application.name", String.class)
            .orElse(null);
        this.tracer = GlobalOpenTelemetry.getTracer("org.pipelineframework");
        this.meter = GlobalOpenTelemetry.getMeter("org.pipelineframework");
        if (metricsEnabled) {
            this.pipelineRunCounter = meter.counterBuilder("tpf.pipeline.run.count")
                .setDescription("Pipeline runs")
                .setUnit("1")
                .build();
            this.pipelineRunErrorCounter = meter.counterBuilder("tpf.pipeline.run.errors")
                .setDescription("Pipeline run errors")
                .setUnit("1")
                .build();
            this.itemCounter = meter.counterBuilder("tpf.pipeline.item.count")
                .setDescription("Pipeline input items")
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
        } else {
            this.pipelineRunCounter = null;
            this.pipelineRunErrorCounter = null;
            this.itemCounter = null;
            this.stepErrorCounter = null;
            this.pipelineRunDuration = null;
            this.stepDuration = null;
        }
    }

    public RunContext startRun(Object input, int stepCount, ParallelismPolicy policy, int maxConcurrency) {
        if (!enabled) {
            return RunContext.disabled();
        }
        boolean multiInput = input instanceof Multi<?>;
        Attributes attributes = baseAttributes(multiInput);
        if (metricsEnabled) {
            pipelineRunCounter.add(1, attributes);
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
            if (pipelineName != null && !pipelineName.isBlank()) {
                span.setAttribute("tpf.pipeline.name", pipelineName);
            }
            context = context.with(span);
        }
        return new RunContext(context, span, System.nanoTime(), attributes, enabled);
    }

    public Object instrumentInput(Object input, RunContext runContext) {
        if (!metricsEnabled || runContext == null || !runContext.enabled()) {
            return input;
        }
        if (input instanceof Uni<?> uni) {
            return uni.onItem().invoke(item -> itemCounter.add(1, runContext.attributes()));
        }
        if (input instanceof Multi<?> multi) {
            return multi.onItem().invoke(item -> itemCounter.add(1, runContext.attributes()));
        }
        return input;
    }

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
        return uni.onItemOrFailure().invoke((item, failure) -> {
            recordStepOutcome(stepClass, startNanos, failure);
            endSpan(span, failure);
        });
    }

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
        return multi.onFailure().invoke(failureRef::set)
            .onTermination().invoke(() -> {
                recordStepOutcome(stepClass, startNanos, failureRef.get());
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
        Attributes attributes = pipelineName != null && !pipelineName.isBlank()
            ? Attributes.of(STEP_CLASS, stepClass.getName(), PIPELINE_NAME, pipelineName)
            : Attributes.of(STEP_CLASS, stepClass.getName());
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
        }
        endSpan(runContext.span(), failure);
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

    private Attributes baseAttributes(boolean multiInput) {
        if (pipelineName == null || pipelineName.isBlank()) {
            return Attributes.of(INPUT_KIND, multiInput ? "multi" : "uni");
        }
        return Attributes.of(
            INPUT_KIND, multiInput ? "multi" : "uni",
            PIPELINE_NAME, pipelineName
        );
    }

    public record RunContext(
        Context context,
        Span span,
        long startNanos,
        Attributes attributes,
        boolean enabled) {

        static RunContext disabled() {
            return new RunContext(Context.current(), null, 0L, Attributes.empty(), false);
        }
    }
}
