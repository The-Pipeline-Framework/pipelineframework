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
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import java.util.Optional;
import org.pipelineframework.config.ParallelismPolicy;

/** Imperative span and context adapter. It owns no metric or replay instruments. */
final class PipelineTracingRecorder {
    private final TelemetryRuntime runtime;
    private final boolean enabled;
    private final boolean perItemSpansEnabled;
    private final Optional<PipelineReplayTopology> replayTopology;

    PipelineTracingRecorder(
        TelemetryPolicy policy,
        TelemetryRuntime runtime,
        Optional<PipelineReplayTopology> replayTopology
    ) {
        this.runtime = runtime;
        this.enabled = policy.tracingEnabled();
        this.perItemSpansEnabled = policy.perItemSpansEnabled();
        this.replayTopology = replayTopology;
    }

    TracedRun startRun(int stepCount, ParallelismPolicy policy, int maxConcurrency, String inputKind) {
        Context context = Context.current();
        if (!enabled) {
            return new TracedRun(context, null);
        }
        Span span = tracer().spanBuilder("tpf.pipeline.run")
            .setSpanKind(SpanKind.INTERNAL)
            .setAttribute("tpf.steps.count", stepCount)
            .setAttribute("tpf.parallelism", policy == null ? "AUTO" : policy.name())
            .setAttribute("tpf.max_concurrency", maxConcurrency)
            .setAttribute("tpf.input", inputKind)
            .setAttribute("tpf.pipeline", replayTopology.map(PipelineReplayTopology::pipeline).orElse("pipeline"))
            .startSpan();
        return new TracedRun(context.with(span), span);
    }

    Span startStep(Class<?> stepClass, PipelineRunContext runContext, boolean perItemOperation) {
        if (!enabled || runContext == null || !runContext.enabled() || (perItemOperation && !perItemSpansEnabled)) {
            return null;
        }
        String resolvedStepClass = PipelineMetricAttributes.resolveStepClassName(stepClass);
        Span span = tracer().spanBuilder("tpf.step")
            .setParent(runContext.context())
            .setSpanKind(SpanKind.INTERNAL)
            .setAttribute("tpf.step.class", resolvedStepClass)
            .startSpan();
        replayTopology.flatMap(topology -> topology.step(resolvedStepClass)).ifPresent(descriptor -> {
            span.setAttribute("tpf.pipeline", replayTopology.orElseThrow().pipeline());
            span.setAttribute("tpf.step", descriptor.step());
            span.setAttribute("tpf.service", descriptor.service());
            span.setAttribute("tpf.cardinality", descriptor.cardinality());
        });
        return span;
    }

    void finish(Span span, Throwable failure) {
        if (span == null) {
            return;
        }
        if (failure != null) {
            span.recordException(failure);
            span.setStatus(StatusCode.ERROR, failure.getMessage());
        }
        span.end();
    }

    void recordRunInflight(PipelineRunContext runContext) {
        if (!enabled || runContext == null || runContext.span() == null) {
            return;
        }
        long samples = runContext.inflightSamples().sum();
        double inflightAverage = samples > 0 ? runContext.inflightSum().sum() / (double) samples : 0d;
        runContext.span().setAttribute("tpf.parallel.max_in_flight", runContext.inflightMax().get());
        runContext.span().setAttribute("tpf.parallel.avg_in_flight", inflightAverage);
    }

    void recordKillSwitch(PipelineRunContext runContext, RetryAmplificationGuard.Trigger trigger) {
        if (!enabled || runContext == null || runContext.span() == null) {
            return;
        }
        runContext.span().addEvent("tpf.kill_switch.triggered", Attributes.builder()
            .put("tpf.kill_switch.triggered", true)
            .put("tpf.kill_switch.reason", "retry_amplification")
            .put("tpf.kill_switch.step", trigger.step())
            .put("tpf.kill_switch.inflight_slope", trigger.inflightSlope())
            .put("tpf.kill_switch.retry_rate", trigger.retryRate())
            .put("tpf.kill_switch.inflight_slope_threshold", trigger.inflightSlopeThreshold())
            .put("tpf.kill_switch.sustain_samples", (long) trigger.sustainSamples())
            .build());
    }

    private Tracer tracer() {
        return runtime.tracer("org.pipelineframework");
    }

    record TracedRun(Context context, Span span) { }
}
