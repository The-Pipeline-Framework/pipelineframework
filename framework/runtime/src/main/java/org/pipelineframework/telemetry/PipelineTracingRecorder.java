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

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import java.util.Optional;
import org.pipelineframework.telemetry.derivation.RunTelemetryDerivation;
import org.pipelineframework.telemetry.derivation.StepTelemetryDerivation;

/** Imperative span and context adapter. It owns no metric or replay instruments. */
final class PipelineTracingRecorder {
    private final TelemetryRuntime runtime;
    private final boolean enabled;
    private final boolean perItemSpansEnabled;

    PipelineTracingRecorder(
        TelemetryPolicy policy,
        TelemetryRuntime runtime
    ) {
        this.runtime = runtime;
        this.enabled = policy.tracingEnabled();
        this.perItemSpansEnabled = policy.perItemSpansEnabled();
    }

    TracedRun startRun(RunTelemetryDerivation.SpanStarted plan) {
        Context context = Context.current();
        if (!enabled) {
            return new TracedRun(context, null);
        }
        Span span = tracer().spanBuilder(plan.name())
            .setSpanKind(SpanKind.INTERNAL)
            .setAllAttributes(TelemetrySdkAttributes.from(plan.attributes()))
            .setAttribute("tpf.steps.count", plan.stepCount())
            .setAttribute("tpf.parallelism", plan.parallelism())
            .setAttribute("tpf.max_concurrency", plan.maxConcurrency())
            .startSpan();
        return new TracedRun(context.with(span), span);
    }

    Span startStep(StepTelemetryDerivation.SpanStarted plan, PipelineRunContext runContext) {
        if (!enabled || runContext == null || !runContext.enabled() || (plan.perItem() && !perItemSpansEnabled)) {
            return null;
        }
        return tracer().spanBuilder(plan.name())
            .setParent(runContext.context())
            .setSpanKind(SpanKind.INTERNAL)
            .setAllAttributes(TelemetrySdkAttributes.from(plan.attributes()))
            .startSpan();
    }

    void finish(Span span, StepTelemetryDerivation.SpanFinished signal) {
        finish(span, signal.failure());
    }

    void finish(Span span, RunTelemetryDerivation.SpanFinished signal) {
        finish(span, signal.failure());
    }

    private void finish(Span span, Optional<Throwable> failure) {
        if (span == null) {
            return;
        }
        failure.ifPresent(error -> {
            span.recordException(error);
            span.setStatus(StatusCode.ERROR, error.getMessage());
        });
        span.end();
    }

    void recordRunInflight(PipelineRunContext runContext, RunTelemetryDerivation.InflightSignal signal) {
        if (!enabled || runContext == null || runContext.span() == null) {
            return;
        }
        runContext.span().setAttribute("tpf.parallel.max_in_flight", signal.maximum());
        runContext.span().setAttribute("tpf.parallel.avg_in_flight", signal.average());
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
