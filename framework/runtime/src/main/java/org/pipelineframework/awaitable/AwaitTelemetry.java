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

package org.pipelineframework.awaitable;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.Cancellable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.pipelineframework.config.PipelineStepConfig;
import org.pipelineframework.telemetry.AwaitObservation;
import org.pipelineframework.telemetry.AwaitTelemetryAttributes;
import org.pipelineframework.telemetry.PipelineTracingSupport;
import org.pipelineframework.telemetry.NoopTelemetryRuntime;
import org.pipelineframework.telemetry.TelemetryPolicy;
import org.pipelineframework.telemetry.TelemetryRuntime;
import org.pipelineframework.telemetry.TelemetrySdkAttributes;

/** Shared, policy-aware Await signal adapter. Semantic callers pass facts; this class owns SDK adaptation only. */
@ApplicationScoped
public class AwaitTelemetry {
    private final TelemetryPolicy policy;
    private final TelemetryRuntime runtime;
    private final Instruments instruments;

    @Inject
    public AwaitTelemetry(PipelineStepConfig config, TelemetryRuntime runtime) {
        this(TelemetryPolicy.from(config, false), runtime);
    }

    public AwaitTelemetry(TelemetryPolicy policy, TelemetryRuntime runtime) {
        this.policy = policy;
        this.runtime = runtime;
        this.instruments = policy.metricsEnabled() ? Instruments.create(runtime) : Instruments.disabled();
    }

    public static AwaitTelemetry disabled() {
        return new AwaitTelemetry(TelemetryPolicy.disabled(), new NoopTelemetryRuntime());
    }

    public void recordDroppedCompletion(String transport, String reason) {
        emit(new AwaitObservation.CompletionDropped(transport, reason, Instant.now()));
    }

    public void recordInteractionDispatched(AwaitInteractionRecord record) {
        emit(new AwaitObservation.InteractionDispatched(context(record, null), Instant.now()));
    }

    public void recordInteractionCreated(AwaitInteractionRecord record) {
        emit(new AwaitObservation.InteractionCreated(context(record, null), Instant.now()));
    }

    public void recordUnitDispatchComplete(AwaitUnitRecord unit) {
        emit(new AwaitObservation.UnitDispatchCompleted(context(null, unit), Instant.now()));
    }

    public void recordCompletionAdmitted(AwaitInteractionRecord record) {
        long latency = record != null && record.createdAtEpochMs() > 0 && record.updatedAtEpochMs() >= record.createdAtEpochMs()
            ? record.updatedAtEpochMs() - record.createdAtEpochMs() : 0L;
        emit(new AwaitObservation.CompletionAdmitted(context(record, null), latency, Instant.now()));
    }

    public void recordLiveHandoff(AwaitInteractionRecord record) {
        emit(new AwaitObservation.LiveHandoff(context(record, null), Instant.now()));
    }

    public void recordScalarContinuationStarted(AwaitInteractionRecord record) {
        emit(new AwaitObservation.ScalarContinuationStarted(context(record, null), Instant.now()));
    }

    public <T> Uni<T> inProviderDispatchSpan(AwaitInteractionRecord record, Supplier<Uni<T>> operation) {
        AwaitObservation.ProviderDispatched observation = new AwaitObservation.ProviderDispatched(context(record, null), Instant.now());
        if (!policy.tracingEnabled()) {
            emit(observation);
            return Uni.createFrom().deferred(() -> operation.get());
        }
        return Uni.createFrom().deferred(() -> {
            Span span = startSpan("tpf.await.provider.dispatch", observation, true);
            AtomicBoolean terminal = new AtomicBoolean();
            Context dispatchContext = Context.current().with(span);
            return Uni.createFrom().emitter(emitter -> {
                AtomicReference<Cancellable> subscription = new AtomicReference<>();
                emitter.onTermination(() -> {
                    Cancellable active = subscription.get();
                    if (active != null) active.cancel();
                    withContext(dispatchContext, () -> endOnce(terminal, span,
                        new java.util.concurrent.CancellationException("Await provider dispatch cancelled")));
                });
                try (Scope ignored = dispatchContext.makeCurrent()) {
                    subscription.set(operation.get().subscribe().with(
                        item -> withContext(dispatchContext, () -> { endOnce(terminal, span, null); emitter.complete(item); }),
                        failure -> withContext(dispatchContext, () -> { endOnce(terminal, span, failure); emitter.fail(failure); })));
                } catch (Throwable failure) {
                    endOnce(terminal, span, failure);
                    emitter.fail(failure);
                }
            });
        });
    }

    /** Provider fixture boundary facts use the same policy/runtime rather than a framework-global tracer. */
    public void recordProviderAdmitted() {
        emit(new AwaitObservation.ProviderAdmitted(emptyContext(), Instant.now()));
    }

    public void recordProviderCompletionDispatched() {
        emit(new AwaitObservation.ProviderCompletionDispatched(emptyContext(), Instant.now()));
    }

    public void recordItemCompleted(AwaitInteractionRecord record, AwaitUnitRecord unit) {
        emit(new AwaitObservation.ItemCompleted(context(record, unit), Instant.now()));
    }

    public void recordEarlyCompletionHeld(AwaitInteractionRecord record, AwaitUnitRecord unit) {
        emit(new AwaitObservation.EarlyCompletionHeld(context(record, unit), Instant.now()));
    }

    public void recordResumeReleased(AwaitUnitRecord unit) {
        emit(new AwaitObservation.ResumeReleased(context(null, unit), Instant.now()));
    }

    public void recordResumeReleased(String stepId, String status, String transport) {
        emit(new AwaitObservation.ResumeReleased(new AwaitObservation.Context(
            stepId, transport, status, null, null, null, null, null, Map.of()), Instant.now()));
    }

    public void recordUnitTerminal(AwaitInteractionRecord record, AwaitUnitRecord unit) {
        long duration = unit != null && unit.createdAtEpochMs() > 0 && unit.updatedAtEpochMs() >= unit.createdAtEpochMs()
            ? unit.updatedAtEpochMs() - unit.createdAtEpochMs() : 0L;
        emit(new AwaitObservation.UnitTerminal(context(record, unit), duration, Instant.now()));
    }

    public void recordAdmissionAcquired(AwaitInteractionRecord record, boolean reused, boolean reconciled,
                                        long waitMillis, boolean locallyTracked) {
        emit(new AwaitObservation.AdmissionAcquired(context(record, null), reused, reconciled, waitMillis,
            locallyTracked, Instant.now()));
    }

    public void recordAdmissionReleased(AwaitInteractionRecord record, boolean released, boolean locallyTracked) {
        emit(new AwaitObservation.AdmissionReleased(context(record, null), released, locallyTracked, Instant.now()));
    }

    public Map<String, Object> captureTraceMetadata() {
        return PipelineTracingSupport.captureCurrentContext();
    }

    public Map<String, Object> captureTraceMetadata(SpanContext context) {
        return PipelineTracingSupport.capture(context);
    }

    private void emit(AwaitObservation observation) {
        instruments.record(observation);
        if (!policy.tracingEnabled()) return;
        String spanName = spanName(observation);
        if (spanName != null) {
            Span span = startSpan(spanName, observation, false);
            span.end();
        }
    }

    private Span startSpan(String name, AwaitObservation observation, boolean continueOrigin) {
        var builder = runtime.tracer("org.pipelineframework").spanBuilder(name);
        AwaitObservation.Context context = observationContext(observation);
        SpanContext origin = context == null ? SpanContext.getInvalid() : PipelineTracingSupport.durableOrigin(context.traceMetadata());
        SpanContext current = Span.current().getSpanContext();
        boolean linked = false;
        if (continueOrigin && origin.isValid()) {
            builder.setParent(Context.root().with(Span.wrap(origin)));
            linked = true;
        } else if (origin.isValid() && (!current.isValid() || !PipelineTracingSupport.same(current, origin))) {
            builder.addLink(origin);
            linked = true;
        } else if (origin.isValid()) {
            linked = PipelineTracingSupport.same(current, origin);
        }
        Span span = builder.startSpan();
        span.setAllAttributes(TelemetrySdkAttributes.from(AwaitTelemetryAttributes.spanAttributes(observation)));
        span.setAttribute("tpf.await.origin.present", origin.isValid());
        span.setAttribute("tpf.await.origin.linked", linked);
        return span;
    }

    private static String spanName(AwaitObservation observation) {
        if (observation instanceof AwaitObservation.InteractionCreated) return "tpf.await.interaction.created";
        if (observation instanceof AwaitObservation.ProviderAdmitted) return "tpf.await.provider.admitted";
        if (observation instanceof AwaitObservation.ProviderCompletionDispatched) return "tpf.await.provider.completion.dispatched";
        if (observation instanceof AwaitObservation.CompletionAdmitted) return "tpf.await.completion.admitted";
        if (observation instanceof AwaitObservation.LiveHandoff) return "tpf.await.live.handoff";
        if (observation instanceof AwaitObservation.ScalarContinuationStarted) return "tpf.await.scalar.continuation";
        return null;
    }

    private static AwaitObservation.Context context(AwaitInteractionRecord record, AwaitUnitRecord unit) {
        return new AwaitObservation.Context(
            record != null ? record.stepId() : unit == null ? null : unit.stepId(),
            record == null ? null : record.transportType(),
            record != null && record.status() != null ? record.status().name() : unit != null && unit.status() != null ? unit.status().name() : null,
            unit == null || unit.cardinality() == null ? null : unit.cardinality(),
            record == null ? null : record.executionId(), record == null ? null : record.interactionId(),
            record == null ? null : record.correlationId(), record == null ? unit == null ? null : unit.unitId() : record.unitId(),
            record == null ? Map.of() : record.transportMetadata());
    }

    private static AwaitObservation.Context emptyContext() {
        return new AwaitObservation.Context(null, null, null, null, null, null, null, null, Map.of());
    }

    private static AwaitObservation.Context observationContext(AwaitObservation observation) {
        if (observation instanceof AwaitObservation.CompletionDropped) return null;
        if (observation instanceof AwaitObservation.InteractionCreated value) return value.context();
        if (observation instanceof AwaitObservation.InteractionDispatched value) return value.context();
        if (observation instanceof AwaitObservation.ProviderDispatched value) return value.context();
        if (observation instanceof AwaitObservation.ProviderAdmitted value) return value.context();
        if (observation instanceof AwaitObservation.ProviderCompletionDispatched value) return value.context();
        if (observation instanceof AwaitObservation.CompletionAdmitted value) return value.context();
        if (observation instanceof AwaitObservation.LiveHandoff value) return value.context();
        if (observation instanceof AwaitObservation.ScalarContinuationStarted value) return value.context();
        if (observation instanceof AwaitObservation.UnitDispatchCompleted value) return value.context();
        if (observation instanceof AwaitObservation.ItemCompleted value) return value.context();
        if (observation instanceof AwaitObservation.EarlyCompletionHeld value) return value.context();
        if (observation instanceof AwaitObservation.ResumeReleased value) return value.context();
        if (observation instanceof AwaitObservation.UnitTerminal value) return value.context();
        if (observation instanceof AwaitObservation.AdmissionAcquired value) return value.context();
        return ((AwaitObservation.AdmissionReleased) observation).context();
    }

    private static void endOnce(AtomicBoolean terminal, Span span, Throwable failure) {
        if (!terminal.compareAndSet(false, true)) return;
        if (failure != null) span.recordException(failure);
        span.end();
    }

    private static void withContext(Context context, Runnable action) {
        try (Scope ignored = context.makeCurrent()) { action.run(); }
    }

    private record Instruments(LongCounter dropped, LongCounter interactionCreated, LongCounter interactionDispatched,
        LongCounter unitDispatchComplete, LongCounter completionAdmitted, LongCounter itemCompleted,
        LongCounter earlyHeld, LongCounter resumeReleased, LongCounter liveHandoff, LongCounter scalarContinuation,
        LongCounter unitTerminal, DoubleHistogram completionLatency, DoubleHistogram unitDuration,
        LongCounter admissionOutcome, LongUpDownCounter admissionPending, DoubleHistogram admissionWait) {
        static Instruments disabled() { return new Instruments(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null); }
        static Instruments create(TelemetryRuntime runtime) {
            var meter = runtime.meter("org.pipelineframework");
            return new Instruments(
                meter.counterBuilder("tpf.await.completion.dropped.total").setDescription("Total deterministic await completions dropped by transport consumers").setUnit("events").build(),
                meter.counterBuilder("tpf.await.interaction.created.total").setDescription("Total durable await interactions created").setUnit("interactions").build(),
                meter.counterBuilder("tpf.await.interaction.dispatched.total").setDescription("Total await interactions dispatched").setUnit("interactions").build(),
                meter.counterBuilder("tpf.await.unit.dispatch_complete.total").setDescription("Total await units whose dispatch completed").setUnit("units").build(),
                meter.counterBuilder("tpf.await.completion.admitted.total").setDescription("Total await completions admitted").setUnit("completions").build(),
                meter.counterBuilder("tpf.await.item.completed.total").setDescription("Total itemized await completions recorded").setUnit("items").build(),
                meter.counterBuilder("tpf.await.completion.early_held.total").setDescription("Total itemized await completions held until parent wait was durable").setUnit("completions").build(),
                meter.counterBuilder("tpf.await.resume.released.total").setDescription("Total await resumes released").setUnit("resumes").build(),
                meter.counterBuilder("tpf.await.live.handoff.total").setDescription("Total await completions handed to a live continuation").setUnit("handoffs").build(),
                meter.counterBuilder("tpf.await.scalar.continuation.started.total").setDescription("Total scalar await continuations started").setUnit("continuations").build(),
                meter.counterBuilder("tpf.await.unit.terminal.total").setDescription("Total await units moved to a terminal state").setUnit("units").build(),
                meter.histogramBuilder("tpf.await.completion.latency").setDescription("Time from await interaction creation to completion admission").setUnit("ms").build(),
                meter.histogramBuilder("tpf.await.unit.duration").setDescription("Time from await unit creation to terminal state").setUnit("ms").build(),
                meter.counterBuilder("tpf.await.admission.outcomes.total").setDescription("Total durable await admission lifecycle outcomes").setUnit("events").build(),
                meter.upDownCounterBuilder("tpf.await.admission.pending").setDescription("Locally observed durable await admission reservations").setUnit("reservations").build(),
                meter.histogramBuilder("tpf.await.admission.wait").setDescription("Time spent waiting for a durable await admission reservation").setUnit("ms").build());
        }
        void record(AwaitObservation observation) {
            if (dropped == null) return;
            Attributes attributes = TelemetrySdkAttributes.from(AwaitTelemetryAttributes.metricAttributes(observation));
            if (observation instanceof AwaitObservation.InteractionCreated) interactionCreated.add(1, attributes);
            else if (observation instanceof AwaitObservation.InteractionDispatched) interactionDispatched.add(1, attributes);
            else if (observation instanceof AwaitObservation.UnitDispatchCompleted) unitDispatchComplete.add(1, attributes);
            else if (observation instanceof AwaitObservation.CompletionAdmitted value) { completionAdmitted.add(1, attributes); if (value.latencyMillis() > 0) completionLatency.record(value.latencyMillis(), attributes); }
            else if (observation instanceof AwaitObservation.LiveHandoff) liveHandoff.add(1, attributes);
            else if (observation instanceof AwaitObservation.ScalarContinuationStarted) scalarContinuation.add(1, attributes);
            else if (observation instanceof AwaitObservation.ItemCompleted) itemCompleted.add(1, attributes);
            else if (observation instanceof AwaitObservation.EarlyCompletionHeld) earlyHeld.add(1, attributes);
            else if (observation instanceof AwaitObservation.ResumeReleased) resumeReleased.add(1, attributes);
            else if (observation instanceof AwaitObservation.UnitTerminal value) { unitTerminal.add(1, attributes); if (value.durationMillis() > 0) unitDuration.record(value.durationMillis(), attributes); }
            else if (observation instanceof AwaitObservation.CompletionDropped) dropped.add(1, attributes);
            else if (observation instanceof AwaitObservation.ProviderAdmitted || observation instanceof AwaitObservation.ProviderCompletionDispatched) { }
            else if (observation instanceof AwaitObservation.AdmissionAcquired value) {
                admissionOutcome.add(1, attributes);
                if (value.locallyTracked()) admissionPending.add(1, attributes);
                if (value.reconciled()) admissionOutcome.add(1, attributes.toBuilder()
                    .put("tpf.await.admission.outcome", "reconciled").build());
                if (value.waitMillis() > 0) {
                    admissionOutcome.add(1, attributes.toBuilder().put("tpf.await.admission.outcome", "waited").build());
                    admissionWait.record(value.waitMillis(), attributes);
                }
            }
            else if (observation instanceof AwaitObservation.AdmissionReleased value && value.released()) { admissionOutcome.add(1, attributes); if (value.locallyTracked()) admissionPending.add(-1, attributes); }
        }
    }
}
