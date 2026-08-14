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
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.pipelineframework.config.PipelineStepConfig;
import org.pipelineframework.telemetry.AwaitObservation;
import org.pipelineframework.telemetry.derivation.AwaitTelemetryDerivation;
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
        emit(new AwaitObservation.InteractionDispatched(
            AwaitObservations.context(Optional.of(record), Optional.empty()), Instant.now()));
    }

    public void recordInteractionCreated(AwaitInteractionRecord record) {
        emit(new AwaitObservation.InteractionCreated(
            AwaitObservations.context(Optional.of(record), Optional.empty()), Instant.now()));
    }

    public void recordUnitDispatchComplete(AwaitUnitRecord unit) {
        emit(new AwaitObservation.UnitDispatchCompleted(
            AwaitObservations.context(Optional.empty(), Optional.of(unit)), Instant.now()));
    }

    public void recordCompletionAdmitted(AwaitInteractionRecord record) {
        emit(AwaitObservations.completionAdmitted(record, Instant.now()));
    }

    public void recordLiveHandoff(AwaitInteractionRecord record) {
        emit(new AwaitObservation.LiveHandoff(
            AwaitObservations.context(Optional.of(record), Optional.empty()), Instant.now()));
    }

    public void recordScalarContinuationStarted(AwaitInteractionRecord record) {
        emit(new AwaitObservation.ScalarContinuationStarted(
            AwaitObservations.context(Optional.of(record), Optional.empty()), Instant.now()));
    }

    public <T> Uni<T> inProviderDispatchSpan(AwaitInteractionRecord record, Supplier<Uni<T>> operation) {
        AwaitObservation.ProviderDispatched observation = new AwaitObservation.ProviderDispatched(
            AwaitObservations.context(Optional.of(record), Optional.empty()), Instant.now());
        if (!policy.tracingEnabled()) {
            emit(observation);
            return Uni.createFrom().deferred(() -> operation.get());
        }
        return Uni.createFrom().deferred(() -> {
            Optional<AwaitTelemetryDerivation.SpanPlan> plan = AwaitTelemetryDerivation.span(observation);
            if (plan.isEmpty()) {
                return operation.get();
            }
            Span span = startSpan(plan.orElseThrow());
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
        emit(new AwaitObservation.ProviderAdmitted(AwaitObservations.emptyContext(), Instant.now()));
    }

    public void recordProviderCompletionDispatched() {
        emit(new AwaitObservation.ProviderCompletionDispatched(AwaitObservations.emptyContext(), Instant.now()));
    }

    public void recordItemCompleted(AwaitInteractionRecord record, AwaitUnitRecord unit) {
        emit(new AwaitObservation.ItemCompleted(
            AwaitObservations.context(Optional.of(record), Optional.of(unit)), Instant.now()));
    }

    public void recordEarlyCompletionHeld(AwaitInteractionRecord record, AwaitUnitRecord unit) {
        emit(new AwaitObservation.EarlyCompletionHeld(
            AwaitObservations.context(Optional.of(record), Optional.of(unit)), Instant.now()));
    }

    public void recordResumeReleased(AwaitUnitRecord unit) {
        emit(new AwaitObservation.ResumeReleased(
            AwaitObservations.context(Optional.empty(), Optional.of(unit)), Instant.now()));
    }

    public void recordResumeReleased(String stepId, String status, String transport) {
        emit(new AwaitObservation.ResumeReleased(new AwaitObservation.Context(
            Optional.ofNullable(stepId), Optional.ofNullable(transport), Optional.ofNullable(status), Optional.empty(),
            Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Map.of()), Instant.now()));
    }

    public void recordUnitTerminal(AwaitInteractionRecord record, AwaitUnitRecord unit) {
        emit(AwaitObservations.unitTerminal(record, unit, Instant.now()));
    }

    public void recordAdmissionAcquired(AwaitInteractionRecord record, boolean reused, boolean reconciled,
                                        long waitMillis, boolean locallyTracked) {
        emit(new AwaitObservation.AdmissionAcquired(
            AwaitObservations.context(Optional.of(record), Optional.empty()), reused, reconciled, waitMillis,
            locallyTracked, Instant.now()));
    }

    public void recordAdmissionReleased(AwaitInteractionRecord record, boolean released, boolean locallyTracked) {
        emit(new AwaitObservation.AdmissionReleased(
            AwaitObservations.context(Optional.of(record), Optional.empty()), released, locallyTracked, Instant.now()));
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
        AwaitTelemetryDerivation.span(observation).ifPresent(plan -> {
            Span span = startSpan(plan);
            span.end();
        });
    }

    private Span startSpan(AwaitTelemetryDerivation.SpanPlan plan) {
        var builder = runtime.tracer("org.pipelineframework").spanBuilder(plan.name());
        SpanContext origin = plan.context().map(AwaitObservation.Context::traceMetadata)
            .map(PipelineTracingSupport::durableOrigin).orElseGet(SpanContext::getInvalid);
        SpanContext current = Span.current().getSpanContext();
        boolean linked = false;
        if (plan.linkMode() == AwaitTelemetryDerivation.LinkMode.CONTINUE_DURABLE_ORIGIN && origin.isValid()) {
            builder.setParent(Context.root().with(Span.wrap(origin)));
            linked = true;
        } else if (origin.isValid() && (!current.isValid() || !PipelineTracingSupport.same(current, origin))) {
            builder.addLink(origin);
            linked = true;
        } else if (origin.isValid()) {
            linked = PipelineTracingSupport.same(current, origin);
        }
        Span span = builder.startSpan();
        span.setAllAttributes(TelemetrySdkAttributes.from(plan.attributes()));
        span.setAttribute("tpf.await.origin.present", origin.isValid());
        span.setAttribute("tpf.await.origin.linked", linked);
        return span;
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
            AwaitTelemetryDerivation.metrics(observation).forEach(this::record);
        }

        private void record(AwaitTelemetryDerivation.MetricSignal signal) {
            Attributes attributes = TelemetrySdkAttributes.from(signal.attributes());
            switch (signal.metric()) {
                case DROPPED -> dropped.add((long) signal.value(), attributes);
                case INTERACTION_CREATED -> interactionCreated.add((long) signal.value(), attributes);
                case INTERACTION_DISPATCHED -> interactionDispatched.add((long) signal.value(), attributes);
                case UNIT_DISPATCH_COMPLETED -> unitDispatchComplete.add((long) signal.value(), attributes);
                case COMPLETION_ADMITTED -> completionAdmitted.add((long) signal.value(), attributes);
                case COMPLETION_LATENCY -> completionLatency.record(signal.value(), attributes);
                case ITEM_COMPLETED -> itemCompleted.add((long) signal.value(), attributes);
                case EARLY_HELD -> earlyHeld.add((long) signal.value(), attributes);
                case RESUME_RELEASED -> resumeReleased.add((long) signal.value(), attributes);
                case LIVE_HANDOFF -> liveHandoff.add((long) signal.value(), attributes);
                case SCALAR_CONTINUATION -> scalarContinuation.add((long) signal.value(), attributes);
                case UNIT_TERMINAL -> unitTerminal.add((long) signal.value(), attributes);
                case UNIT_DURATION -> unitDuration.record(signal.value(), attributes);
                case ADMISSION_OUTCOME -> admissionOutcome.add((long) signal.value(), attributes);
                case ADMISSION_PENDING -> admissionPending.add((long) signal.value(), attributes);
                case ADMISSION_WAIT -> admissionWait.record(signal.value(), attributes);
            }
        }
    }
}
