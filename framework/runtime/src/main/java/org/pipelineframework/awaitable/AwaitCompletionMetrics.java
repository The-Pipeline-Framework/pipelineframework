package org.pipelineframework.awaitable;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.telemetry.TelemetryRuntimes;
import io.opentelemetry.api.metrics.DoubleHistogram;

/**
 * Await completion observability helpers.
 */
public final class AwaitCompletionMetrics {

    private static final AttributeKey<String> TRANSPORT = AttributeKey.stringKey("tpf.await.transport");
    private static final AttributeKey<String> REASON = AttributeKey.stringKey("tpf.await.completion.reason");
    private static final AttributeKey<String> STEP_ID = AttributeKey.stringKey("tpf.await.step_id");
    private static final AttributeKey<String> CARDINALITY = AttributeKey.stringKey("tpf.await.cardinality");
    private static final AttributeKey<String> STATUS = AttributeKey.stringKey("tpf.await.status");

    private static volatile LongCounter droppedCompletionCounter;
    private static volatile LongCounter interactionCreatedCounter;
    private static volatile LongCounter interactionDispatchedCounter;
    private static volatile LongCounter unitDispatchCompleteCounter;
    private static volatile LongCounter completionAdmittedCounter;
    private static volatile LongCounter itemCompletedCounter;
    private static volatile LongCounter earlyCompletionHeldCounter;
    private static volatile LongCounter resumeReleasedCounter;
    private static volatile LongCounter liveHandoffCounter;
    private static volatile LongCounter scalarContinuationCounter;
    private static volatile LongCounter unitTerminalCounter;
    private static volatile DoubleHistogram completionLatencyHistogram;
    private static volatile DoubleHistogram unitDurationHistogram;
    private static volatile LongCounter admissionOutcomeCounter;
    private static volatile LongUpDownCounter admissionPendingCounter;
    private static volatile DoubleHistogram admissionWaitHistogram;

    private AwaitCompletionMetrics() {
    }

    public static void recordDroppedCompletion(String transport, String reason) {
        ensureInitialized();
        droppedCompletionCounter.add(1, Attributes.builder()
            .put(TRANSPORT, normalize(transport))
            .put(REASON, normalize(reason))
            .build());
    }

    public static void recordInteractionDispatched(AwaitInteractionRecord record) {
        ensureInitialized();
        interactionDispatchedCounter.add(1, interactionAttributes(record));
    }

    public static void recordInteractionCreated(AwaitInteractionRecord record) {
        ensureInitialized();
        interactionCreatedCounter.add(1, interactionAttributes(record));
        emitSpan("tpf.await.interaction.created", record, false);
    }

    public static void recordUnitDispatchComplete(AwaitUnitRecord unit) {
        ensureInitialized();
        unitDispatchCompleteCounter.add(1, unitAttributes(unit));
    }

    public static void recordCompletionAdmitted(AwaitInteractionRecord record) {
        ensureInitialized();
        Attributes attributes = interactionAttributes(record);
        completionAdmittedCounter.add(1, attributes);
        if (record != null && record.createdAtEpochMs() > 0 && record.updatedAtEpochMs() >= record.createdAtEpochMs()) {
            completionLatencyHistogram.record(record.updatedAtEpochMs() - record.createdAtEpochMs(), attributes);
        }
        emitSpan("tpf.await.completion.admitted", record, false);
    }

    public static void recordLiveHandoff(AwaitInteractionRecord record) {
        ensureInitialized();
        liveHandoffCounter.add(1, interactionAttributes(record));
        emitSpan("tpf.await.live.handoff", record, false);
    }

    public static void recordScalarContinuationStarted(AwaitInteractionRecord record) {
        ensureInitialized();
        scalarContinuationCounter.add(1, interactionAttributes(record));
        emitSpan("tpf.await.scalar.continuation", record, false);
    }

    /**
     * Captures the originating span in durable interaction metadata. This is deliberately not a metric attribute.
     */
    public static Map<String, Object> captureTraceMetadata() {
        return captureTraceMetadata(Span.current().getSpanContext());
    }

    public static Map<String, Object> captureTraceMetadata(SpanContext context) {
        if (context == null || !context.isValid()) {
            return Map.of();
        }
        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("tpf.trace.id", context.getTraceId());
        metadata.put("tpf.trace.span_id", context.getSpanId());
        metadata.put("tpf.trace.flags", context.getTraceFlags().asHex());
        return Map.copyOf(metadata);
    }

    /**
     * Decorates the actual reactive dispatch subscription so Kafka/transport instrumentation sees the originating span.
     */
    public static <T> Uni<T> inProviderDispatchSpan(
        AwaitInteractionRecord record,
        Supplier<Uni<T>> operation
    ) {
        return Uni.createFrom().deferred(() -> {
            Span span = startSpan("tpf.await.provider.dispatch", record, true);
            AtomicBoolean terminal = new AtomicBoolean();
            try (Scope ignored = span.makeCurrent()) {
                return operation.get()
                    .onItemOrFailure().invoke((item, failure) -> endOnce(terminal, span, failure))
                    .onCancellation().invoke(() -> endOnce(
                        terminal, span, new java.util.concurrent.CancellationException("Await provider dispatch cancelled")));
            }
        });
    }

    public static void recordItemCompleted(AwaitInteractionRecord record, AwaitUnitRecord unit) {
        ensureInitialized();
        itemCompletedCounter.add(1, unitAttributes(unit, record == null ? null : record.transportType()));
    }

    public static void recordEarlyCompletionHeld(AwaitInteractionRecord record, AwaitUnitRecord unit) {
        ensureInitialized();
        earlyCompletionHeldCounter.add(1, unitAttributes(unit, record == null ? null : record.transportType()));
    }

    public static void recordResumeReleased(AwaitUnitRecord unit) {
        ensureInitialized();
        resumeReleasedCounter.add(1, unitAttributes(unit));
    }

    public static void recordResumeReleased(AwaitReplayView lifecycleEvent) {
        ensureInitialized();
        resumeReleasedCounter.add(1, lifecycleAttributes(lifecycleEvent));
    }

    public static void recordUnitTerminal(AwaitInteractionRecord record, AwaitUnitRecord unit) {
        ensureInitialized();
        Attributes attributes = unitAttributes(unit, record == null ? null : record.transportType());
        unitTerminalCounter.add(1, attributes);
        if (unit != null && unit.createdAtEpochMs() > 0 && unit.updatedAtEpochMs() >= unit.createdAtEpochMs()) {
            unitDurationHistogram.record(unit.updatedAtEpochMs() - unit.createdAtEpochMs(), attributes);
        }
    }

    public static void recordAdmissionAcquired(
        AwaitInteractionRecord record,
        boolean reused,
        boolean reconciled,
        long waitMillis,
        boolean locallyTracked
    ) {
        ensureInitialized();
        Attributes attributes = interactionAttributes(record);
        if (reused) {
            admissionOutcomeCounter.add(1, admissionAttributes(attributes, "reused"));
        } else {
            admissionOutcomeCounter.add(1, admissionAttributes(attributes, "acquired"));
        }
        if (locallyTracked) {
            admissionPendingCounter.add(1, pendingAttributes(record));
        }
        if (reconciled) {
            admissionOutcomeCounter.add(1, admissionAttributes(attributes, "reconciled"));
        }
        if (waitMillis > 0) {
            admissionOutcomeCounter.add(1, admissionAttributes(attributes, "waited"));
            admissionWaitHistogram.record(waitMillis, attributes);
        }
    }

    public static void recordAdmissionReleased(AwaitInteractionRecord record, boolean released, boolean locallyTracked) {
        ensureInitialized();
        if (!released) {
            return;
        }
        Attributes attributes = interactionAttributes(record);
        admissionOutcomeCounter.add(1, admissionAttributes(attributes, "released"));
        if (locallyTracked) {
            admissionPendingCounter.add(-1, pendingAttributes(record));
        }
    }

    private static void ensureInitialized() {
        if (droppedCompletionCounter != null) {
            return;
        }
        synchronized (AwaitCompletionMetrics.class) {
            if (droppedCompletionCounter != null) {
                return;
            }
            var meter = TelemetryRuntimes.global().meter("org.pipelineframework");
            interactionCreatedCounter = meter.counterBuilder("tpf.await.interaction.created.total")
                .setDescription("Total durable await interactions created")
                .setUnit("interactions")
                .build();
            interactionDispatchedCounter = meter.counterBuilder("tpf.await.interaction.dispatched.total")
                .setDescription("Total await interactions dispatched")
                .setUnit("interactions")
                .build();
            unitDispatchCompleteCounter = meter.counterBuilder("tpf.await.unit.dispatch_complete.total")
                .setDescription("Total await units whose dispatch completed")
                .setUnit("units")
                .build();
            completionAdmittedCounter = meter.counterBuilder("tpf.await.completion.admitted.total")
                .setDescription("Total await completions admitted")
                .setUnit("completions")
                .build();
            itemCompletedCounter = meter.counterBuilder("tpf.await.item.completed.total")
                .setDescription("Total itemized await completions recorded")
                .setUnit("items")
                .build();
            earlyCompletionHeldCounter = meter.counterBuilder("tpf.await.completion.early_held.total")
                .setDescription("Total itemized await completions held until parent wait was durable")
                .setUnit("completions")
                .build();
            resumeReleasedCounter = meter.counterBuilder("tpf.await.resume.released.total")
                .setDescription("Total await resumes released")
                .setUnit("resumes")
                .build();
            liveHandoffCounter = meter.counterBuilder("tpf.await.live.handoff.total")
                .setDescription("Total await completions handed to a live continuation")
                .setUnit("handoffs")
                .build();
            scalarContinuationCounter = meter.counterBuilder("tpf.await.scalar.continuation.started.total")
                .setDescription("Total scalar await continuations started")
                .setUnit("continuations")
                .build();
            unitTerminalCounter = meter.counterBuilder("tpf.await.unit.terminal.total")
                .setDescription("Total await units moved to a terminal state")
                .setUnit("units")
                .build();
            completionLatencyHistogram = meter.histogramBuilder("tpf.await.completion.latency")
                .setDescription("Time from await interaction creation to completion admission")
                .setUnit("ms")
                .build();
            unitDurationHistogram = meter.histogramBuilder("tpf.await.unit.duration")
                .setDescription("Time from await unit creation to terminal state")
                .setUnit("ms")
                .build();
            admissionOutcomeCounter = meter.counterBuilder("tpf.await.admission.outcomes.total")
                .setDescription("Total durable await admission lifecycle outcomes")
                .setUnit("events")
                .build();
            admissionPendingCounter = meter.upDownCounterBuilder("tpf.await.admission.pending")
                .setDescription("Locally observed durable await admission reservations")
                .setUnit("reservations")
                .build();
            admissionWaitHistogram = meter.histogramBuilder("tpf.await.admission.wait")
                .setDescription("Time spent waiting for a durable await admission reservation")
                .setUnit("ms")
                .build();
            droppedCompletionCounter = meter.counterBuilder("tpf.await.completion.dropped.total")
                .setDescription("Total deterministic await completions dropped by transport consumers")
                .setUnit("events")
                .build();
        }
    }

    private static Attributes interactionAttributes(AwaitInteractionRecord record) {
        AttributesBuilder builder = Attributes.builder();
        put(builder, STEP_ID, record == null ? null : record.stepId());
        put(builder, TRANSPORT, record == null ? null : record.transportType());
        put(builder, STATUS, record == null || record.status() == null ? null : record.status().name());
        return builder.build();
    }

    private static Attributes pendingAttributes(AwaitInteractionRecord record) {
        AttributesBuilder builder = Attributes.builder();
        put(builder, STEP_ID, record == null ? null : record.stepId());
        put(builder, TRANSPORT, record == null ? null : record.transportType());
        return builder.build();
    }

    private static Attributes admissionAttributes(Attributes attributes, String outcome) {
        return attributes.toBuilder().put(AttributeKey.stringKey("tpf.await.admission.outcome"), outcome).build();
    }

    private static Attributes unitAttributes(AwaitUnitRecord unit) {
        return unitAttributes(unit, null);
    }

    private static Attributes unitAttributes(AwaitUnitRecord unit, String transport) {
        AttributesBuilder builder = Attributes.builder();
        put(builder, STEP_ID, unit == null ? null : unit.stepId());
        put(builder, CARDINALITY, unit == null ? null : unit.cardinality());
        put(builder, STATUS, unit == null || unit.status() == null ? null : unit.status().name());
        put(builder, TRANSPORT, transport);
        return builder.build();
    }

    private static Attributes lifecycleAttributes(AwaitReplayView event) {
        AttributesBuilder builder = Attributes.builder();
        put(builder, STEP_ID, event == null ? null : event.stepId());
        put(builder, STATUS, event == null ? null : event.status());
        put(builder, TRANSPORT, event == null ? null : event.transport());
        return builder.build();
    }

    private static void put(AttributesBuilder builder, AttributeKey<String> key, String value) {
        builder.put(key, normalize(value));
    }

    public static synchronized void resetForTest() {
        droppedCompletionCounter = null;
        interactionCreatedCounter = null;
        interactionDispatchedCounter = null;
        unitDispatchCompleteCounter = null;
        completionAdmittedCounter = null;
        itemCompletedCounter = null;
        earlyCompletionHeldCounter = null;
        resumeReleasedCounter = null;
        liveHandoffCounter = null;
        scalarContinuationCounter = null;
        unitTerminalCounter = null;
        completionLatencyHistogram = null;
        unitDurationHistogram = null;
        admissionOutcomeCounter = null;
        admissionPendingCounter = null;
        admissionWaitHistogram = null;
    }

    private static String normalize(String value) {
        if (value == null || value.isBlank()) {
            return "unknown";
        }
        return value;
    }

    private static void emitSpan(String name, AwaitInteractionRecord record, boolean continueOrigin) {
        Span span = startSpan(name, record, continueOrigin);
        try {
            addSpanAttributes(span, record);
        } finally {
            span.end();
        }
    }

    private static Span startSpan(String name, AwaitInteractionRecord record, boolean continueOrigin) {
        var builder = TelemetryRuntimes.global().tracer("org.pipelineframework").spanBuilder(name);
        SpanContext origin = origin(record);
        SpanContext current = Span.current().getSpanContext();
        boolean originLinked = false;
        if (continueOrigin && origin != null && origin.isValid()) {
            builder.setParent(Context.root().with(Span.wrap(origin)));
            originLinked = true;
        } else if (origin != null && origin.isValid()
            && (current == null || !current.isValid() || !sameSpan(current, origin))) {
            builder.addLink(origin);
            originLinked = true;
        } else if (origin != null && origin.isValid() && current != null && current.isValid()) {
            originLinked = sameSpan(current, origin);
        }
        Span span = builder.startSpan();
        addSpanAttributes(span, record);
        span.setAttribute("tpf.await.origin.present", origin != null && origin.isValid());
        span.setAttribute("tpf.await.origin.linked", originLinked);
        return span;
    }

    private static void endOnce(AtomicBoolean terminal, Span span, Throwable failure) {
        if (!terminal.compareAndSet(false, true)) {
            return;
        }
        if (failure != null) {
            span.recordException(failure);
        }
        span.end();
    }

    private static SpanContext origin(AwaitInteractionRecord record) {
        if (record == null || record.transportMetadata() == null) {
            return SpanContext.getInvalid();
        }
        Object traceId = record.transportMetadata().get("tpf.trace.id");
        Object spanId = record.transportMetadata().get("tpf.trace.span_id");
        Object flags = record.transportMetadata().get("tpf.trace.flags");
        if (!(traceId instanceof String trace) || !(spanId instanceof String span)) {
            return SpanContext.getInvalid();
        }
        return SpanContext.createFromRemoteParent(trace, span,
            TraceFlags.fromHex(flags instanceof String value ? value : "01", 0), TraceState.getDefault());
    }

    private static boolean sameSpan(SpanContext first, SpanContext second) {
        return first.getTraceId().equals(second.getTraceId()) && first.getSpanId().equals(second.getSpanId());
    }

    private static void addSpanAttributes(Span span, AwaitInteractionRecord record) {
        span.setAllAttributes(interactionAttributes(record));
        if (record == null) {
            return;
        }
        span.setAttribute("tpf.await.execution_id", record.executionId());
        span.setAttribute("tpf.await.interaction_id", record.interactionId());
        span.setAttribute("tpf.await.correlation_id", record.correlationId());
        span.setAttribute("tpf.await.unit_id", record.unitId());
    }

    public interface AwaitReplayView {
        String stepId();

        String status();

        String transport();
    }
}
