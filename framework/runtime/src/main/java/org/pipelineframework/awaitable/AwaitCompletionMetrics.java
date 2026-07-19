package org.pipelineframework.awaitable;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongUpDownCounter;
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
    private static volatile LongCounter interactionDispatchedCounter;
    private static volatile LongCounter unitDispatchCompleteCounter;
    private static volatile LongCounter completionAdmittedCounter;
    private static volatile LongCounter itemCompletedCounter;
    private static volatile LongCounter earlyCompletionHeldCounter;
    private static volatile LongCounter resumeReleasedCounter;
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
            var meter = GlobalOpenTelemetry.getMeter("org.pipelineframework");
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
        interactionDispatchedCounter = null;
        unitDispatchCompleteCounter = null;
        completionAdmittedCounter = null;
        itemCompletedCounter = null;
        earlyCompletionHeldCounter = null;
        resumeReleasedCounter = null;
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

    public interface AwaitReplayView {
        String stepId();

        String status();

        String transport();
    }
}
