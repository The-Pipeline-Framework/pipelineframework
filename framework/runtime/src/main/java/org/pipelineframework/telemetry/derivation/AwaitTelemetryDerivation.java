/*
 * Copyright (c) 2026 Mariano Barcia
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */
package org.pipelineframework.telemetry.derivation;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.pipelineframework.telemetry.AwaitObservation;
import org.pipelineframework.telemetry.AwaitTelemetryAttributes;

/** Pure metric and span plans for Await observations. */
public final class AwaitTelemetryDerivation {
    private AwaitTelemetryDerivation() { }

    public static List<MetricSignal> metrics(AwaitObservation observation) {
        Map<String, String> attributes = AwaitTelemetryAttributes.metricAttributes(observation);
        List<MetricSignal> signals = new ArrayList<>();
        if (observation instanceof AwaitObservation.InteractionCreated) add(signals, Metric.INTERACTION_CREATED, attributes);
        else if (observation instanceof AwaitObservation.InteractionDispatched) add(signals, Metric.INTERACTION_DISPATCHED, attributes);
        else if (observation instanceof AwaitObservation.UnitDispatchCompleted) add(signals, Metric.UNIT_DISPATCH_COMPLETED, attributes);
        else if (observation instanceof AwaitObservation.CompletionAdmitted value) {
            add(signals, Metric.COMPLETION_ADMITTED, attributes);
            if (value.latencyMillis() > 0L) add(signals, Metric.COMPLETION_LATENCY, value.latencyMillis(), attributes);
        } else if (observation instanceof AwaitObservation.LiveHandoff) add(signals, Metric.LIVE_HANDOFF, attributes);
        else if (observation instanceof AwaitObservation.ScalarContinuationStarted) add(signals, Metric.SCALAR_CONTINUATION, attributes);
        else if (observation instanceof AwaitObservation.ItemCompleted) add(signals, Metric.ITEM_COMPLETED, attributes);
        else if (observation instanceof AwaitObservation.EarlyCompletionHeld) add(signals, Metric.EARLY_HELD, attributes);
        else if (observation instanceof AwaitObservation.ResumeReleased) add(signals, Metric.RESUME_RELEASED, attributes);
        else if (observation instanceof AwaitObservation.UnitTerminal value) {
            add(signals, Metric.UNIT_TERMINAL, attributes);
            if (value.durationMillis() > 0L) add(signals, Metric.UNIT_DURATION, value.durationMillis(), attributes);
        } else if (observation instanceof AwaitObservation.CompletionDropped) add(signals, Metric.DROPPED, attributes);
        else if (observation instanceof AwaitObservation.AdmissionAcquired value) {
            add(signals, Metric.ADMISSION_OUTCOME, attributes);
            if (value.locallyTracked()) add(signals, Metric.ADMISSION_PENDING, 1d, attributes);
            if (value.reconciled()) add(signals, Metric.ADMISSION_OUTCOME, outcome(attributes, "reconciled"));
            if (value.waitMillis() > 0L) {
                add(signals, Metric.ADMISSION_OUTCOME, outcome(attributes, "waited"));
                add(signals, Metric.ADMISSION_WAIT, value.waitMillis(), attributes);
            }
        } else if (observation instanceof AwaitObservation.AdmissionReleased value && value.released()) {
            add(signals, Metric.ADMISSION_OUTCOME, attributes);
            if (value.locallyTracked()) add(signals, Metric.ADMISSION_PENDING, -1d, attributes);
        }
        return List.copyOf(signals);
    }

    public static Optional<SpanPlan> span(AwaitObservation observation) {
        String name;
        LinkMode linkMode = LinkMode.LINK_DURABLE_ORIGIN;
        if (observation instanceof AwaitObservation.InteractionCreated) name = "tpf.await.interaction.created";
        else if (observation instanceof AwaitObservation.ProviderDispatched) {
            name = "tpf.await.provider.dispatch";
            linkMode = LinkMode.CONTINUE_DURABLE_ORIGIN;
        } else if (observation instanceof AwaitObservation.ProviderAdmitted) name = "tpf.await.provider.admitted";
        else if (observation instanceof AwaitObservation.ProviderCompletionDispatched) name = "tpf.await.provider.completion.dispatched";
        else if (observation instanceof AwaitObservation.CompletionAdmitted) name = "tpf.await.completion.admitted";
        else if (observation instanceof AwaitObservation.LiveHandoff) name = "tpf.await.live.handoff";
        else if (observation instanceof AwaitObservation.ScalarContinuationStarted) name = "tpf.await.scalar.continuation";
        else return Optional.empty();
        return Optional.of(new SpanPlan(name, AwaitTelemetryAttributes.spanAttributes(observation),
            context(observation), linkMode));
    }

    public static Optional<AwaitObservation.Context> context(AwaitObservation observation) {
        if (observation instanceof AwaitObservation.CompletionDropped) return Optional.empty();
        if (observation instanceof AwaitObservation.InteractionCreated value) return Optional.of(value.context());
        if (observation instanceof AwaitObservation.InteractionDispatched value) return Optional.of(value.context());
        if (observation instanceof AwaitObservation.ProviderDispatched value) return Optional.of(value.context());
        if (observation instanceof AwaitObservation.ProviderAdmitted value) return Optional.of(value.context());
        if (observation instanceof AwaitObservation.ProviderCompletionDispatched value) return Optional.of(value.context());
        if (observation instanceof AwaitObservation.CompletionAdmitted value) return Optional.of(value.context());
        if (observation instanceof AwaitObservation.LiveHandoff value) return Optional.of(value.context());
        if (observation instanceof AwaitObservation.ScalarContinuationStarted value) return Optional.of(value.context());
        if (observation instanceof AwaitObservation.UnitDispatchCompleted value) return Optional.of(value.context());
        if (observation instanceof AwaitObservation.ItemCompleted value) return Optional.of(value.context());
        if (observation instanceof AwaitObservation.EarlyCompletionHeld value) return Optional.of(value.context());
        if (observation instanceof AwaitObservation.ResumeReleased value) return Optional.of(value.context());
        if (observation instanceof AwaitObservation.UnitTerminal value) return Optional.of(value.context());
        if (observation instanceof AwaitObservation.AdmissionAcquired value) return Optional.of(value.context());
        return Optional.of(((AwaitObservation.AdmissionReleased) observation).context());
    }

    private static Map<String, String> outcome(Map<String, String> attributes, String outcome) {
        Map<String, String> result = new LinkedHashMap<>(attributes);
        result.put("tpf.await.admission.outcome", outcome);
        return Map.copyOf(result);
    }

    private static void add(List<MetricSignal> signals, Metric metric, Map<String, String> attributes) {
        add(signals, metric, 1d, attributes);
    }

    private static void add(List<MetricSignal> signals, Metric metric, double value, Map<String, String> attributes) {
        signals.add(new MetricSignal(metric, value, attributes));
    }

    public record MetricSignal(Metric metric, double value, Map<String, String> attributes) {
        public MetricSignal { attributes = Map.copyOf(attributes); }
    }

    public record SpanPlan(String name, Map<String, String> attributes,
                           Optional<AwaitObservation.Context> context, LinkMode linkMode) {
        public SpanPlan { attributes = Map.copyOf(attributes); }
    }

    public enum LinkMode { CONTINUE_DURABLE_ORIGIN, LINK_DURABLE_ORIGIN }
    public enum Metric {
        DROPPED, INTERACTION_CREATED, INTERACTION_DISPATCHED, UNIT_DISPATCH_COMPLETED,
        COMPLETION_ADMITTED, COMPLETION_LATENCY, ITEM_COMPLETED, EARLY_HELD, RESUME_RELEASED,
        LIVE_HANDOFF, SCALAR_CONTINUATION, UNIT_TERMINAL, UNIT_DURATION, ADMISSION_OUTCOME,
        ADMISSION_PENDING, ADMISSION_WAIT
    }
}
