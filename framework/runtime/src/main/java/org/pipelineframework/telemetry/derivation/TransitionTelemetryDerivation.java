/*
 * Copyright (c) 2026 Mariano Barcia
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */
package org.pipelineframework.telemetry.derivation;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.pipelineframework.telemetry.observation.TransitionObservation;

/** Pure metric/span plans for transition-worker observations. */
public final class TransitionTelemetryDerivation {
    private TransitionTelemetryDerivation() { }

    public static List<MetricSignal> metrics(TransitionObservation observation) {
        return switch (observation) {
            case TransitionObservation.Admitted ignored ->
                List.of(new MetricSignal(Metric.ACTIVE, 1d, Map.of()));
            case TransitionObservation.Released ignored ->
                List.of(new MetricSignal(Metric.ACTIVE, -1d, Map.of()));
            case TransitionObservation.Saturated ignored ->
                List.of(new MetricSignal(Metric.SATURATED, 1d, Map.of()));
            case TransitionObservation.Dispatched ignored ->
                List.of(new MetricSignal(Metric.DISPATCHED, 1d, Map.of()));
            case TransitionObservation.OutcomeRecorded outcome -> List.of(new MetricSignal(Metric.OUTCOME, 1d,
                Map.of("tpf.transition.outcome", outcome.outcome().toLowerCase(Locale.ROOT))));
            case TransitionObservation.DurationRecorded duration ->
                List.of(new MetricSignal(Metric.DURATION, duration.durationNanos() / 1_000_000d, Map.of()));
        };
    }

    public static Optional<SpanPlan> span(TransitionObservation observation) {
        return observation instanceof TransitionObservation.Dispatched
            ? Optional.of(new SpanPlan("tpf.transition.dispatched", Map.of())) : Optional.empty();
    }

    public record MetricSignal(Metric metric, double value, Map<String, String> attributes) {
        public MetricSignal { attributes = Map.copyOf(attributes); }
    }
    public record SpanPlan(String name, Map<String, String> attributes) {
        public SpanPlan { attributes = Map.copyOf(attributes); }
    }
    public enum Metric { ACTIVE, SATURATED, DISPATCHED, OUTCOME, DURATION }
}
