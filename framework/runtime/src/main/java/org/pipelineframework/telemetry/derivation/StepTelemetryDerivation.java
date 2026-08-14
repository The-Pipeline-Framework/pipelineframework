/*
 * Copyright (c) 2026 Mariano Barcia
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */
package org.pipelineframework.telemetry.derivation;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.pipelineframework.telemetry.observation.StepObservation;

/** Pure sink-specific interpretation of step observations. */
public final class StepTelemetryDerivation {
    private StepTelemetryDerivation() { }

    public static StartedSignals started(StepObservation.Started observation, Map<String, String> metricAttributes,
                                         Map<String, String> spanAttributes) {
        RetrySafetyStarted retrySafety = new RetrySafetyStarted(observation.context().stepClass());
        return new StartedSignals(
            new MetricStarted(metricAttributes, retrySafety.stepClass()),
            new SpanStarted("tpf.step", spanAttributes, observation.context().perItem()),
            retrySafety);
    }

    public static TerminalSignals terminal(StepObservation observation, Map<String, String> metricAttributes) {
        if (observation instanceof StepObservation.Completed completed) {
            return terminal(metricAttributes, completed.durationNanos(), false, false, Optional.empty(),
                completed.context().stepClass());
        }
        if (observation instanceof StepObservation.Failed failed) {
            return terminal(metricAttributes, failed.durationNanos(), true, false, Optional.of(failed.failure()),
                failed.context().stepClass());
        }
        if (observation instanceof StepObservation.Cancelled cancelled) {
            return terminal(metricAttributes, cancelled.durationNanos(), false, true, Optional.empty(),
                cancelled.context().stepClass());
        }
        throw new IllegalArgumentException("A terminal step observation is required");
    }

    private static TerminalSignals terminal(Map<String, String> attributes, long durationNanos, boolean error,
                                              boolean cancelled, Optional<Throwable> failure, String stepClass) {
        RetrySafetyFinished retrySafety = new RetrySafetyFinished(stepClass);
        return new TerminalSignals(
            new MetricFinished(attributes, durationNanos / 1_000_000d, error, retrySafety.stepClass()),
            new SpanFinished(failure, cancelled),
            new ReplayFinished(cancelled ? ReplayOutcome.CANCELLED : error ? ReplayOutcome.FAILURE : ReplayOutcome.SUCCESS,
                failure),
            retrySafety);
    }

    public record StartedSignals(MetricStarted metric, SpanStarted span, RetrySafetyStarted retrySafety) { }
    public record TerminalSignals(MetricFinished metric, SpanFinished span, ReplayFinished replay,
                                  RetrySafetyFinished retrySafety) { }
    public record MetricStarted(Map<String, String> attributes, String stepClass) {
        public MetricStarted { attributes = Map.copyOf(attributes); Objects.requireNonNull(stepClass, "stepClass"); }
    }
    public record MetricFinished(Map<String, String> attributes, double durationMillis, boolean error,
                                 String stepClass) {
        public MetricFinished {
            attributes = Map.copyOf(attributes);
            durationMillis = Math.max(0d, durationMillis);
            Objects.requireNonNull(stepClass, "stepClass");
        }
    }
    public record SpanStarted(String name, Map<String, String> attributes, boolean perItem) {
        public SpanStarted { Objects.requireNonNull(name, "name"); attributes = Map.copyOf(attributes); }
    }
    public record SpanFinished(Optional<Throwable> failure, boolean cancelled) {
        public SpanFinished { Objects.requireNonNull(failure, "failure"); }
    }
    public record ReplayFinished(ReplayOutcome outcome, Optional<Throwable> failure) {
        public ReplayFinished { Objects.requireNonNull(outcome, "outcome"); Objects.requireNonNull(failure, "failure"); }
    }
    public record RetrySafetyStarted(String stepClass) { }
    public record RetrySafetyFinished(String stepClass) { }
    public enum ReplayOutcome { SUCCESS, FAILURE, CANCELLED }
}
