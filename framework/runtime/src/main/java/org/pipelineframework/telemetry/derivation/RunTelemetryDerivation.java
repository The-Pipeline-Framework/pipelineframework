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
import org.pipelineframework.telemetry.observation.RunObservation;

/** Pure sink-specific interpretation of run observations. */
public final class RunTelemetryDerivation {
    private RunTelemetryDerivation() { }

    public static StartedSignals started(RunObservation.Started observation, Map<String, String> metricAttributes,
                                         Map<String, String> spanAttributes) {
        return new StartedSignals(new MetricStarted(metricAttributes, observation.maxConcurrency()),
            new SpanStarted("tpf.pipeline.run", spanAttributes, observation.stepCount(), observation.parallelism(),
                observation.maxConcurrency()));
    }

    public static TerminalSignals terminal(RunObservation observation) {
        return switch (observation) {
            case RunObservation.Started ignored ->
                throw new IllegalArgumentException("A terminal run observation is required");
            case RunObservation.Completed completed ->
                terminal(completed.durationMillis(), Optional.empty(), false);
            case RunObservation.Failed failed ->
                terminal(failed.durationMillis(), Optional.of(failed.failure()), false);
            case RunObservation.Cancelled cancelled ->
                terminal(cancelled.durationMillis(), Optional.empty(), true);
        };
    }

    public static InflightSignal inflight(long maximum, long sum, long samples) {
        return new InflightSignal(maximum, samples > 0L ? sum / (double) samples : 0d);
    }

    private static TerminalSignals terminal(long durationMillis, Optional<Throwable> failure, boolean cancelled) {
        return new TerminalSignals(new MetricFinished(durationMillis, failure.isPresent()),
            new SpanFinished(failure, cancelled), new ReplayFinished(durationMillis,
            failure.isPresent() ? ReplayOutcome.FAILURE : ReplayOutcome.SUCCESS,
            failure));
    }

    public record StartedSignals(MetricStarted metric, SpanStarted span) { }
    public record TerminalSignals(MetricFinished metric, SpanFinished span, ReplayFinished replay) { }
    public record MetricStarted(Map<String, String> attributes, int maxConcurrency) {
        public MetricStarted { attributes = Map.copyOf(attributes); }
    }
    public record MetricFinished(long durationMillis, boolean error) {
        public MetricFinished { durationMillis = Math.max(0L, durationMillis); }
    }
    public record SpanStarted(String name, Map<String, String> attributes, int stepCount,
                              String parallelism, int maxConcurrency) {
        public SpanStarted { Objects.requireNonNull(name, "name"); attributes = Map.copyOf(attributes); }
    }
    public record SpanFinished(Optional<Throwable> failure, boolean cancelled) {
        public SpanFinished { Objects.requireNonNull(failure, "failure"); }
    }
    public record ReplayFinished(long durationMillis, ReplayOutcome outcome, Optional<Throwable> failure) {
        public ReplayFinished { Objects.requireNonNull(outcome, "outcome"); Objects.requireNonNull(failure, "failure"); }
    }
    public record InflightSignal(long maximum, double average) { }
    public enum ReplayOutcome { SUCCESS, FAILURE }
}
