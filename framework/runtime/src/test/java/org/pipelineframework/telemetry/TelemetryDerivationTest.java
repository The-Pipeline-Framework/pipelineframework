/*
 * Copyright (c) 2026 Mariano Barcia
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */
package org.pipelineframework.telemetry;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.pipelineframework.telemetry.derivation.AwaitTelemetryDerivation;
import org.pipelineframework.telemetry.derivation.PipelineSloDerivation;
import org.pipelineframework.telemetry.derivation.RetryTelemetryDerivation;
import org.pipelineframework.telemetry.derivation.RunTelemetryDerivation;
import org.pipelineframework.telemetry.derivation.StepTelemetryDerivation;
import org.pipelineframework.telemetry.derivation.TransitionTelemetryDerivation;
import org.pipelineframework.telemetry.observation.RunObservation;
import org.pipelineframework.telemetry.observation.RetryObservation;
import org.pipelineframework.telemetry.observation.StepObservation;
import org.pipelineframework.telemetry.observation.TransitionObservation;

class TelemetryDerivationTest {
    private static final Map<String, String> STEP_ATTRIBUTES = Map.of("tpf.step.class", "example.Step");
    private static final StepObservation.Context STEP = new StepObservation.Context("example.Step", true);

    @Test
    void stepCancellationHasOneConsistentNonErrorMeaningAcrossSinks() {
        var started = StepTelemetryDerivation.started(
            new StepObservation.Started(STEP, Instant.EPOCH), STEP_ATTRIBUTES, STEP_ATTRIBUTES);
        var signals = StepTelemetryDerivation.terminal(
            new StepObservation.Cancelled(STEP, 2_000_000L, Instant.EPOCH), STEP_ATTRIBUTES);

        assertEquals("example.Step", started.metric().stepClass());
        assertFalse(signals.metric().error());
        assertEquals("example.Step", signals.metric().stepClass());
        assertEquals(2d, signals.metric().durationMillis());
        assertTrue(signals.span().failure().isEmpty());
        assertTrue(signals.span().cancelled());
        assertEquals(StepTelemetryDerivation.ReplayOutcome.CANCELLED, signals.replay().outcome());
        assertTrue(signals.replay().failure().isEmpty());
        assertEquals("example.Step", signals.retrySafety().stepClass());
    }

    @Test
    void stepFailureHasOneConsistentErrorMeaningAcrossSinks() {
        IllegalStateException failure = new IllegalStateException("boom");
        var signals = StepTelemetryDerivation.terminal(
            new StepObservation.Failed(STEP, 1_000_000L, failure, Instant.EPOCH), STEP_ATTRIBUTES);

        assertTrue(signals.metric().error());
        assertEquals("example.Step", signals.metric().stepClass());
        assertSame(failure, signals.span().failure().orElseThrow());
        assertEquals(StepTelemetryDerivation.ReplayOutcome.FAILURE, signals.replay().outcome());
        assertSame(failure, signals.replay().failure().orElseThrow());
    }

    @Test
    void completedStepAndRunDeriveSuccessWithoutErrorMetadata() {
        var step = StepTelemetryDerivation.terminal(
            new StepObservation.Completed(STEP, 1L, Instant.EPOCH), STEP_ATTRIBUTES);
        var run = RunTelemetryDerivation.terminal(new RunObservation.Completed("run", 42L, Instant.EPOCH));

        assertFalse(step.metric().error());
        assertEquals(StepTelemetryDerivation.ReplayOutcome.SUCCESS, step.replay().outcome());
        assertFalse(run.metric().error());
        assertEquals(RunTelemetryDerivation.ReplayOutcome.SUCCESS, run.replay().outcome());
    }

    @Test
    void cancelledRunIsExplicitAndDoesNotBecomeAnError() {
        var signals = RunTelemetryDerivation.terminal(
            new RunObservation.Cancelled("run", 42L, Instant.EPOCH));

        assertFalse(signals.metric().error());
        assertTrue(signals.span().cancelled());
        assertTrue(signals.span().failure().isEmpty());
        assertEquals(RunTelemetryDerivation.ReplayOutcome.SUCCESS, signals.replay().outcome(),
            "The current replay protocol has no run-cancel event, so its existing completion contract is preserved");
    }

    @Test
    void transitionDispatchDerivesExistingMetricAndSpanMeaning() {
        var observation = new TransitionObservation.Dispatched(Instant.EPOCH);
        assertEquals(TransitionTelemetryDerivation.Metric.DISPATCHED,
            TransitionTelemetryDerivation.metrics(observation).getFirst().metric());
        assertEquals("tpf.transition.dispatched",
            TransitionTelemetryDerivation.span(observation).orElseThrow().name());
    }

    @Test
    void awaitCompletionDerivesMetricAndExistingSpanName() {
        AwaitObservation.Context context = new AwaitObservation.Context(
            Optional.of("approval"), Optional.of("REST"), Optional.of("COMPLETED"),
            Optional.of("ONE_TO_ONE"), Optional.of("execution"), Optional.of("interaction"),
            Optional.of("correlation"), Optional.of("unit"), Map.of());
        AwaitObservation.CompletionAdmitted observation =
            new AwaitObservation.CompletionAdmitted(context, 12L, Instant.EPOCH);

        assertEquals(2, AwaitTelemetryDerivation.metrics(observation).size());
        assertEquals("tpf.await.completion.admitted",
            AwaitTelemetryDerivation.span(observation).orElseThrow().name());
    }

    @Test
    void awaitAdmissionDerivesReconciledWaitedAndPendingSignals() {
        AwaitObservation.Context context = new AwaitObservation.Context(
            Optional.of("approval"), Optional.of("REST"), Optional.of("WAITING"),
            Optional.of("ONE_TO_ONE"), Optional.of("execution"), Optional.of("interaction"),
            Optional.of("correlation"), Optional.of("unit"), Map.of());
        var acquired = AwaitTelemetryDerivation.metrics(new AwaitObservation.AdmissionAcquired(
            context, false, true, 12L, true, Instant.EPOCH));
        var released = AwaitTelemetryDerivation.metrics(new AwaitObservation.AdmissionReleased(
            context, true, true, Instant.EPOCH));

        assertEquals(5, acquired.size());
        assertTrue(acquired.stream().anyMatch(signal ->
            signal.metric() == AwaitTelemetryDerivation.Metric.ADMISSION_OUTCOME
                && "reconciled".equals(signal.attributes().get("tpf.await.admission.outcome"))));
        assertTrue(acquired.stream().anyMatch(signal ->
            signal.metric() == AwaitTelemetryDerivation.Metric.ADMISSION_OUTCOME
                && "waited".equals(signal.attributes().get("tpf.await.admission.outcome"))));
        assertTrue(acquired.stream().anyMatch(signal ->
            signal.metric() == AwaitTelemetryDerivation.Metric.ADMISSION_PENDING && signal.value() == 1d));
        assertEquals(2, released.size());
        assertTrue(released.stream().anyMatch(signal ->
            signal.metric() == AwaitTelemetryDerivation.Metric.ADMISSION_PENDING && signal.value() == -1d));
    }

    @Test
    void sloClassificationIsPureAndDeterministic() {
        assertTrue(PipelineSloDerivation.throughput(Map.of("boundary", "x"), 10, 60_000, 10)
            .orElseThrow().good());
        var success = PipelineSloDerivation.success(Map.of("boundary", "x"), 10, 8).orElseThrow();
        assertEquals(10, success.total());
        assertEquals(8, success.good());
    }

    @Test
    void sloClassificationRejectsInvalidNumericInputs() {
        Map<String, String> attributes = Map.of("boundary", "x");

        assertTrue(PipelineSloDerivation.throughput(attributes, -1, 1d, 1d).isEmpty());
        assertTrue(PipelineSloDerivation.throughput(attributes, 1, Double.NaN, 1d).isEmpty());
        assertTrue(PipelineSloDerivation.throughput(attributes, 1, Double.POSITIVE_INFINITY, 1d).isEmpty());
        assertTrue(PipelineSloDerivation.throughput(attributes, 1, 1d, Double.NaN).isEmpty());
        assertTrue(PipelineSloDerivation.success(attributes, 1, -1).isEmpty());
    }

    @Test
    void retryReplayFailureIsExplicitlyOptional() {
        var withoutFailure = RetryTelemetryDerivation.derive(
            new RetryObservation("example.Step", null, Instant.EPOCH), STEP_ATTRIBUTES);
        IllegalStateException failure = new IllegalStateException("retry");
        var withFailure = RetryTelemetryDerivation.derive(
            new RetryObservation("example.Step", failure, Instant.EPOCH), STEP_ATTRIBUTES);

        assertTrue(withoutFailure.replay().failure().isEmpty());
        assertSame(failure, withFailure.replay().failure().orElseThrow());
    }
}
