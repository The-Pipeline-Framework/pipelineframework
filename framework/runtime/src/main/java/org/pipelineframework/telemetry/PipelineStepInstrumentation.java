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

import io.opentelemetry.api.trace.Span;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.pipelineframework.telemetry.derivation.StepTelemetryDerivation;
import org.pipelineframework.telemetry.observation.StepObservation;

/** Reactive lifecycle decorator for a single pipeline step subscription. */
final class PipelineStepInstrumentation {
    private final PipelineMetricsRecorder metrics;
    private final PipelineTracingRecorder tracing;
    private final PipelineReplaySupport replay;
    private final RetryAmplificationGuardRuntime retryGuard;
    private final PipelineMetricAttributes attributes;
    private final PipelineSpanAttributes spanAttributes;

    PipelineStepInstrumentation(
        PipelineMetricsRecorder metrics,
        PipelineTracingRecorder tracing,
        PipelineReplaySupport replay,
        RetryAmplificationGuardRuntime retryGuard,
        PipelineMetricAttributes attributes,
        PipelineSpanAttributes spanAttributes
    ) {
        this.metrics = metrics;
        this.tracing = tracing;
        this.replay = replay;
        this.retryGuard = retryGuard;
        this.attributes = attributes;
        this.spanAttributes = spanAttributes;
    }

    <T> Uni<T> instrument(
        Class<?> stepClass, Uni<T> result, PipelineRunContext runContext, boolean perItem,
        ExecutionReplayTracker.StepExecutionScope replayScope
    ) {
        if (runContext == null || !runContext.enabled()) {
            return result;
        }
        return Uni.createFrom().deferred(() -> {
            StepObservation.Started observation = started(stepClass, perItem);
            StepTelemetryDerivation.StartedSignals signals = StepTelemetryDerivation.started(
                observation, attributes.step(observation.context().stepClass()),
                spanAttributes.step(observation.context().stepClass()));
            Span span = replayScope != null ? replayScope.span() : tracing.startStep(signals.span(), runContext);
            long started = System.nanoTime();
            AtomicBoolean terminal = new AtomicBoolean();
            metrics.record(signals.metric(), runContext);
            retryGuard.itemStarted(signals.retrySafety().stepClass());
            return result.onItemOrFailure().invoke((item, failure) ->
                finish(terminal, observation.context(), runContext, replayScope, span, started, failure, false))
                .onCancellation().invoke(() -> finish(terminal, observation.context(), runContext, replayScope, span,
                    started, null, true));
        });
    }

    <T> Multi<T> instrument(
        Class<?> stepClass, Multi<T> result, PipelineRunContext runContext, boolean perItem,
        ExecutionReplayTracker.StepExecutionScope replayScope
    ) {
        if (runContext == null || !runContext.enabled()) {
            return result;
        }
        return Multi.createFrom().deferred(() -> {
            StepObservation.Started observation = started(stepClass, perItem);
            StepTelemetryDerivation.StartedSignals signals = StepTelemetryDerivation.started(
                observation, attributes.step(observation.context().stepClass()),
                spanAttributes.step(observation.context().stepClass()));
            Span span = replayScope != null ? replayScope.span() : tracing.startStep(signals.span(), runContext);
            long started = System.nanoTime();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            AtomicBoolean terminal = new AtomicBoolean();
            metrics.record(signals.metric(), runContext);
            retryGuard.itemStarted(signals.retrySafety().stepClass());
            return result.onFailure().invoke(failure::set)
                .onTermination().invoke(() -> finish(terminal, observation.context(), runContext, replayScope, span, started,
                    failure.get(), false))
                .onCancellation().invoke(() -> finish(terminal, observation.context(), runContext, replayScope, span,
                    started, null, true));
        });
    }

    private static StepObservation.Started started(Class<?> stepClass, boolean perItem) {
        return new StepObservation.Started(new StepObservation.Context(
            PipelineMetricAttributes.resolveStepClassName(stepClass), perItem), Instant.now());
    }

    private void finish(
        AtomicBoolean terminal, StepObservation.Context stepContext, PipelineRunContext runContext,
        ExecutionReplayTracker.StepExecutionScope replayScope, Span span, long started,
        Throwable failure, boolean cancelled
    ) {
        if (!terminal.compareAndSet(false, true)) {
            return;
        }
        long duration = System.nanoTime() - started;
        StepObservation observation = cancelled
            ? new StepObservation.Cancelled(stepContext, duration, Instant.now())
            : failure == null
                ? new StepObservation.Completed(stepContext, duration, Instant.now())
                : new StepObservation.Failed(stepContext, duration, failure, Instant.now());
        StepTelemetryDerivation.TerminalSignals signals = StepTelemetryDerivation.terminal(
            observation, attributes.step(stepContext.stepClass()));
        metrics.record(signals.metric(), runContext);
        retryGuard.itemEnded(signals.retrySafety().stepClass());
        if (replayScope != null) {
            replay.complete(replayScope, signals.replay());
            return;
        }
        tracing.finish(span, signals.span());
    }
}
