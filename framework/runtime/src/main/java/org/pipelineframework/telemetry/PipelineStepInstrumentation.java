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

import org.pipelineframework.telemetry.PipelineRunContext;
import io.opentelemetry.api.trace.Span;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Reactive lifecycle decorator for a single pipeline step subscription. */
final class PipelineStepInstrumentation {
    private final PipelineMetricsRecorder metrics;
    private final PipelineTracingRecorder tracing;
    private final PipelineReplaySupport replay;
    private final RetryAmplificationGuardRuntime retryGuard;

    PipelineStepInstrumentation(
        PipelineMetricsRecorder metrics,
        PipelineTracingRecorder tracing,
        PipelineReplaySupport replay,
        RetryAmplificationGuardRuntime retryGuard
    ) {
        this.metrics = metrics;
        this.tracing = tracing;
        this.replay = replay;
        this.retryGuard = retryGuard;
    }

    <T> Uni<T> instrument(
        Class<?> stepClass, Uni<T> result, PipelineRunContext runContext, boolean perItem,
        ExecutionReplayTracker.StepExecutionScope replayScope
    ) {
        if (runContext == null || !runContext.enabled()) {
            return result;
        }
        return Uni.createFrom().deferred(() -> {
            Span span = replayScope != null ? replayScope.span() : tracing.startStep(stepClass, runContext, perItem);
            long started = System.nanoTime();
            AtomicBoolean terminal = new AtomicBoolean();
            started(stepClass, runContext);
            return result.onItemOrFailure().invoke((item, failure) ->
                finish(terminal, stepClass, runContext, replayScope, span, started, failure, false))
                .onCancellation().invoke(() -> finish(terminal, stepClass, runContext, replayScope, span, started,
                    new CancellationException("Step Uni cancelled"), true));
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
            Span span = replayScope != null ? replayScope.span() : tracing.startStep(stepClass, runContext, perItem);
            long started = System.nanoTime();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            AtomicBoolean terminal = new AtomicBoolean();
            started(stepClass, runContext);
            return result.onFailure().invoke(failure::set)
                .onTermination().invoke(() -> finish(terminal, stepClass, runContext, replayScope, span, started,
                    failure.get(), false))
                .onCancellation().invoke(() -> finish(terminal, stepClass, runContext, replayScope, span, started,
                    new CancellationException("Step Multi cancelled"), true));
        });
    }

    private void started(Class<?> stepClass, PipelineRunContext runContext) {
        metrics.stepStarted(stepClass, runContext);
        retryGuard.itemStarted(PipelineMetricAttributes.resolveStepClassName(stepClass));
    }

    private void finish(
        AtomicBoolean terminal, Class<?> stepClass, PipelineRunContext runContext,
        ExecutionReplayTracker.StepExecutionScope replayScope, Span span, long started,
        Throwable failure, boolean cancelled
    ) {
        if (!terminal.compareAndSet(false, true)) {
            return;
        }
        metrics.stepFinished(stepClass, runContext, started, failure, cancelled);
        retryGuard.itemEnded(PipelineMetricAttributes.resolveStepClassName(stepClass));
        if (replayScope != null) {
            replay.complete(replayScope, failure, cancelled);
            return;
        }
        tracing.finish(span, cancelled ? null : failure);
    }
}
