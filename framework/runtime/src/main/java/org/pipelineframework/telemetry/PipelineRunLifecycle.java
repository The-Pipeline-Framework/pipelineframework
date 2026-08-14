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
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import org.pipelineframework.config.ParallelismPolicy;

/** Owns the mutable state of active pipeline runs and their terminal lifecycle. */
final class PipelineRunLifecycle {
    private final TelemetryPolicy policy;
    private final PipelineMetricsRecorder metrics;
    private final PipelineTracingRecorder tracing;
    private final PipelineReplaySupport replay;
    private final RetryAmplificationGuardRuntime retryGuard;
    private final PipelineRetryTelemetry retryTelemetry;
    private final ConcurrentMap<String, PipelineRunContext> activeRuns = new ConcurrentHashMap<>();

    PipelineRunLifecycle(
        TelemetryPolicy policy,
        PipelineMetricsRecorder metrics,
        PipelineTracingRecorder tracing,
        PipelineReplaySupport replay,
        RetryAmplificationGuardRuntime retryGuard,
        PipelineRetryTelemetry retryTelemetry
    ) {
        this.policy = policy;
        this.metrics = metrics;
        this.tracing = tracing;
        this.replay = replay;
        this.retryGuard = retryGuard;
        this.retryTelemetry = retryTelemetry;
    }

    PipelineRunContext start(Object input, int stepCount, ParallelismPolicy parallelism, int maxConcurrency) {
        if (!policy.frameworkEnabled() && !policy.retryAmplificationEnabled()) {
            return PipelineRunContext.disabled();
        }
        String inputKind = input instanceof Multi<?> ? "multi" : "uni";
        metrics.runStarted(inputKind, maxConcurrency);
        PipelineTracingRecorder.TracedRun tracedRun = tracing.startRun(stepCount, parallelism, maxConcurrency, inputKind);
        PipelineRunContext context = new PipelineRunContext(
            UUID.randomUUID().toString(), tracedRun.context(), tracedRun.span(), System.nanoTime(), Instant.now(),
            metrics.runAttributes(inputKind), true, new AtomicLong(), new AtomicLong(), new LongAdder(), new LongAdder(),
            new LongAdder(), new LongAdder(), replay.runParameters(), replay.runState(), new AtomicBoolean(false));
        replay.runStarted(context);
        activeRuns.put(context.runId(), context);
        retryTelemetry.register(context);
        retryGuard.runStarted(context.runId(), trigger -> retryTelemetry.killSwitchTriggered(context, trigger));
        return context;
    }

    Object instrumentCompletion(Object publisher, PipelineRunContext context) {
        if (context == null || !context.enabled()) {
            return publisher;
        }
        if (publisher instanceof Uni<?> uni) {
            return uni.onItemOrFailure().invoke((item, failure) -> finish(context, failure));
        }
        if (publisher instanceof Multi<?> multi) {
            AtomicReference<Throwable> failure = new AtomicReference<>();
            return multi.onFailure().invoke(failure::set)
                .onTermination().invoke(() -> finish(context, failure.get()));
        }
        return publisher;
    }

    void abort(PipelineRunContext context, Throwable failure) {
        if (context != null && context.enabled()) {
            finish(context, failure == null ? new IllegalStateException("Pipeline aborted.") : failure);
        }
    }

    void abortActive(Throwable failure) {
        Throwable effectiveFailure = failure == null ? new IllegalStateException("Pipeline aborted.") : failure;
        List.copyOf(activeRuns.values()).forEach(context -> finish(context, effectiveFailure));
    }

    void finish(PipelineRunContext context, Throwable failure) {
        if (context == null || !context.enabled() || !context.endSignalled().compareAndSet(false, true)) {
            return;
        }
        activeRuns.remove(context.runId(), context);
        retryGuard.runFinished(context.runId());
        long durationMillis = Math.max(0L, Math.round((System.nanoTime() - context.startNanos()) / 1_000_000d));
        metrics.runFinished(context, failure);
        tracing.recordRunInflight(context);
        replay.runFinished(context, durationMillis, failure);
        tracing.finish(context.span(), failure);
        retryTelemetry.unregister(context);
    }

    void shutdown(PipelineRetryTelemetry retryTelemetry) {
        retryGuard.shutdown();
        activeRuns.values().forEach(retryTelemetry::unregister);
        activeRuns.clear();
    }
}
