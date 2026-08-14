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

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import org.pipelineframework.config.ParallelismPolicy;
import org.pipelineframework.telemetry.derivation.RunTelemetryDerivation;
import org.pipelineframework.telemetry.derivation.PipelineSloDerivation;
import org.pipelineframework.telemetry.observation.RunObservation;

/** Owns the mutable state of active pipeline runs and their terminal lifecycle. */
final class PipelineRunLifecycle {
    private final TelemetryPolicy policy;
    private final PipelineMetricsRecorder metrics;
    private final PipelineTracingRecorder tracing;
    private final PipelineReplaySupport replay;
    private final RetryAmplificationGuardRuntime retryGuard;
    private final PipelineRetryTelemetry retryTelemetry;
    private final PipelineMetricAttributes attributes;
    private final PipelineSpanAttributes spanAttributes;
    private final ConcurrentMap<String, PipelineRunContext> activeRuns = new ConcurrentHashMap<>();

    PipelineRunLifecycle(
        TelemetryPolicy policy,
        PipelineMetricsRecorder metrics,
        PipelineTracingRecorder tracing,
        PipelineReplaySupport replay,
        RetryAmplificationGuardRuntime retryGuard,
        PipelineRetryTelemetry retryTelemetry,
        PipelineMetricAttributes attributes,
        PipelineSpanAttributes spanAttributes
    ) {
        this.policy = policy;
        this.metrics = metrics;
        this.tracing = tracing;
        this.replay = replay;
        this.retryGuard = retryGuard;
        this.retryTelemetry = retryTelemetry;
        this.attributes = attributes;
        this.spanAttributes = spanAttributes;
    }

    PipelineRunContext start(Object input, int stepCount, ParallelismPolicy parallelism, int maxConcurrency) {
        if (!policy.frameworkEnabled() && !policy.retryAmplificationEnabled()) {
            return PipelineRunContext.disabled();
        }
        String inputKind = input instanceof Multi<?> ? "multi" : "uni";
        String runId = UUID.randomUUID().toString();
        RunObservation.Started observation = new RunObservation.Started(runId, inputKind, stepCount,
            parallelism == null ? "AUTO" : parallelism.name(), maxConcurrency, Instant.now());
        var metricAttributes = attributes.run(inputKind);
        RunTelemetryDerivation.StartedSignals signals = RunTelemetryDerivation.started(
            observation, metricAttributes, spanAttributes.run(inputKind));
        metrics.record(signals.metric());
        PipelineTracingRecorder.TracedRun tracedRun = tracing.startRun(signals.span());
        PipelineRunContext context = new PipelineRunContext(
            runId, tracedRun.context(), tracedRun.span(), System.nanoTime(), observation.occurredAt(),
            TelemetrySdkAttributes.from(metricAttributes), true, new AtomicLong(), new AtomicLong(), new LongAdder(), new LongAdder(),
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
            return uni.onItemOrFailure().invoke((item, failure) -> finish(context, Optional.ofNullable(failure), false))
                .onCancellation().invoke(() -> finish(context, Optional.empty(), true));
        }
        if (publisher instanceof Multi<?> multi) {
            AtomicReference<Throwable> failure = new AtomicReference<>();
            return multi.onFailure().invoke(failure::set)
                .onTermination().invoke(() -> finish(context, Optional.ofNullable(failure.get()), false))
                .onCancellation().invoke(() -> finish(context, Optional.empty(), true));
        }
        return publisher;
    }

    void abort(PipelineRunContext context, Throwable failure) {
        if (context != null && context.enabled()) {
            finish(context, Optional.of(failure == null ? new IllegalStateException("Pipeline aborted.") : failure), false);
        }
    }

    void abortActive(Throwable failure) {
        Throwable effectiveFailure = failure == null ? new IllegalStateException("Pipeline aborted.") : failure;
        List.copyOf(activeRuns.values()).forEach(context -> finish(context, Optional.of(effectiveFailure), false));
    }

    void finish(PipelineRunContext context, Optional<Throwable> failure, boolean cancelled) {
        if (context == null || !context.enabled() || !context.endSignalled().compareAndSet(false, true)) {
            return;
        }
        activeRuns.remove(context.runId(), context);
        retryGuard.runFinished(context.runId());
        long durationMillis = Math.max(0L, Math.round((System.nanoTime() - context.startNanos()) / 1_000_000d));
        RunObservation observation = cancelled
            ? new RunObservation.Cancelled(context.runId(), durationMillis, Instant.now())
            : failure.<RunObservation>map(error ->
                new RunObservation.Failed(context.runId(), durationMillis, error, Instant.now()))
                .orElseGet(() -> new RunObservation.Completed(context.runId(), durationMillis, Instant.now()));
        RunTelemetryDerivation.TerminalSignals signals = RunTelemetryDerivation.terminal(observation);
        metrics.record(signals.metric(), context);
        attributes.sloBoundary().flatMap(boundary -> PipelineSloDerivation.throughput(
            boundary, context.itemsConsumed().sum(), signals.metric().durationMillis(),
            TelemetrySloConfig.itemThroughputPerMinute())).ifPresent(metrics::record);
        attributes.sloBoundary().flatMap(boundary -> PipelineSloDerivation.success(
            boundary, context.itemsConsumed().sum(), context.itemsProduced().sum())).ifPresent(metrics::record);
        tracing.recordRunInflight(context, RunTelemetryDerivation.inflight(
            context.inflightMax().get(), context.inflightSum().sum(), context.inflightSamples().sum()));
        replay.runFinished(context, signals.replay());
        tracing.finish(context.span(), signals.span());
        retryTelemetry.unregister(context);
    }

    void shutdown(PipelineRetryTelemetry retryTelemetry) {
        retryGuard.shutdown();
        activeRuns.values().forEach(retryTelemetry::unregister);
        activeRuns.clear();
    }
}
