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
import io.opentelemetry.api.trace.SpanContext;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.pipelineframework.branching.BranchVariantIdentity;
import org.pipelineframework.config.PipelineStepConfig;
import org.pipelineframework.telemetry.derivation.RunTelemetryDerivation;
import org.pipelineframework.telemetry.derivation.StepTelemetryDerivation;
import org.pipelineframework.telemetry.derivation.RetryTelemetryDerivation;
import org.pipelineframework.telemetry.observation.RejectObservation;

/** Imperative replay adapter: topology, scope and exporter coordination only. */
final class PipelineReplaySupport {
    private final Optional<ExecutionReplayTracker> tracker;
    private final Optional<PipelineReplayTopology> topology;
    private final PipelineStepConfig stepConfig;

    PipelineReplaySupport(
        TelemetryPolicy policy,
        TelemetryRuntime runtime,
        PipelineReplayExporter exporter,
        Optional<PipelineReplayTopology> topology,
        PipelineMetricsRecorder metrics,
        PipelineStepConfig stepConfig
    ) {
        this.topology = topology;
        this.stepConfig = stepConfig;
        tracker = policy.replayEnabled()
            ? Optional.of(new ExecutionReplayTracker(
                runtime.tracer("org.pipelineframework"), exporter, topology.orElseThrow(),
                metrics.transitionCounter(), metrics.transitionLatency()))
            : Optional.empty();
    }

    boolean enabled() { return tracker.isPresent(); }

    Optional<PipelineReplayTopology> topology() { return topology; }

    PipelineReplayRunParameters runParameters() {
        return tracker.isPresent() ? PipelineReplayRunParametersCapture.capture(stepConfig) : null;
    }

    ExecutionReplayTracker.RunReplayState runState() {
        return tracker.isPresent() ? new ExecutionReplayTracker.RunReplayState() : null;
    }

    void runStarted(PipelineRunContext context) { tracker.ifPresent(current -> current.runStarted(context)); }

    void runFinished(PipelineRunContext context, RunTelemetryDerivation.ReplayFinished signal) {
        tracker.ifPresent(current -> {
            if (signal.outcome() == RunTelemetryDerivation.ReplayOutcome.SUCCESS) {
                current.runCompleted(context, signal.durationMillis());
            } else {
                current.runFailed(context, signal.durationMillis(), signal.failure().orElseThrow());
            }
        });
    }

    ExecutionReplayTracker.StepExecutionScope beginStep(
        Class<?> stepClass, PipelineRunContext context, boolean perItemOperation, Object input
    ) {
        return tracker.filter(ignored -> valid(stepClass, context)).map(current ->
            current.beginStep(PipelineMetricAttributes.resolveStepClassName(stepClass), context, perItemOperation, input))
            .orElse(null);
    }

    ExecutionReplayTracker.StepExecutionScope beginPendingStep(
        Class<?> stepClass, PipelineRunContext context, boolean perItemOperation
    ) {
        return tracker.filter(ignored -> valid(stepClass, context)).map(current ->
            current.beginPendingStep(PipelineMetricAttributes.resolveStepClassName(stepClass), context, perItemOperation))
            .orElse(null);
    }

    void recordInput(ExecutionReplayTracker.StepExecutionScope scope, Object input) {
        tracker.filter(ignored -> scope != null).ifPresent(current -> current.recordInput(scope, input));
    }

    void recordOutput(ExecutionReplayTracker.StepExecutionScope scope, Object output) {
        tracker.filter(ignored -> scope != null).ifPresent(current -> current.recordOutput(scope, output));
    }

    void recordSkip(
        Class<?> stepClass, PipelineRunContext context, Object input, List<String> acceptedTypes,
        Optional<BranchVariantIdentity> variantIdentity
    ) {
        tracker.filter(ignored -> valid(stepClass, context)).ifPresent(current -> current.recordSkip(
            PipelineMetricAttributes.resolveStepClassName(stepClass), context, input,
            input == null ? "null" : input.getClass().getName(), acceptedTypes,
            variantIdentity == null ? Optional.empty() : variantIdentity));
    }

    void recordCacheHit(Object scope) {
        tracker.filter(ignored -> scope instanceof ExecutionReplayTracker.StepExecutionScope)
            .ifPresent(current -> current.recordCacheHit((ExecutionReplayTracker.StepExecutionScope) scope));
    }

    void complete(ExecutionReplayTracker.StepExecutionScope scope, StepTelemetryDerivation.ReplayFinished signal) {
        tracker.filter(ignored -> scope != null).ifPresent(current -> {
            if (signal.outcome() == StepTelemetryDerivation.ReplayOutcome.CANCELLED) {
                current.completeCancelled(scope);
            } else if (signal.outcome() == StepTelemetryDerivation.ReplayOutcome.SUCCESS) {
                current.completeSuccess(scope);
            } else {
                current.completeFailure(scope, signal.failure().orElseThrow());
            }
        });
    }

    void recordAwaitLifecycle(AwaitReplayLifecycleEvent event) {
        tracker.filter(ignored -> event != null).ifPresent(current -> current.recordAwaitLifecycle(event, Instant.now()));
    }

    void recordConnectorEvent(String connectorStep, String service, String eventName, String from, String to,
                              Map<String, String> attributes) {
        tracker.ifPresent(current -> current.recordConnectorEvent(
            connectorStep, service, eventName, from, to, attributes, Instant.now()));
    }

    void recordRetry(RetryTelemetryDerivation.ReplaySignal signal) {
        tracker.ifPresent(current -> current.recordRetry(
            signal.stepClass(), currentSpanId(), signal.failure().orElse(null)));
    }

    void recordReject(RejectObservation observation) {
        tracker.ifPresent(current -> current.recordReject(observation.stepClass(), currentSpanId(), observation.scope(),
            observation.errorType(), observation.errorMessage()));
    }

    private boolean valid(Class<?> stepClass, PipelineRunContext context) {
        return stepClass != null && context != null && context.enabled();
    }

    private static String currentSpanId() {
        SpanContext current = Span.current().getSpanContext();
        return current != null && current.isValid() ? current.getSpanId() : null;
    }
}
