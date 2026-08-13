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

import java.time.Duration;
import java.util.Optional;

/**
 * Compatibility bridge for retry and reject facts.
 *
 * <p>It owns neither safety policy nor SDK instruments: facts are dispatched to
 * the focused metrics, tracing, replay and safety collaborators of the owning run.</p>
 */
public final class PipelineRetryTelemetry {
    private final TelemetryPolicy policy;
    private final PipelineMetricsRecorder metrics;
    private final PipelineTracingRecorder tracing;
    private final PipelineReplaySupport replay;
    private final RetryAmplificationGuardRuntime guard;

    PipelineRetryTelemetry(
        TelemetryPolicy policy,
        PipelineMetricsRecorder metrics,
        PipelineTracingRecorder tracing,
        PipelineReplaySupport replay,
        RetryAmplificationGuardRuntime guard
    ) {
        this.policy = policy;
        this.metrics = metrics;
        this.tracing = tracing;
        this.replay = replay;
        this.guard = guard;
        RetryObservationCompatibility.add(this);
    }

    void register(PipelineRunContext context) {
        RetryObservationCompatibility.register(context, this);
    }

    void unregister(PipelineRunContext context) {
        RetryObservationCompatibility.unregister(context, this);
    }

    void retry(Class<?> stepClass, Throwable failure) {
        if (stepClass == null || !(policy.metricsEnabled() || policy.retryAmplificationEnabled() || replay.enabled())) {
            return;
        }
        String step = PipelineMetricAttributes.resolveStepClassName(stepClass);
        guard.retryRecorded(step);
        metrics.retryRecorded(stepClass);
        replay.recordRetry(stepClass, failure);
    }

    void reject(Class<?> stepClass, String rejectScope, String errorType, String errorMessage) {
        if (stepClass != null) {
            replay.recordReject(stepClass, rejectScope, errorType, errorMessage);
        }
    }

    void killSwitchTriggered(PipelineRunContext context, RetryAmplificationGuard.Trigger trigger) {
        metrics.killSwitchTriggered(trigger);
        tracing.recordKillSwitch(context, trigger);
    }

    boolean guardEnabled() { return policy.retryAmplificationEnabled(); }

    RetryAmplificationGuardMode guardMode() { return policy.retryAmplificationMode(); }

    Duration checkInterval() { return guard.sampleInterval(); }

    Optional<RetryAmplificationGuard.Trigger> trigger() { return guard.trigger(); }

    Optional<RetryAmplificationGuard.Trigger> trigger(PipelineRunContext context) {
        return context == null || !context.enabled() ? guard.trigger() : guard.trigger(context.runId());
    }

    void shutdown() {
        RetryObservationCompatibility.remove(this);
    }

    public static void recordRetry(Class<?> stepClass, Throwable failure) {
        RetryObservationCompatibility.retry(stepClass, failure);
    }


    public static void recordReject(Class<?> stepClass, String scope, String errorType, String errorMessage) {
        RetryObservationCompatibility.reject(stepClass, scope, errorType, errorMessage);
    }
}
