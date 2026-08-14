/*
 * Copyright (c) 2023-2025 Mariano Barcia
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
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.enterprise.inject.Typed;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.pipelineframework.branching.BranchVariantIdentity;
import org.pipelineframework.config.ParallelismPolicy;
import org.pipelineframework.config.PipelineStepConfig;

/**
 * Compatibility façade and composition root for pipeline telemetry.
 * New runtime code should depend on focused telemetry seams rather than this type.
 */
@ApplicationScoped
@Typed(PipelineTelemetry.class)
public class PipelineTelemetry implements PipelineRunTelemetry, PipelineStepTelemetry.Seam,
    RetryAmplificationTelemetry, PipelineReplayTelemetry, TelemetryPolicySource {
    private final PipelineTelemetryRuntime runtime;

    @Inject
    PipelineTelemetry(PipelineTelemetryRuntime runtime) {
        this.runtime = runtime;
    }

    public PipelineTelemetry(PipelineStepConfig stepConfig) {
        runtime = new PipelineTelemetryRuntime(stepConfig);
    }

    public PipelineTelemetry(PipelineStepConfig stepConfig, PipelineReplayExporter exporter,
                             PipelineReplayTopology topology) {
        runtime = new PipelineTelemetryRuntime(stepConfig, exporter, topology);
    }

    public PipelineTelemetry(PipelineStepConfig stepConfig, PipelineReplayExporter exporter,
                             PipelineReplayTopology topology, TelemetryRuntime telemetryRuntime) {
        runtime = new PipelineTelemetryRuntime(stepConfig, exporter, topology, telemetryRuntime);
    }

    PipelineTelemetry(PipelineStepConfig stepConfig, PipelineReplayExporter exporter,
                      Optional<PipelineReplayTopology> topology, TelemetryRuntime telemetryRuntime) {
        runtime = new PipelineTelemetryRuntime(stepConfig, exporter, topology, telemetryRuntime);
    }

    public PipelineRunContext startRun(Object input, int stepCount, ParallelismPolicy policy, int maxConcurrency) { return runtime.startRun(input, stepCount, policy, maxConcurrency); }
    public Object instrumentInput(Object input, PipelineRunContext context) { return runtime.instrumentInput(input, context); }
    public <T> Multi<T> instrumentItemConsumed(Class<?> step, Multi<T> input) { return runtime.instrumentItemConsumed(step, input); }
    public <T> Multi<T> instrumentItemConsumed(Class<?> step, PipelineRunContext context, Multi<T> input) { return runtime.instrumentItemConsumed(step, context, input); }
    public <T> Multi<T> instrumentItemConsumed(Class<?> step, Object context, Multi<T> input) { return instrumentItemConsumed(step, runContext(context), input); }
    public <T> Uni<T> instrumentItemConsumed(Class<?> step, Uni<T> input) { return runtime.instrumentItemConsumed(step, input); }
    public <T> Uni<T> instrumentItemConsumed(Class<?> step, PipelineRunContext context, Uni<T> input) { return runtime.instrumentItemConsumed(step, context, input); }
    public <T> Uni<T> instrumentItemConsumed(Class<?> step, Object context, Uni<T> input) { return instrumentItemConsumed(step, runContext(context), input); }
    public <T> Multi<T> instrumentItemProduced(Class<?> step, Multi<T> output) { return runtime.instrumentItemProduced(step, output); }
    public <T> Multi<T> instrumentItemProduced(Class<?> step, PipelineRunContext context, Multi<T> output) { return runtime.instrumentItemProduced(step, context, output); }
    public <T> Multi<T> instrumentItemProduced(Class<?> step, Object context, Multi<T> output) { return instrumentItemProduced(step, runContext(context), output); }
    public <T> Uni<T> instrumentItemProduced(Class<?> step, Uni<T> output) { return runtime.instrumentItemProduced(step, output); }
    public <T> Uni<T> instrumentItemProduced(Class<?> step, PipelineRunContext context, Uni<T> output) { return runtime.instrumentItemProduced(step, context, output); }
    public <T> Uni<T> instrumentItemProduced(Class<?> step, Object context, Uni<T> output) { return instrumentItemProduced(step, runContext(context), output); }
    public Object instrumentRunCompletion(Object current, PipelineRunContext context) { return runtime.instrumentRunCompletion(current, context); }
    public void abortRun(PipelineRunContext context, Throwable failure) { runtime.abortRun(context, failure); }
    public void abortActiveRun(Throwable failure) { runtime.abortActiveRun(failure); }
    public <T> Uni<T> instrumentStepUni(Class<?> step, Uni<T> uni, PipelineRunContext context, boolean perItem) { return runtime.instrumentStepUni(step, uni, context, perItem); }
    public <T> Uni<T> instrumentStepUni(Class<?> step, Uni<T> uni, Object context, boolean perItem) { return instrumentStepUni(step, uni, runContext(context), perItem); }
    public <T> Uni<T> instrumentStepUni(Class<?> step, Uni<T> uni, PipelineRunContext context, boolean perItem, ExecutionReplayTracker.StepExecutionScope scope) { return runtime.instrumentStepUni(step, uni, context, perItem, scope); }
    public <T> Uni<T> instrumentStepUni(Class<?> step, Uni<T> uni, Object context, boolean perItem, ExecutionReplayTracker.StepExecutionScope scope) { return instrumentStepUni(step, uni, runContext(context), perItem, scope); }
    public <T> Multi<T> instrumentStepMulti(Class<?> step, Multi<T> multi, PipelineRunContext context, boolean perItem) { return runtime.instrumentStepMulti(step, multi, context, perItem); }
    public <T> Multi<T> instrumentStepMulti(Class<?> step, Multi<T> multi, Object context, boolean perItem) { return instrumentStepMulti(step, multi, runContext(context), perItem); }
    public <T> Multi<T> instrumentStepMulti(Class<?> step, Multi<T> multi, PipelineRunContext context, boolean perItem, ExecutionReplayTracker.StepExecutionScope scope) { return runtime.instrumentStepMulti(step, multi, context, perItem, scope); }
    public <T> Multi<T> instrumentStepMulti(Class<?> step, Multi<T> multi, Object context, boolean perItem, ExecutionReplayTracker.StepExecutionScope scope) { return instrumentStepMulti(step, multi, runContext(context), perItem, scope); }
    public ExecutionReplayTracker.StepExecutionScope beginReplayStep(Class<?> step, PipelineRunContext context, boolean perItem, Object item) { return runtime.beginReplayStep(step, context, perItem, item); }
    public ExecutionReplayTracker.StepExecutionScope beginReplayStep(Class<?> step, Object context, boolean perItem, Object item) { return beginReplayStep(step, runContext(context), perItem, item); }
    public ExecutionReplayTracker.StepExecutionScope beginPendingReplayStep(Class<?> step, PipelineRunContext context, boolean perItem) { return runtime.beginPendingReplayStep(step, context, perItem); }
    public ExecutionReplayTracker.StepExecutionScope beginPendingReplayStep(Class<?> step, Object context, boolean perItem) { return beginPendingReplayStep(step, runContext(context), perItem); }
    public void recordReplayInput(ExecutionReplayTracker.StepExecutionScope scope, Object item) { runtime.recordReplayInput(scope, item); }
    public void recordReplayOutput(ExecutionReplayTracker.StepExecutionScope scope, Object item) { runtime.recordReplayOutput(scope, item); }
    public void recordReplaySkip(Class<?> step, PipelineRunContext context, Object item, List<String> accepted) { runtime.recordReplaySkip(step, context, item, accepted); }
    public void recordReplaySkip(Class<?> step, PipelineRunContext context, Object item, List<String> accepted, Optional<BranchVariantIdentity> variant) { runtime.recordReplaySkip(step, context, item, accepted, variant); }
    public void recordReplaySkip(Class<?> step, Object context, Object item, List<String> accepted, Optional<BranchVariantIdentity> variant) { recordReplaySkip(step, runContext(context), item, accepted, variant); }
    public void recordReplayCacheHit(Object scope) { runtime.recordReplayCacheHit(scope); }
    public void recordAwaitLifecycle(AwaitReplayLifecycleEvent event) { runtime.recordAwaitLifecycle(event); }
    public void recordConnectorReplayEvent(String step, String service, String event, String from, String to, Map<String, String> attributes) { runtime.recordConnectorReplayEvent(step, service, event, from, to, attributes); }
    public boolean retryAmplificationGuardEnabled() { return runtime.retryAmplificationGuardEnabled(); }
    public RetryAmplificationGuardMode retryAmplificationMode() { return runtime.retryAmplificationMode(); }
    public Duration retryAmplificationCheckInterval() { return runtime.retryAmplificationCheckInterval(); }
    public Optional<RetryAmplificationGuard.Trigger> retryAmplificationTrigger() { return runtime.retryAmplificationTrigger(); }
    public Optional<RetryAmplificationGuard.Trigger> retryAmplificationTrigger(PipelineRunContext context) { return runtime.retryAmplificationTrigger(context); }
    public TelemetryPolicy telemetryPolicy() { return runtime.telemetryPolicy(); }
    public static void recordRetry(Class<?> step) { PipelineRetryTelemetry.recordRetry(step, null); }
    public static void recordRetry(Class<?> step, Throwable failure) { PipelineRetryTelemetry.recordRetry(step, failure); }
    public static void recordReject(Class<?> step, String scope, Throwable failure) {
        PipelineRetryTelemetry.recordReject(step, scope,
            failure == null ? null : failure.getClass().getName(), failure == null ? null : failure.getMessage());
    }
    public static void recordReject(Class<?> step, String scope, String type, String message) {
        PipelineRetryTelemetry.recordReject(step, scope, type, message);
    }

    @PreDestroy
    void shutdown() { runtime.shutdownRetryAmplificationScheduler(); }

    private static PipelineRunContext runContext(Object context) {
        if (context instanceof PipelineRunContext runContext) {
            return runContext;
        }
        throw new IllegalArgumentException("Pipeline step telemetry requires a PipelineRunContext.");
    }
}
