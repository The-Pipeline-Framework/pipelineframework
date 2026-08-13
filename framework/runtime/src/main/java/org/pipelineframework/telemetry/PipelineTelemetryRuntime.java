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

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.pipelineframework.branching.BranchVariantIdentity;
import java.util.Map;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.enterprise.inject.Instance;

import org.jboss.logging.Logger;
import io.quarkus.arc.Unremovable;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.config.ParallelismPolicy;
import org.pipelineframework.config.PipelineStepConfig;
import org.pipelineframework.telemetry.PipelineRunContext;

/**
 * Records pipeline-level spans and metrics for step execution.
 */
@ApplicationScoped
@Unremovable
public class PipelineTelemetryRuntime implements PipelineRunTelemetry, PipelineStepTelemetry.Seam,
    RetryAmplificationTelemetry, PipelineReplayTelemetry, TelemetryPolicySource {

    private static final Logger LOG = Logger.getLogger(PipelineTelemetryRuntime.class);
    private final PipelineTracingRecorder tracing;
    private final PipelineMetricsRecorder metrics;
    private final PipelineStepInstrumentation steps;
    private final PipelineReplaySupport replay;
    private final RetryAmplificationGuardRuntime retryAmplificationGuard;
    private final TelemetryPolicy telemetryPolicy;
    private final PipelineRetryTelemetry retryTelemetry;
    private final PipelineRunLifecycle lifecycle;

    /**
     * Create a telemetry helper from the configured pipeline settings.
     *
     * @param stepConfig pipeline configuration mapping
     */
    @Inject
    public PipelineTelemetryRuntime(PipelineStepConfig stepConfig, Instance<PipelineReplayExporter> replayExporters,
                             TelemetryRuntime telemetryRuntime) {
        this(stepConfig, resolveReplayExporter(replayExporters), PipelineReplayTopologyLoader.load(), telemetryRuntime);
    }

    PipelineTelemetryRuntime(PipelineStepConfig stepConfig) {
        this(stepConfig, new NoopPipelineReplayExporter(), PipelineReplayTopologyLoader.load(),
            TelemetryCompatibilityAccess.runtime());
    }

    private static PipelineReplayExporter resolveReplayExporter(Instance<PipelineReplayExporter> replayExporters) {
        if (replayExporters != null && replayExporters.isResolvable()) {
            return replayExporters.get();
        }
        return new NoopPipelineReplayExporter();
    }

    PipelineTelemetryRuntime(
        PipelineStepConfig stepConfig,
        PipelineReplayExporter replayExporter,
        PipelineReplayTopology replayTopology) {
        this(stepConfig, replayExporter, Optional.ofNullable(replayTopology), TelemetryCompatibilityAccess.runtime());
    }

    /**
     * Compatibility façade constructor with an injectable SDK runtime for deterministic adapter tests.
     */
    PipelineTelemetryRuntime(
        PipelineStepConfig stepConfig,
        PipelineReplayExporter replayExporter,
        PipelineReplayTopology replayTopology,
        TelemetryRuntime telemetryRuntime) {
        this(stepConfig, replayExporter, Optional.ofNullable(replayTopology), telemetryRuntime);
    }

    PipelineTelemetryRuntime(
        PipelineStepConfig stepConfig,
        PipelineReplayExporter replayExporter,
        Optional<PipelineReplayTopology> replayTopology,
        TelemetryRuntime telemetryRuntime) {
        PipelineStepConfig.TelemetryConfig telemetry = stepConfig.telemetry();
        this.telemetryPolicy = TelemetryPolicy.from(stepConfig, replayTopology.isPresent());
        PipelineStepConfig.ReplayConfig replayConfig = telemetry == null ? null : telemetry.replay();
        boolean replayRequested = replayConfig != null && Boolean.TRUE.equals(replayConfig.enabled());
        if (replayRequested && !telemetryPolicy.replayEnabled()) {
            LOG.warn(
                "pipeline.telemetry.replay.enabled=true requires pipeline.telemetry.replay.exporter=file, "
                    + "pipeline.telemetry.replay.file.path, tracing, per-item spans, and replay topology metadata.");
        }
        this.tracing = new PipelineTracingRecorder(telemetryPolicy, telemetryRuntime, replayTopology);
        this.metrics = new PipelineMetricsRecorder(telemetryPolicy, telemetryRuntime, replayTopology);
        this.retryAmplificationGuard = new RetryAmplificationGuardRuntime(telemetryPolicy);
        this.replay = new PipelineReplaySupport(
            telemetryPolicy, telemetryRuntime, replayExporter, replayTopology, metrics, stepConfig);
        this.steps = new PipelineStepInstrumentation(metrics, tracing, replay, retryAmplificationGuard);
        this.retryTelemetry = new PipelineRetryTelemetry(telemetryPolicy, metrics, tracing, replay, retryAmplificationGuard);
        this.lifecycle = new PipelineRunLifecycle(
            telemetryPolicy, metrics, tracing, replay, retryAmplificationGuard, retryTelemetry);
    }

    /**
     * Start a pipeline run and return the telemetry context.
     *
     * @param input pipeline input
     * @param stepCount number of steps
     * @param policy parallelism policy
     * @param maxConcurrency max concurrency
     * @return telemetry run context
     */
    public PipelineRunContext startRun(Object input, int stepCount, ParallelismPolicy policy, int maxConcurrency) {
        return lifecycle.start(input, stepCount, policy, maxConcurrency);
    }

    /**
     * Instrument pipeline input.
     *
     * @param input input Uni or Multi
     * @param runContext telemetry context
     * @return instrumented input
     */
    public Object instrumentInput(Object input, PipelineRunContext runContext) {
        return input;
    }

    /**
     * Instrument a consumer step to count items entering the configured item boundary.
     *
     * @param stepClass step class
     * @param input step input
     * @param <T> item type
     * @return instrumented input
     */
    public <T> Multi<T> instrumentItemConsumed(Class<?> stepClass, Multi<T> input) {
        return instrumentItemConsumed(stepClass, null, input);
    }

    /**
     * Instrument a consumer step to count items consumed at the configured item boundary.
     *
     * @param stepClass step class
     * @param runContext run context
     * @param input step input
     * @param <T> item type
     * @return instrumented input
     */
    public <T> Multi<T> instrumentItemConsumed(
        Class<?> stepClass,
        PipelineRunContext runContext,
        Multi<T> input) {
        return metrics.instrumentConsumed(stepClass, runContext, input);
    }

    @Override
    public <T> Multi<T> instrumentItemConsumed(Class<?> stepClass, Object context, Multi<T> input) {
        return instrumentItemConsumed(stepClass, runContext(context), input);
    }

    @Override
    public <T> Uni<T> instrumentItemConsumed(Class<?> stepClass, Object context, Uni<T> input) {
        return instrumentItemConsumed(stepClass, runContext(context), input);
    }

    @Override
    public <T> Multi<T> instrumentItemProduced(Class<?> stepClass, Object context, Multi<T> output) {
        return instrumentItemProduced(stepClass, runContext(context), output);
    }

    @Override
    public <T> Uni<T> instrumentItemProduced(Class<?> stepClass, Object context, Uni<T> output) {
        return instrumentItemProduced(stepClass, runContext(context), output);
    }

    /**
     * Instrument a consumer step to count items entering the configured item boundary.
     *
     * @param stepClass step class
     * @param input step input
     * @param <T> item type
     * @return instrumented input
     */
    public <T> Uni<T> instrumentItemConsumed(Class<?> stepClass, Uni<T> input) {
        return instrumentItemConsumed(stepClass, null, input);
    }

    /**
     * Instrument a consumer step to count items consumed at the configured item boundary.
     *
     * @param stepClass step class
     * @param runContext run context
     * @param input step input
     * @param <T> item type
     * @return instrumented input
     */
    public <T> Uni<T> instrumentItemConsumed(
        Class<?> stepClass,
        PipelineRunContext runContext,
        Uni<T> input) {
        return metrics.instrumentConsumed(stepClass, runContext, input);
    }

    /**
     * Instrument a producer step to count items emitted at the configured item boundary.
     *
     * @param stepClass step class
     * @param output step output
     * @param <T> item type
     * @return instrumented output
     */
    public <T> Multi<T> instrumentItemProduced(Class<?> stepClass, Multi<T> output) {
        return instrumentItemProduced(stepClass, null, output);
    }

    /**
     * Instrument a producer step to count items emitted at the configured item boundary.
     *
     * @param stepClass step class
     * @param runContext run context
     * @param output step output
     * @param <T> item type
     * @return instrumented output
     */
    public <T> Multi<T> instrumentItemProduced(
        Class<?> stepClass,
        PipelineRunContext runContext,
        Multi<T> output) {
        return metrics.instrumentProduced(stepClass, runContext, output);
    }

    /**
     * Instrument a producer step to count items emitted at the configured item boundary.
     *
     * @param stepClass step class
     * @param output step output
     * @param <T> item type
     * @return instrumented output
     */
    public <T> Uni<T> instrumentItemProduced(Class<?> stepClass, Uni<T> output) {
        return instrumentItemProduced(stepClass, null, output);
    }

    /**
     * Instrument a producer step to count items emitted at the configured item boundary.
     *
     * @param stepClass step class
     * @param runContext run context
     * @param output step output
     * @param <T> item type
     * @return instrumented output
     */
    public <T> Uni<T> instrumentItemProduced(
        Class<?> stepClass,
        PipelineRunContext runContext,
        Uni<T> output) {
        return metrics.instrumentProduced(stepClass, runContext, output);
    }

    /**
     * Attach completion hooks to a Uni or Multi to close the run.
     *
     * @param current Uni or Multi
     * @param runContext telemetry context
     * @return instrumented publisher
     */
    public Object instrumentRunCompletion(Object current, PipelineRunContext runContext) {
        return lifecycle.instrumentCompletion(current, runContext);
    }

    public void abortRun(PipelineRunContext runContext, Throwable failure) {
        lifecycle.abort(runContext, failure);
    }

    public void abortActiveRun(Throwable failure) {
        lifecycle.abortActive(failure);
    }

    /**
     * Instrument a step execution that returns a Uni.
     *
     * @param stepClass step class
     * @param uni step result
     * @param runContext telemetry context
     * @param perItemOperation true when called per item
     * @param <T> output type
     * @return instrumented Uni
     */
    public <T> Uni<T> instrumentStepUni(
        Class<?> stepClass,
        Uni<T> uni,
        PipelineRunContext runContext,
        boolean perItemOperation) {
        return instrumentStepUni(stepClass, uni, runContext, perItemOperation, null);
    }

    public <T> Uni<T> instrumentStepUni(
        Class<?> stepClass,
        Uni<T> uni,
        PipelineRunContext runContext,
        boolean perItemOperation,
        ExecutionReplayTracker.StepExecutionScope replayScope) {
        return steps.instrument(stepClass, uni, runContext, perItemOperation, replayScope);
    }

    /**
     * Instrument a step execution that returns a Multi.
     *
     * @param stepClass step class
     * @param multi step result
     * @param runContext telemetry context
     * @param perItemOperation true when called per item
     * @param <T> output type
     * @return instrumented Multi
     */
    public <T> Multi<T> instrumentStepMulti(
        Class<?> stepClass,
        Multi<T> multi,
        PipelineRunContext runContext,
        boolean perItemOperation) {
        return instrumentStepMulti(stepClass, multi, runContext, perItemOperation, null);
    }

    public <T> Multi<T> instrumentStepMulti(
        Class<?> stepClass,
        Multi<T> multi,
        PipelineRunContext runContext,
        boolean perItemOperation,
        ExecutionReplayTracker.StepExecutionScope replayScope) {
        return steps.instrument(stepClass, multi, runContext, perItemOperation, replayScope);
    }

    public ExecutionReplayTracker.StepExecutionScope beginReplayStep(
        Class<?> stepClass,
        PipelineRunContext runContext,
        boolean perItemOperation,
        Object inputItem) {
        return replay.beginStep(stepClass, runContext, perItemOperation, inputItem);
    }

    @Override
    public ExecutionReplayTracker.StepExecutionScope beginReplayStep(
        Class<?> stepClass, Object context, boolean perItemOperation, Object inputItem) {
        return beginReplayStep(stepClass, runContext(context), perItemOperation, inputItem);
    }

    public ExecutionReplayTracker.StepExecutionScope beginPendingReplayStep(
        Class<?> stepClass,
        PipelineRunContext runContext,
        boolean perItemOperation) {
        return replay.beginPendingStep(stepClass, runContext, perItemOperation);
    }

    @Override
    public ExecutionReplayTracker.StepExecutionScope beginPendingReplayStep(
        Class<?> stepClass, Object context, boolean perItemOperation) {
        return beginPendingReplayStep(stepClass, runContext(context), perItemOperation);
    }

    public void recordReplayInput(ExecutionReplayTracker.StepExecutionScope scope, Object inputItem) {
        replay.recordInput(scope, inputItem);
    }

    public void recordReplayOutput(ExecutionReplayTracker.StepExecutionScope scope, Object outputItem) {
        replay.recordOutput(scope, outputItem);
    }

    public void recordReplaySkip(
        Class<?> stepClass,
        PipelineRunContext runContext,
        Object inputItem,
        List<String> acceptedTypes
    ) {
        recordReplaySkip(stepClass, runContext, inputItem, acceptedTypes, Optional.empty());
    }

    @Override
    public void recordReplaySkip(
        Class<?> stepClass, Object context, Object inputItem, List<String> acceptedTypes,
        Optional<BranchVariantIdentity> variantIdentity) {
        recordReplaySkip(stepClass, runContext(context), inputItem, acceptedTypes, variantIdentity);
    }

    @Override
    public <T> Uni<T> instrumentStepUni(
        Class<?> stepClass, Uni<T> result, Object context, boolean perItemOperation,
        ExecutionReplayTracker.StepExecutionScope scope) {
        return instrumentStepUni(stepClass, result, runContext(context), perItemOperation, scope);
    }

    @Override
    public <T> Uni<T> instrumentStepUni(
        Class<?> stepClass, Uni<T> result, Object context, boolean perItemOperation) {
        return instrumentStepUni(stepClass, result, runContext(context), perItemOperation);
    }

    @Override
    public <T> Multi<T> instrumentStepMulti(
        Class<?> stepClass, Multi<T> result, Object context, boolean perItemOperation,
        ExecutionReplayTracker.StepExecutionScope scope) {
        return instrumentStepMulti(stepClass, result, runContext(context), perItemOperation, scope);
    }

    @Override
    public <T> Multi<T> instrumentStepMulti(
        Class<?> stepClass, Multi<T> result, Object context, boolean perItemOperation) {
        return instrumentStepMulti(stepClass, result, runContext(context), perItemOperation);
    }

    private static PipelineRunContext runContext(Object context) {
        if (context instanceof PipelineRunContext runContext) {
            return runContext;
        }
        throw new IllegalArgumentException("Pipeline step telemetry requires a PipelineRunContext.");
    }

    public void recordReplaySkip(
        Class<?> stepClass,
        PipelineRunContext runContext,
        Object inputItem,
        List<String> acceptedTypes,
        Optional<BranchVariantIdentity> variantIdentity
    ) {
        replay.recordSkip(stepClass, runContext, inputItem, acceptedTypes, variantIdentity);
    }

    public void recordReplayCacheHit(Object scope) {
        replay.recordCacheHit(scope);
    }

    public void recordAwaitLifecycle(AwaitReplayLifecycleEvent lifecycleEvent) {
        replay.recordAwaitLifecycle(lifecycleEvent);
    }

    public void recordConnectorReplayEvent(
        String connectorStep,
        String service,
        String eventName,
        String from,
        String to,
        Map<String, String> attributes
    ) {
        replay.recordConnectorEvent(connectorStep, service, eventName, from, to, attributes);
    }

    public boolean retryAmplificationGuardEnabled() {
        return retryTelemetry.guardEnabled();
    }

    public RetryAmplificationGuardMode retryAmplificationMode() {
        return retryTelemetry.guardMode();
    }

    public Duration retryAmplificationCheckInterval() {
        return retryTelemetry.checkInterval();
    }

    public Optional<RetryAmplificationGuard.Trigger> retryAmplificationTrigger() {
        return retryTelemetry.trigger();
    }

    public Optional<RetryAmplificationGuard.Trigger> retryAmplificationTrigger(PipelineRunContext runContext) {
        return retryTelemetry.trigger(runContext);
    }

    @Override
    public TelemetryPolicy telemetryPolicy() {
        return telemetryPolicy;
    }


    @PreDestroy
    void shutdownRetryAmplificationScheduler() {
        lifecycle.shutdown(retryTelemetry);
        retryTelemetry.shutdown();
    }

}
