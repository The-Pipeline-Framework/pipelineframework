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

import java.util.List;
import java.util.Optional;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * Internal execution-facing telemetry decorator.
 *
 * <p>It centralises the optional telemetry and replay wiring required by the
 * runtime executor while keeping replay scopes type-safe inside the telemetry package.</p>
 */
public final class PipelineStepTelemetry {

    private static final PipelineStepTelemetry DISABLED = new PipelineStepTelemetry(Optional.empty(), Optional.empty());

    private final Optional<PipelineTelemetry> telemetry;
    private final Optional<PipelineTelemetry.RunContext> runContext;

    private PipelineStepTelemetry(
        Optional<PipelineTelemetry> telemetry,
        Optional<PipelineTelemetry.RunContext> runContext
    ) {
        this.telemetry = telemetry;
        this.runContext = runContext;
    }

    public static PipelineStepTelemetry of(
        PipelineTelemetry telemetry,
        PipelineTelemetry.RunContext runContext
    ) {
        if (telemetry == null) {
            return DISABLED;
        }
        return new PipelineStepTelemetry(Optional.of(telemetry), Optional.ofNullable(runContext));
    }

    public <T> Uni<T> consume(Class<?> stepClass, Uni<T> input) {
        return telemetry.map(current -> runContext
            .map(context -> current.instrumentItemConsumed(stepClass, context, input))
            .orElseGet(() -> current.instrumentItemConsumed(stepClass, input)))
            .orElse(input);
    }

    public <T> Multi<T> consume(Class<?> stepClass, Multi<T> input) {
        return telemetry.map(current -> runContext
            .map(context -> current.instrumentItemConsumed(stepClass, context, input))
            .orElseGet(() -> current.instrumentItemConsumed(stepClass, input)))
            .orElse(input);
    }

    public <T> Uni<T> produce(Class<?> stepClass, Uni<T> output) {
        return telemetry.map(current -> runContext
            .map(context -> current.instrumentItemProduced(stepClass, context, output))
            .orElseGet(() -> current.instrumentItemProduced(stepClass, output)))
            .orElse(output);
    }

    public <T> Multi<T> produce(Class<?> stepClass, Multi<T> output) {
        return telemetry.map(current -> runContext
            .map(context -> current.instrumentItemProduced(stepClass, context, output))
            .orElseGet(() -> current.instrumentItemProduced(stepClass, output)))
            .orElse(output);
    }

    public ReplayScope beginReplayStep(Class<?> stepClass, boolean perItemOperation, Object inputItem) {
        return new ReplayScope(telemetry.flatMap(current -> runContext.flatMap(context ->
            Optional.ofNullable(current.beginReplayStep(stepClass, context, perItemOperation, inputItem)))));
    }

    public ReplayScope beginPendingReplayStep(Class<?> stepClass, boolean perItemOperation) {
        return new ReplayScope(telemetry.flatMap(current -> runContext.flatMap(context ->
            Optional.ofNullable(current.beginPendingReplayStep(stepClass, context, perItemOperation)))));
    }

    public <T> Uni<T> recordInput(ReplayScope scope, Uni<T> input) {
        return scope.value.isPresent() ? input.onItem().invoke(item -> recordInput(scope, item)) : input;
    }

    public <T> Multi<T> recordInput(ReplayScope scope, Multi<T> input) {
        return scope.value.isPresent() ? input.onItem().invoke(item -> recordInput(scope, item)) : input;
    }

    public void recordInput(ReplayScope scope, Object inputItem) {
        telemetry.ifPresent(current -> scope.value.ifPresent(replayScope ->
            current.recordReplayInput(replayScope, inputItem)));
    }

    public void recordOutput(ReplayScope scope, Object outputItem) {
        telemetry.ifPresent(current -> scope.value.ifPresent(replayScope ->
            current.recordReplayOutput(replayScope, outputItem)));
    }

    public void recordCacheHit(ReplayScope scope) {
        telemetry.ifPresent(current -> scope.value.ifPresent(current::recordReplayCacheHit));
    }

    public void recordSkip(Class<?> stepClass, Object inputItem, List<String> acceptedTypes) {
        telemetry.ifPresent(current -> runContext.ifPresent(context ->
            current.recordReplaySkip(stepClass, context, inputItem, acceptedTypes)));
    }

    public <T> Uni<T> instrument(Class<?> stepClass, Uni<T> result, boolean perItemOperation, ReplayScope scope) {
        if (telemetry.isEmpty() || runContext.isEmpty()) {
            return result;
        }
        PipelineTelemetry current = telemetry.orElseThrow();
        PipelineTelemetry.RunContext context = runContext.orElseThrow();
        return scope.value
            .map(replayScope -> current.instrumentStepUni(stepClass, result, context, perItemOperation, replayScope))
            .orElseGet(() -> current.instrumentStepUni(stepClass, result, context, perItemOperation));
    }

    public <T> Multi<T> instrument(Class<?> stepClass, Multi<T> result, boolean perItemOperation, ReplayScope scope) {
        if (telemetry.isEmpty() || runContext.isEmpty()) {
            return result;
        }
        PipelineTelemetry current = telemetry.orElseThrow();
        PipelineTelemetry.RunContext context = runContext.orElseThrow();
        return scope.value
            .map(replayScope -> current.instrumentStepMulti(stepClass, result, context, perItemOperation, replayScope))
            .orElseGet(() -> current.instrumentStepMulti(stepClass, result, context, perItemOperation));
    }

    public static final class ReplayScope {
        private final Optional<ExecutionReplayTracker.StepExecutionScope> value;

        private ReplayScope(Optional<ExecutionReplayTracker.StepExecutionScope> value) {
            this.value = value;
        }
    }
}
