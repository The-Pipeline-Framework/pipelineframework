/*
 * Copyright (c) 2023-2026 Mariano Barcia
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

package org.pipelineframework.runtime.core;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Framework-neutral sequencing core for ordered pipeline step execution.
 *
 * <p>The core owns step iteration and range validation only. Host runtimes keep responsibility for
 * transport binding, telemetry, cache, lifecycle, and actual step invocation semantics.</p>
 */
public final class PipelineRunnerCore {

    /**
     * Run an ordered step range synchronously from the caller perspective.
     *
     * @param input input object for the first selected step
     * @param steps ordered steps
     * @param startStepIndex first step index to run
     * @param stopBeforeStepIndex exclusive stop index
     * @param invoker runtime-specific step invoker
     * @param <S> step type
     * @return output from the last invoked step, or the original input when no step is invoked
     */
    public <S> Object runSync(
            Object input,
            List<S> steps,
            int startStepIndex,
            int stopBeforeStepIndex,
            SyncStepInvoker<S> invoker) {
        return runSync(input, steps, startStepIndex, stopBeforeStepIndex, invoker, index -> {
        });
    }

    /**
     * Run an ordered step range synchronously with a null-step callback.
     *
     * @param input input object for the first selected step
     * @param steps ordered steps
     * @param startStepIndex first step index to run
     * @param stopBeforeStepIndex exclusive stop index
     * @param invoker runtime-specific step invoker
     * @param nullStepHandler callback for skipped null steps
     * @param <S> step type
     * @return output from the last invoked step, or the original input when no step is invoked
     */
    public <S> Object runSync(
            Object input,
            List<S> steps,
            int startStepIndex,
            int stopBeforeStepIndex,
            SyncStepInvoker<S> invoker,
            NullStepHandler nullStepHandler) {
        Objects.requireNonNull(steps, "steps must not be null");
        Objects.requireNonNull(invoker, "invoker must not be null");
        Objects.requireNonNull(nullStepHandler, "nullStepHandler must not be null");
        validateRange(steps, startStepIndex, stopBeforeStepIndex);

        Object current = input;
        for (int index = startStepIndex; index < stopBeforeStepIndex; index++) {
            S step = steps.get(index);
            if (step == null) {
                nullStepHandler.onNullStep(index);
                continue;
            }
            current = invoker.invoke(step, current, index);
        }
        return current;
    }

    /**
     * Run all ordered steps with asynchronous step boundaries.
     *
     * @param input input object for the first step
     * @param steps ordered steps
     * @param invoker runtime-specific asynchronous step invoker
     * @param <S> step type
     * @return stage that completes with the last step output
     */
    public <S> CompletionStage<Object> runAsync(Object input, List<S> steps, AsyncStepInvoker<S> invoker) {
        Objects.requireNonNull(steps, "steps must not be null");
        Objects.requireNonNull(invoker, "invoker must not be null");

        CompletionStage<Object> current = CompletableFuture.completedFuture(input);
        for (int index = 0; index < steps.size(); index++) {
            S step = steps.get(index);
            if (step == null) {
                continue;
            }
            int stepIndex = index;
            current = current.thenCompose(value -> invoker.invoke(step, value, stepIndex));
        }
        return current;
    }

    private void validateRange(List<?> steps, int startStepIndex, int stopBeforeStepIndex) {
        if (startStepIndex < 0 || startStepIndex > steps.size()) {
            throw new IllegalArgumentException("startStepIndex is out of range: " + startStepIndex);
        }
        if (stopBeforeStepIndex < startStepIndex || stopBeforeStepIndex > steps.size()) {
            throw new IllegalArgumentException("stopBeforeStepIndex is out of range: " + stopBeforeStepIndex);
        }
    }

    @FunctionalInterface
    public interface SyncStepInvoker<S> {
        Object invoke(S step, Object current, int stepIndex);
    }

    @FunctionalInterface
    public interface AsyncStepInvoker<S> {
        CompletionStage<Object> invoke(S step, Object current, int stepIndex);
    }

    @FunctionalInterface
    public interface NullStepHandler {
        void onNullStep(int stepIndex);
    }
}
