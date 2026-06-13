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

package org.pipelineframework.runtime.spring;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.pipelineframework.runtime.core.PipelineRunnerCore;
import org.pipelineframework.runtime.core.PipelineUnaryStep;

/**
 * Spring adapter for the framework-neutral pipeline runner core.
 */
public class SpringPipelineRunner {
    private final PipelineRunnerCore runnerCore;
    private final List<PipelineUnaryStep<?, ?>> steps;

    public SpringPipelineRunner(PipelineRunnerCore runnerCore, List<PipelineUnaryStep<?, ?>> steps) {
        this.runnerCore = Objects.requireNonNull(runnerCore, "runnerCore");
        this.steps = List.copyOf(Objects.requireNonNull(steps, "steps"));
    }

    /**
     * Run all configured unary steps sequentially.
     *
     * @param input input for the first step
     * @return stage that completes with the final step output
     */
    public CompletionStage<Object> run(Object input) {
        return runnerCore.runAsync(input, steps, this::applyUntyped);
    }

    public int stepCount() {
        return steps.size();
    }

    @SuppressWarnings("unchecked")
    private CompletionStage<Object> applyUntyped(PipelineUnaryStep<?, ?> step, Object input, int stepIndex) {
        PipelineUnaryStep<Object, Object> typedStep = (PipelineUnaryStep<Object, Object>) step;
        return typedStep.apply(input);
    }
}
