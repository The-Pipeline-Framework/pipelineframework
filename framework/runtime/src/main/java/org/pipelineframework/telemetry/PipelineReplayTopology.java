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

package org.pipelineframework.telemetry;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Build-time pipeline topology metadata used by live metrics and replay rendering.
 *
 * @param pipeline pipeline identifier
 * @param steps ordered steps, including optional side-effect branches
 * @param transitions step-to-step transitions
 */
public record PipelineReplayTopology(
    String pipeline,
    List<Step> steps,
    List<Transition> transitions
) {
    /**
     * Resolves step metadata by runtime step class name.
     *
     * @param runtimeStepClass runtime step class
     * @return matching step metadata when present
     */
    public Optional<Step> step(String runtimeStepClass) {
        if (runtimeStepClass == null || steps == null) {
            return Optional.empty();
        }
        return steps.stream()
            .filter(step -> runtimeStepClass.equals(step.runtimeStepClass()))
            .findFirst();
    }

    /**
     * Returns the first outbound transition for a runtime step.
     *
     * @param runtimeStepClass source runtime step class
     * @return outbound transition when present
     */
    public Optional<Transition> outbound(String runtimeStepClass) {
        if (runtimeStepClass == null || transitions == null) {
            return Optional.empty();
        }
        return transitions.stream()
            .filter(transition -> runtimeStepClass.equals(transition.fromRuntimeStepClass()))
            .findFirst();
    }

    /**
     * Returns the first inbound transition for a runtime step.
     *
     * @param runtimeStepClass target runtime step class
     * @return inbound transition when present
     */
    public Optional<Transition> inbound(String runtimeStepClass) {
        if (runtimeStepClass == null || transitions == null) {
            return Optional.empty();
        }
        return transitions.stream()
            .filter(transition -> runtimeStepClass.equals(transition.toRuntimeStepClass()))
            .findFirst();
    }

    /**
     * Creates a map keyed by runtime step class name.
     *
     * @return ordered descriptor map
     */
    public Map<String, Step> stepsByRuntimeClass() {
        LinkedHashMap<String, Step> ordered = new LinkedHashMap<>();
        if (steps != null) {
            for (Step step : steps) {
                if (step != null && step.runtimeStepClass() != null) {
                    ordered.put(step.runtimeStepClass(), step);
                }
            }
        }
        return java.util.Collections.unmodifiableMap(ordered);
    }

    /**
     * Ordered step descriptor.
     *
     * @param runtimeStepClass generated runtime step class
     * @param step logical step name
     * @param service delegate service name
     * @param cardinality canonical step cardinality
     * @param index ordinal position in the pipeline
     * @param sideEffect whether this step is a synthetic side-effect branch
     * @param parentStep logical parent step when this step branches from a base step
     * @param pluginKind canonical plugin kind when this is a plugin node
     */
    public record Step(
        String runtimeStepClass,
        String step,
        String service,
        String cardinality,
        int index,
        boolean sideEffect,
        String parentStep,
        String pluginKind
    ) {
    }

    /**
     * Directed transition between two steps.
     *
     * @param id stable edge identifier
     * @param fromRuntimeStepClass source runtime step class
     * @param toRuntimeStepClass target runtime step class
     * @param from source logical step name
     * @param to target logical step name
     * @param fromService source service name
     * @param toService target service name
     * @param cardinality source step cardinality
     */
    public record Transition(
        String id,
        String fromRuntimeStepClass,
        String toRuntimeStepClass,
        String from,
        String to,
        String fromService,
        String toService,
        String cardinality
    ) {
    }
}
