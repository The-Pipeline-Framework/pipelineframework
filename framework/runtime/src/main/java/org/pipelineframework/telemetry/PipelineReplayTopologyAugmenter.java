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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class PipelineReplayTopologyAugmenter {

    private PipelineReplayTopologyAugmenter() {
    }

    static PipelineReplayTopology augment(
        PipelineReplayTopology baseTopology,
        List<PipelineExecutionEvent> executionEvents) {
        if (baseTopology == null || executionEvents == null || executionEvents.isEmpty()) {
            return baseTopology;
        }
        List<PipelineReplayTopology.Step> mergedSteps =
            new ArrayList<>(baseTopology.steps() == null ? List.of() : baseTopology.steps());
        List<PipelineReplayTopology.Transition> mergedTransitions =
            new ArrayList<>(baseTopology.transitions() == null ? List.of() : baseTopology.transitions());
        Map<String, PipelineReplayTopology.Step> stepsByName = new LinkedHashMap<>();
        for (PipelineReplayTopology.Step step : mergedSteps) {
            stepsByName.put(step.step(), step);
        }

        int nextIndex = mergedSteps.stream().mapToInt(PipelineReplayTopology.Step::index).max().orElse(-1) + 1;
        List<PipelineExecutionEvent> orderedEvents = executionEvents.stream()
            .sorted(
                Comparator.comparingDouble(PipelineExecutionEvent::startTime)
                    .thenComparing(event -> event.sequence() == null ? 0L : event.sequence())
                    .thenComparing(event -> event.step() == null ? "" : event.step())
                    .thenComparing(event -> event.service() == null ? "" : event.service()))
            .toList();
        for (PipelineExecutionEvent event : orderedEvents) {
            PipelineReplayTopology.Step step = stepsByName.get(event.step());
            if (step == null) {
                boolean sideEffect = event.from() != null && (event.to() == null || event.to().equals(event.step()));
                step = new PipelineReplayTopology.Step(
                    "runtime::" + event.step(),
                    event.step(),
                    event.service(),
                    event.cardinality(),
                    nextIndex++,
                    sideEffect,
                    sideEffect ? event.from() : null,
                    inferPluginKind(event, sideEffect));
                mergedSteps.add(step);
                stepsByName.put(step.step(), step);
            }
            if (event.from() != null && event.to() != null) {
                ensureTransition(mergedTransitions, stepsByName, event.from(), event.to(), event.cardinality());
            } else if (event.from() != null
                && event.step() != null
                && (event.to() == null || event.to().equals(event.step()))) {
                ensureTransition(mergedTransitions, stepsByName, event.from(), event.step(), event.cardinality());
            }
        }
        return new PipelineReplayTopology(baseTopology.pipeline(), List.copyOf(mergedSteps), List.copyOf(mergedTransitions));
    }

    private static String inferPluginKind(PipelineExecutionEvent event, boolean sideEffect) {
        if (!sideEffect || event == null) {
            return null;
        }
        if ("reject".equals(event.event())) {
            return "reject";
        }
        String stepName = event.step() == null ? "" : event.step().toLowerCase();
        String serviceName = event.service() == null ? "" : event.service().toLowerCase();
        String combined = stepName + " " + serviceName;
        if (combined.contains("invalidateall")) {
            return "cache-invalidate-all";
        }
        if (combined.contains("invalidate")) {
            return "cache-invalidate";
        }
        if (combined.contains("cache")) {
            return "cache";
        }
        if (combined.contains("persist")) {
            return "persistence";
        }
        return null;
    }

    private static void ensureTransition(
        List<PipelineReplayTopology.Transition> transitions,
        Map<String, PipelineReplayTopology.Step> stepsByName,
        String from,
        String to,
        String cardinality) {
        if (from == null || to == null || from.equals(to)) {
            return;
        }
        for (PipelineReplayTopology.Transition transition : transitions) {
            if (from.equals(transition.from()) && to.equals(transition.to())) {
                return;
            }
        }
        PipelineReplayTopology.Step fromStep = stepsByName.get(from);
        PipelineReplayTopology.Step toStep = stepsByName.get(to);
        transitions.add(new PipelineReplayTopology.Transition(
            from + "->" + to,
            fromStep == null ? "runtime::" + from : fromStep.runtimeStepClass(),
            toStep == null ? "runtime::" + to : toStep.runtimeStepClass(),
            from,
            to,
            fromStep == null ? from + "Service" : fromStep.service(),
            toStep == null ? to + "Service" : toStep.service(),
            cardinality == null ? "one-to-one" : cardinality));
    }
}
