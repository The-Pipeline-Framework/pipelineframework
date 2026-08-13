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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.pipelineframework.config.pipeline.PipelineTelemetryResourceLoader;

/**
 * Pure, low-cardinality metric attribute derivation for pipeline observations.
 *
 * <p>The OpenTelemetry conversion belongs at the recorder boundary. Keeping this
 * value free of SDK types makes the metric contract directly characterisable.</p>
 */
final class PipelineMetricAttributes {
    private final Optional<PipelineReplayTopology> replayTopology;
    private final Optional<PipelineTelemetryResourceLoader.ItemBoundary> itemBoundary;
    private final Map<String, String> stepParents;

    PipelineMetricAttributes(
        Optional<PipelineReplayTopology> replayTopology,
        Optional<PipelineTelemetryResourceLoader.ItemBoundary> itemBoundary
    ) {
        this.replayTopology = replayTopology;
        this.itemBoundary = itemBoundary;
        this.stepParents = itemBoundary.map(PipelineTelemetryResourceLoader.ItemBoundary::stepParents).orElse(Map.of());
    }

    Map<String, String> run(String inputKind) {
        Map<String, String> attributes = new LinkedHashMap<>();
        attributes.put("tpf.input", inputKind == null ? "unknown" : inputKind);
        replayTopology.map(PipelineReplayTopology::pipeline)
            .filter(pipeline -> !pipeline.isBlank())
            .ifPresent(pipeline -> attributes.put("tpf.pipeline", pipeline));
        return Map.copyOf(attributes);
    }

    Map<String, String> step(Class<?> stepClass) {
        return step(resolveStepClassName(stepClass));
    }

    Map<String, String> step(String stepClassName) {
        if (stepClassName == null) {
            return Map.of();
        }
        Map<String, String> attributes = new LinkedHashMap<>();
        attributes.put("tpf.step.class", stepClassName);
        attributes.put("tpf.step.parent", stepParents.getOrDefault(stepClassName, stepClassName));
        replayTopology.flatMap(topology -> topology.step(stepClassName)).ifPresent(step -> {
            attributes.put("tpf.pipeline", replayTopology.orElseThrow().pipeline());
            putIfPresent(attributes, "tpf.step", step.step());
            putIfPresent(attributes, "tpf.service", step.service());
            putIfPresent(attributes, "tpf.cardinality", step.cardinality());
        });
        return Map.copyOf(attributes);
    }

    Map<String, String> boundary(Class<?> stepClass, boolean consumed) {
        return itemBoundary.map(boundary -> boundary(resolveStepClassName(stepClass),
            consumed ? boundary.itemInputType() : boundary.itemOutputType()))
            .orElseGet(() -> step(stepClass));
    }

    Map<String, String> boundary(String stepClassName, String itemType) {
        Map<String, String> attributes = new LinkedHashMap<>(step(stepClassName));
        attributes.put("tpf.item.type", itemType);
        return Map.copyOf(attributes);
    }

    Optional<PipelineTelemetryResourceLoader.ItemBoundary> itemBoundary() {
        return itemBoundary;
    }

    Optional<Map<String, String>> sloBoundary() {
        return itemBoundary.filter(boundary -> boundary.consumerStep() != null && !boundary.consumerStep().isBlank()
                && boundary.itemInputType() != null && !boundary.itemInputType().isBlank())
            .map(boundary -> boundary(boundary.consumerStep(), boundary.itemInputType()));
    }

    static String resolveStepClassName(Class<?> stepClass) {
        if (stepClass == null) {
            return null;
        }
        String name = stepClass.getName();
        if ((name.contains("_Subclass") || name.contains("$$") || name.contains("_ClientProxy"))
            && stepClass.getSuperclass() != null) {
            return stepClass.getSuperclass().getName();
        }
        return name;
    }

    private static void putIfPresent(Map<String, String> attributes, String key, String value) {
        if (value != null) {
            attributes.put(key, value);
        }
    }
}
