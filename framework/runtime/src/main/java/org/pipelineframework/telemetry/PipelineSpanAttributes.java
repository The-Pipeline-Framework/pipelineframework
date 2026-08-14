/*
 * Copyright (c) 2026 Mariano Barcia
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */
package org.pipelineframework.telemetry;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/** Pure span attributes, intentionally independent from metric-cardinality derivation. */
final class PipelineSpanAttributes {
    private final Optional<PipelineReplayTopology> topology;

    PipelineSpanAttributes(Optional<PipelineReplayTopology> topology) {
        this.topology = topology;
    }

    Map<String, String> run(String inputKind) {
        return Map.of(
            "tpf.input", inputKind == null ? "unknown" : inputKind,
            "tpf.pipeline", topology.map(PipelineReplayTopology::pipeline).orElse("pipeline"));
    }

    Map<String, String> step(String stepClass) {
        Map<String, String> attributes = new LinkedHashMap<>();
        attributes.put("tpf.step.class", stepClass);
        topology.flatMap(value -> value.step(stepClass)).ifPresent(descriptor -> {
            attributes.put("tpf.pipeline", topology.orElseThrow().pipeline());
            put(attributes, "tpf.step", descriptor.step());
            put(attributes, "tpf.service", descriptor.service());
            put(attributes, "tpf.cardinality", descriptor.cardinality());
        });
        return Map.copyOf(attributes);
    }

    private static void put(Map<String, String> attributes, String key, String value) {
        if (value != null) {
            attributes.put(key, value);
        }
    }
}
