/*
 * Copyright (c) 2026 Mariano Barcia
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */
package org.pipelineframework.telemetry.derivation;

import java.util.Map;
import java.util.Optional;

/** Pure SLO signal derivation from a completed run snapshot. */
public final class PipelineSloDerivation {
    private PipelineSloDerivation() { }

    public static Optional<ThroughputSignal> throughput(Map<String, String> attributes, long consumed,
                                                         double durationMillis, double thresholdPerMinute) {
        if (attributes.isEmpty() || consumed < 0L || !Double.isFinite(durationMillis) || durationMillis <= 0d
                || !Double.isFinite(thresholdPerMinute)) {
            return Optional.empty();
        }
        double itemsPerMinute = consumed / (durationMillis / 60_000d);
        return Optional.of(new ThroughputSignal(attributes, itemsPerMinute >= thresholdPerMinute));
    }

    public static Optional<SuccessSignal> success(Map<String, String> attributes, long consumed, long produced) {
        if (attributes.isEmpty() || consumed <= 0L || produced < 0L) {
            return Optional.empty();
        }
        return Optional.of(new SuccessSignal(attributes, consumed, Math.min(consumed, produced)));
    }

    public record ThroughputSignal(Map<String, String> attributes, boolean good) {
        public ThroughputSignal { attributes = Map.copyOf(attributes); }
    }

    public record SuccessSignal(Map<String, String> attributes, long total, long good) {
        public SuccessSignal { attributes = Map.copyOf(attributes); }
    }
}
