/*
 * Copyright (c) 2026 Mariano Barcia
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */
package org.pipelineframework.telemetry;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Executable contributor example: observation + pure derivation + obligation + adapter execution. */
class TelemetryExtensionGoldenPathTest {
    @Test
    void aNewFactDoesNotRequireFacadeOrRuntimeChanges() {
        LeaseAcquired observation = new LeaseAcquired("transition-worker", Instant.EPOCH);
        MetricSignal signal = derive(observation);
        RecordingMetricAdapter adapter = new RecordingMetricAdapter();

        adapter.record(signal);

        assertEquals(Obligation.LEASE_ACQUIRED, signal.obligation());
        assertEquals(List.of(signal), adapter.recorded);
    }

    private static MetricSignal derive(LeaseAcquired observation) {
        return new MetricSignal(Obligation.LEASE_ACQUIRED, "tpf.example.lease.acquired",
            Map.of("tpf.owner", observation.owner()));
    }

    private record LeaseAcquired(String owner, Instant occurredAt) { }
    private record MetricSignal(Obligation obligation, String name, Map<String, String> attributes) { }
    private enum Obligation { LEASE_ACQUIRED }

    private static final class RecordingMetricAdapter {
        private final List<MetricSignal> recorded = new ArrayList<>();
        void record(MetricSignal signal) { recorded.add(signal); }
    }
}
