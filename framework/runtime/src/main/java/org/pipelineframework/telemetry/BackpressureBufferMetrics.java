/*
 * Copyright (c) 2026 Mariano Barcia
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */
package org.pipelineframework.telemetry;

import io.smallrye.mutiny.Multi;

/** Compatibility delegate for the stateful backpressure metrics adapter. */
public final class BackpressureBufferMetrics {
    private BackpressureBufferMetrics() { }

    private static BackpressureBufferMetricsRecorder delegate() {
        return TelemetryCompatibilityAccess.adapter(
            BackpressureBufferMetricsRecorder.class, BackpressureBufferMetricsRecorder::new);
    }

    public static <T> Multi<T> buffer(Multi<T> input, Class<?> stepClass, int capacity) {
        return delegate().buffer(input, stepClass, capacity);
    }
}
