/*
 * Copyright (c) 2023-2025 Mariano Barcia
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */
package org.pipelineframework.telemetry;

/** Compatibility delegates for the focused APM metrics adapter. */
public final class ApmCompatibilityMetrics {
    private ApmCompatibilityMetrics() { }

    private static ApmCompatibilityMetricsRecorder delegate() {
        return TelemetryCompatibilityAccess.adapter(
            ApmCompatibilityMetricsRecorder.class, ApmCompatibilityMetricsRecorder::new);
    }

    public static void recordOrchestratorSuccess(double durationMs) {
        delegate().recordOrchestratorSuccess(durationMs);
    }

    public static void recordOrchestratorFailure(double durationMs) {
        delegate().recordOrchestratorFailure(durationMs);
    }
}
