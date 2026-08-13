/*
 * Copyright (c) 2026 Mariano Barcia
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */
package org.pipelineframework.orchestrator;

import org.pipelineframework.telemetry.TelemetryCompatibilityAccess;

/** Compatibility delegate for dead-letter metrics. */
final class DeadLetterMetrics {
    private DeadLetterMetrics() { }

    private static DeadLetterMetricsRecorder delegate() {
        return TelemetryCompatibilityAccess.adapter(DeadLetterMetricsRecorder.class, DeadLetterMetricsRecorder::new);
    }

    static void record(String provider, DeadLetterEnvelope envelope) {
        delegate().record(provider, envelope);
    }
}
