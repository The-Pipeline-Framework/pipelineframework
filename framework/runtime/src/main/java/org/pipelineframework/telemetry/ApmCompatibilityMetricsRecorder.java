/*
 * Copyright (c) 2023-2025 Mariano Barcia
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

import jakarta.inject.Singleton;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;

/**
 * Emits New Relic APM-compatible metrics for orchestrator CLI runs.
 *
 * <p>These metrics are a compatibility shim when RPC metrics do not
 * synthesize APM transactions for short-lived orchestrator runs.</p>
 */
@Singleton
final class ApmCompatibilityMetricsRecorder {

    private final AttributeKey<String> TRANSACTION_TYPE = AttributeKey.stringKey("transaction.type");
    private final AttributeKey<String> TRANSACTION_NAME = AttributeKey.stringKey("transaction.name");

    ApmCompatibilityMetricsRecorder() {
    }

    /**
     * Record a successful orchestrator transaction duration.
     *
     * @param durationMs duration in milliseconds
     */
    public void recordOrchestratorSuccess(double durationMs) {
        record(durationMs, false);
    }

    /**
     * Record a failed orchestrator transaction duration.
     *
     * @param durationMs duration in milliseconds
     */
    public void recordOrchestratorFailure(double durationMs) {
        record(durationMs, true);
    }

    private void record(double durationMs, boolean error) {
        Instruments instruments = instruments();
        Attributes attributes = Attributes.builder()
            .put(TRANSACTION_TYPE, "Other")
            .put(TRANSACTION_NAME, "OtherTransaction/OrchestratorService/Run")
            .build();
        instruments.transactionCount().add(1, attributes);
        instruments.transactionDuration().record(durationMs, attributes);
        if (error) {
            instruments.errorCount().add(1, attributes);
        }
    }

    private Instruments instruments() {
        Meter meter = TelemetryCompatibilityAccess.metricsRuntime().meter("org.pipelineframework.apm");
        return new Instruments(meter.counterBuilder("apm.service.transaction.count").build(),
            meter.counterBuilder("apm.service.error.count").build(),
            meter.histogramBuilder("apm.service.transaction.duration").setUnit("ms").build());
    }

    private record Instruments(LongCounter transactionCount, LongCounter errorCount,
                               DoubleHistogram transactionDuration) { }
}
