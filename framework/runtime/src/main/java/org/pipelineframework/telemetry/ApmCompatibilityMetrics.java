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

import io.opentelemetry.api.GlobalOpenTelemetry;
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
public final class ApmCompatibilityMetrics {

    private static volatile Meter meter;
    private static volatile LongCounter transactionCount;
    private static volatile LongCounter errorCount;
    private static volatile DoubleHistogram transactionDuration;

    private static final AttributeKey<String> TRANSACTION_TYPE = AttributeKey.stringKey("transaction.type");
    private static final AttributeKey<String> TRANSACTION_NAME = AttributeKey.stringKey("transaction.name");

    private ApmCompatibilityMetrics() {
    }

    /**
     * Record a successful orchestrator transaction duration.
     *
     * @param durationMs duration in milliseconds
     */
    public static void recordOrchestratorSuccess(double durationMs) {
        record(durationMs, false);
    }

    /**
     * Record a failed orchestrator transaction duration.
     *
     * @param durationMs duration in milliseconds
     */
    public static void recordOrchestratorFailure(double durationMs) {
        record(durationMs, true);
    }

    static void resetForTest() {
        meter = null;
        transactionCount = null;
        errorCount = null;
        transactionDuration = null;
    }

    private static void record(double durationMs, boolean error) {
        ensureInitialized();
        Attributes attributes = Attributes.builder()
            .put(TRANSACTION_TYPE, "Other")
            .put(TRANSACTION_NAME, "OtherTransaction/OrchestratorService/Run")
            .build();
        transactionCount.add(1, attributes);
        transactionDuration.record(durationMs, attributes);
        if (error) {
            errorCount.add(1, attributes);
        }
    }

    private static void ensureInitialized() {
        if (meter != null) {
            return;
        }
        synchronized (ApmCompatibilityMetrics.class) {
            if (meter != null) {
                return;
            }
            Meter localMeter = GlobalOpenTelemetry.getMeter("org.pipelineframework.apm");
            transactionCount = localMeter.counterBuilder("apm.service.transaction.count").build();
            errorCount = localMeter.counterBuilder("apm.service.error.count").build();
            transactionDuration = localMeter.histogramBuilder("apm.service.transaction.duration").setUnit("ms").build();
            meter = localMeter;
        }
    }
}
