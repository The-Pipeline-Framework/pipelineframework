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

package org.pipelineframework.reject;

import jakarta.inject.Singleton;

import org.pipelineframework.telemetry.TelemetryCompatibilityAccess;

import org.pipelineframework.telemetry.TelemetryRuntimes;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;

/**
 * Item reject sink metrics helper.
 */
@Singleton
final class ItemRejectMetricsRecorder {

    private final AttributeKey<String> PROVIDER = AttributeKey.stringKey("tpf.reject.provider");
    private final AttributeKey<String> STEP_CLASS = AttributeKey.stringKey("tpf.step.class");
    private final AttributeKey<String> REJECT_SCOPE = AttributeKey.stringKey("tpf.reject.scope");
    private final AttributeKey<String> ERROR_CLASS = AttributeKey.stringKey("tpf.error.class");

    /**
     * Prevents instantiation of this utility class.
     */
    ItemRejectMetricsRecorder() {
    }

    /**
     * Record a single item-reject publication in the reject metrics counter.
     *
     * Increments the reject counter and attaches attributes for the sink provider (uses
     * "unknown" if `provider` is null), the step class, the reject scope, and the error class
     * from the provided envelope.
     *
     * @param provider the sink provider name (may be null)
     * @param envelope the reject envelope containing stepClass, rejectScope, and errorClass; if null no metric is recorded
     */
    public void record(String provider, ItemRejectEnvelope envelope) {
        if (envelope == null) {
            return;
        }
        counter().add(1, Attributes.of(
            PROVIDER, provider == null ? "unknown" : provider,
            STEP_CLASS, envelope.stepClass(),
            REJECT_SCOPE, envelope.rejectScope(),
            ERROR_CLASS, envelope.errorClass()));
    }

    private LongCounter counter() {
        return TelemetryCompatibilityAccess.metricsRuntime().meter("org.pipelineframework")
            .counterBuilder("tpf.step.reject.total")
            .setDescription("Total rejected step items routed to item reject sinks")
            .setUnit("items")
            .build();
    }
}
