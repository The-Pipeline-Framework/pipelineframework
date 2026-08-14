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

import java.util.LinkedHashMap;
import java.util.Map;

/** Pure, sink-specific attribute derivation for Await observations. */
public final class AwaitTelemetryAttributes {
    private AwaitTelemetryAttributes() { }

    public static Map<String, String> metricAttributes(AwaitObservation observation) {
        Map<String, String> attributes = lowCardinality(observation);
        if (observation instanceof AwaitObservation.CompletionDropped value) {
            attributes.put("tpf.await.completion.reason", normal(value.reason()));
        } else if (observation instanceof AwaitObservation.AdmissionAcquired value) {
            attributes.put("tpf.await.admission.outcome", value.reused() ? "reused" : "acquired");
        } else if (observation instanceof AwaitObservation.AdmissionReleased) {
            attributes.put("tpf.await.admission.outcome", "released");
        }
        return Map.copyOf(attributes);
    }

    public static Map<String, String> spanAttributes(AwaitObservation observation) {
        Map<String, String> attributes = lowCardinality(observation);
        AwaitObservation.Context context = context(observation);
        if (context != null) {
            putIfPresent(attributes, "tpf.await.execution_id", context.executionId());
            putIfPresent(attributes, "tpf.await.interaction_id", context.interactionId());
            putIfPresent(attributes, "tpf.await.correlation_id", context.correlationId());
            putIfPresent(attributes, "tpf.await.unit_id", context.unitId());
        }
        return Map.copyOf(attributes);
    }

    public static Map<String, String> replayAttributes(AwaitObservation observation) {
        return spanAttributes(observation);
    }

    private static Map<String, String> lowCardinality(AwaitObservation observation) {
        Map<String, String> attributes = new LinkedHashMap<>();
        if (observation instanceof AwaitObservation.CompletionDropped value) {
            attributes.put("tpf.await.transport", normal(value.transport()));
            return attributes;
        }
        AwaitObservation.Context context = context(observation);
        if (context != null) {
            attributes.put("tpf.await.step_id", normal(context.stepId()));
            attributes.put("tpf.await.transport", normal(context.transport()));
            attributes.put("tpf.await.status", normal(context.status()));
            if (context.cardinality() != null && !context.cardinality().isBlank()) {
                attributes.put("tpf.await.cardinality", context.cardinality());
            }
        }
        return attributes;
    }

    private static AwaitObservation.Context context(AwaitObservation observation) {
        if (observation instanceof AwaitObservation.CompletionDropped) return null;
        if (observation instanceof AwaitObservation.InteractionCreated value) return value.context();
        if (observation instanceof AwaitObservation.InteractionDispatched value) return value.context();
        if (observation instanceof AwaitObservation.ProviderDispatched value) return value.context();
        if (observation instanceof AwaitObservation.ProviderAdmitted value) return value.context();
        if (observation instanceof AwaitObservation.ProviderCompletionDispatched value) return value.context();
        if (observation instanceof AwaitObservation.CompletionAdmitted value) return value.context();
        if (observation instanceof AwaitObservation.LiveHandoff value) return value.context();
        if (observation instanceof AwaitObservation.ScalarContinuationStarted value) return value.context();
        if (observation instanceof AwaitObservation.UnitDispatchCompleted value) return value.context();
        if (observation instanceof AwaitObservation.ItemCompleted value) return value.context();
        if (observation instanceof AwaitObservation.EarlyCompletionHeld value) return value.context();
        if (observation instanceof AwaitObservation.ResumeReleased value) return value.context();
        if (observation instanceof AwaitObservation.UnitTerminal value) return value.context();
        if (observation instanceof AwaitObservation.AdmissionAcquired value) return value.context();
        return ((AwaitObservation.AdmissionReleased) observation).context();
    }

    private static void putIfPresent(Map<String, String> attributes, String key, String value) {
        if (value != null && !value.isBlank()) attributes.put(key, value);
    }

    private static String normal(String value) {
        return value == null || value.isBlank() ? "unknown" : value;
    }
}
