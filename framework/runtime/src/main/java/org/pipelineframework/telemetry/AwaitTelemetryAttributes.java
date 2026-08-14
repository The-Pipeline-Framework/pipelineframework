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
import java.util.Optional;

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
        context(observation).ifPresent(context -> {
            putIfPresent(attributes, "tpf.await.execution_id", context.executionId());
            putIfPresent(attributes, "tpf.await.interaction_id", context.interactionId());
            putIfPresent(attributes, "tpf.await.correlation_id", context.correlationId());
            putIfPresent(attributes, "tpf.await.unit_id", context.unitId());
        });
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
        context(observation).ifPresent(context -> {
            attributes.put("tpf.await.step_id", normal(context.stepId()));
            attributes.put("tpf.await.transport", normal(context.transport()));
            attributes.put("tpf.await.status", normal(context.status()));
            context.cardinality().filter(value -> !value.isBlank())
                .ifPresent(value -> attributes.put("tpf.await.cardinality", value));
        });
        return attributes;
    }

    private static Optional<AwaitObservation.Context> context(AwaitObservation observation) {
        return switch (observation) {
            case AwaitObservation.CompletionDropped ignored -> Optional.empty();
            case AwaitObservation.InteractionCreated value -> Optional.of(value.context());
            case AwaitObservation.InteractionDispatched value -> Optional.of(value.context());
            case AwaitObservation.ProviderDispatched value -> Optional.of(value.context());
            case AwaitObservation.ProviderAdmitted value -> Optional.of(value.context());
            case AwaitObservation.ProviderCompletionDispatched value -> Optional.of(value.context());
            case AwaitObservation.CompletionAdmitted value -> Optional.of(value.context());
            case AwaitObservation.LiveHandoff value -> Optional.of(value.context());
            case AwaitObservation.ScalarContinuationStarted value -> Optional.of(value.context());
            case AwaitObservation.UnitDispatchCompleted value -> Optional.of(value.context());
            case AwaitObservation.ItemCompleted value -> Optional.of(value.context());
            case AwaitObservation.EarlyCompletionHeld value -> Optional.of(value.context());
            case AwaitObservation.ResumeReleased value -> Optional.of(value.context());
            case AwaitObservation.UnitTerminal value -> Optional.of(value.context());
            case AwaitObservation.AdmissionAcquired value -> Optional.of(value.context());
            case AwaitObservation.AdmissionReleased value -> Optional.of(value.context());
        };
    }

    private static void putIfPresent(Map<String, String> attributes, String key, Optional<String> value) {
        value.filter(item -> !item.isBlank()).ifPresent(item -> attributes.put(key, item));
    }

    private static String normal(Optional<String> value) {
        return value.filter(item -> !item.isBlank()).orElse("unknown");
    }

    private static String normal(String value) {
        return value == null || value.isBlank() ? "unknown" : value;
    }
}
