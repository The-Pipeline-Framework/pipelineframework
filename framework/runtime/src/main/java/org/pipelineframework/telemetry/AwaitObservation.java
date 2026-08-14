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

import java.time.Instant;
import java.util.Map;

/** Granular facts emitted by the Await runtime. They contain no SDK behaviour. */
public sealed interface AwaitObservation permits AwaitObservation.InteractionCreated,
    AwaitObservation.InteractionDispatched, AwaitObservation.ProviderDispatched,
    AwaitObservation.ProviderAdmitted, AwaitObservation.ProviderCompletionDispatched,
    AwaitObservation.CompletionAdmitted, AwaitObservation.LiveHandoff,
    AwaitObservation.ScalarContinuationStarted, AwaitObservation.UnitDispatchCompleted,
    AwaitObservation.ItemCompleted, AwaitObservation.EarlyCompletionHeld,
    AwaitObservation.ResumeReleased, AwaitObservation.UnitTerminal,
    AwaitObservation.CompletionDropped, AwaitObservation.AdmissionAcquired,
    AwaitObservation.AdmissionReleased {

    Instant occurredAt();

    record Context(String stepId, String transport, String status, String cardinality,
                   String executionId, String interactionId, String correlationId, String unitId,
                   Map<String, Object> traceMetadata) {
        public Context {
            traceMetadata = traceMetadata == null ? Map.of() : Map.copyOf(traceMetadata);
        }
    }

    record InteractionCreated(Context context, Instant occurredAt) implements AwaitObservation { }
    record InteractionDispatched(Context context, Instant occurredAt) implements AwaitObservation { }
    record ProviderDispatched(Context context, Instant occurredAt) implements AwaitObservation { }
    record ProviderAdmitted(Context context, Instant occurredAt) implements AwaitObservation { }
    record ProviderCompletionDispatched(Context context, Instant occurredAt) implements AwaitObservation { }
    record CompletionAdmitted(Context context, long latencyMillis, Instant occurredAt) implements AwaitObservation { }
    record LiveHandoff(Context context, Instant occurredAt) implements AwaitObservation { }
    record ScalarContinuationStarted(Context context, Instant occurredAt) implements AwaitObservation { }
    record UnitDispatchCompleted(Context context, Instant occurredAt) implements AwaitObservation { }
    record ItemCompleted(Context context, Instant occurredAt) implements AwaitObservation { }
    record EarlyCompletionHeld(Context context, Instant occurredAt) implements AwaitObservation { }
    record ResumeReleased(Context context, Instant occurredAt) implements AwaitObservation { }
    record UnitTerminal(Context context, long durationMillis, Instant occurredAt) implements AwaitObservation { }
    record CompletionDropped(String transport, String reason, Instant occurredAt) implements AwaitObservation { }
    record AdmissionAcquired(Context context, boolean reused, boolean reconciled, long waitMillis,
                             boolean locallyTracked, Instant occurredAt) implements AwaitObservation { }
    record AdmissionReleased(Context context, boolean released, boolean locallyTracked,
                             Instant occurredAt) implements AwaitObservation { }
}
