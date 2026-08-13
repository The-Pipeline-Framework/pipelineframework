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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AwaitTelemetryAttributesTest {
    @Test
    void metricsRemainLowCardinalityWhileSpansCarryJourneyIdentity() {
        AwaitObservation.Context context = new AwaitObservation.Context("AwaitPayment", "GRPC", "COMPLETED",
            "ONE_TO_ONE", "execution-1", "interaction-1", "correlation-1", "unit-1", Map.of());
        AwaitObservation observation = new AwaitObservation.CompletionAdmitted(context, 42L, Instant.EPOCH);

        Map<String, String> metrics = AwaitTelemetryAttributes.metricAttributes(observation);
        Map<String, String> spans = AwaitTelemetryAttributes.spanAttributes(observation);

        assertFalse(metrics.containsKey("tpf.await.execution_id"));
        assertFalse(metrics.containsKey("tpf.await.interaction_id"));
        assertFalse(metrics.containsKey("tpf.await.correlation_id"));
        assertTrue(spans.containsKey("tpf.await.execution_id"));
        assertTrue(spans.containsKey("tpf.await.interaction_id"));
        assertTrue(spans.containsKey("tpf.await.correlation_id"));
    }
}
