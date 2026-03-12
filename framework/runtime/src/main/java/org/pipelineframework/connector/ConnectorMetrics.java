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

package org.pipelineframework.connector;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;

/**
 * Connector-level event counters.
 */
public final class ConnectorMetrics {
    private static final AttributeKey<String> CONNECTOR = AttributeKey.stringKey("tpf.connector.name");
    private static final AttributeKey<String> OUTCOME = AttributeKey.stringKey("tpf.connector.outcome");

    private static volatile LongCounter events;

    private ConnectorMetrics() {
    }

    public static void record(String connectorName, String outcome) {
        counter().add(1L, Attributes.of(
            CONNECTOR, connectorName == null || connectorName.isBlank() ? "unknown" : connectorName,
            OUTCOME, outcome == null || outcome.isBlank() ? "unknown" : outcome));
    }

    /**
     * For tests only.
     */
    static void resetForTest() {
        synchronized (ConnectorMetrics.class) {
            events = null;
        }
    }

    private static LongCounter counter() {
        LongCounter active = events;
        if (active != null) {
            return active;
        }
        synchronized (ConnectorMetrics.class) {
            if (events == null) {
                events = GlobalOpenTelemetry.getMeter("org.pipelineframework")
                    .counterBuilder("tpf.connector.events.total")
                    .setDescription("Connector events by connector name and outcome.")
                    .build();
            }
            return events;
        }
    }
}
