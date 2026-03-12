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

package org.pipelineframework.config.pipeline;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.pipelineframework.config.connector.ConnectorConfig;
import org.pipelineframework.config.connector.ConnectorSourceConfig;
import org.pipelineframework.config.connector.ConnectorTargetConfig;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PipelineYamlConfigPlatformTest {

    @Test
    void canonicalizesLegacyPlatformAliases() {
        PipelineYamlConfig fromLegacy = new PipelineYamlConfig(
            "org.example",
            "REST",
            "LAMBDA",
            List.of(),
            List.of());
        PipelineYamlConfig fromCanonical = new PipelineYamlConfig(
            "org.example",
            "REST",
            "FUNCTION",
            List.of(),
            List.of());
        PipelineYamlConfig fromStandard = new PipelineYamlConfig(
            "org.example",
            "REST",
            "STANDARD",
            List.of(),
            List.of());

        assertEquals("FUNCTION", fromLegacy.platform());
        assertEquals("FUNCTION", fromCanonical.platform());
        assertEquals("COMPUTE", fromStandard.platform());
    }

    @Test
    void defaultsToComputeWhenPlatformMissing() {
        PipelineYamlConfig config = new PipelineYamlConfig(
            "org.example",
            "REST",
            null,
            List.of(),
            List.of());
        assertEquals("COMPUTE", config.platform());
    }

    @Test
    void withTransportPreservesConnectors() {
        ConnectorConfig connector = new ConnectorConfig(
            "orders-to-delivery",
            true,
            new ConnectorSourceConfig("OUTPUT_BUS", "Order Ready", "com.example.ReadyOrder"),
            new ConnectorTargetConfig("LIVE_INGEST", "deliver-order", "com.example.DispatchOrder", "com.example.Target"),
            null,
            "GRPC",
            "PRE_FORWARD",
            "BUFFER",
            "PROPAGATE",
            256,
            10000,
            List.of("orderId"),
            null);
        PipelineYamlConfig config = new PipelineYamlConfig(
            "org.example",
            "REST",
            "FUNCTION",
            List.of(),
            List.of(),
            List.of(connector));

        PipelineYamlConfig updated = config.withTransport("GRPC");

        assertEquals("GRPC", updated.transport());
        assertEquals(config.connectors(), updated.connectors());
    }
}
