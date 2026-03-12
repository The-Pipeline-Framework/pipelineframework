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

package org.pipelineframework.config.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class ConnectorTargetConfigTest {

    @Test
    void constructorCreatesValidTargetConfig() {
        ConnectorTargetConfig config = new ConnectorTargetConfig(
            "LIVE_INGEST",
            "deliver-order",
            "com.example.DispatchOrder",
            "com.example.DispatchTarget");

        assertNotNull(config);
        assertEquals("LIVE_INGEST", config.kind());
        assertEquals("deliver-order", config.pipeline());
        assertEquals("com.example.DispatchOrder", config.type());
        assertEquals("com.example.DispatchTarget", config.adapter());
    }

    @Test
    void constructorAllowsNullKind() {
        ConnectorTargetConfig config = new ConnectorTargetConfig(
            null,
            "pipeline",
            "type",
            "adapter");

        assertNull(config.kind());
    }

    @Test
    void constructorAllowsNullPipeline() {
        ConnectorTargetConfig config = new ConnectorTargetConfig(
            "LIVE_INGEST",
            null,
            "type",
            "adapter");

        assertNull(config.pipeline());
    }

    @Test
    void constructorAllowsNullType() {
        ConnectorTargetConfig config = new ConnectorTargetConfig(
            "LIVE_INGEST",
            "pipeline",
            null,
            "adapter");

        assertNull(config.type());
    }

    @Test
    void constructorAllowsNullAdapter() {
        ConnectorTargetConfig config = new ConnectorTargetConfig(
            "LIVE_INGEST",
            "pipeline",
            "type",
            null);

        assertNull(config.adapter());
    }

    @Test
    void targetConfigSupportsLiveIngestKind() {
        ConnectorTargetConfig config = new ConnectorTargetConfig(
            "LIVE_INGEST",
            "pipeline",
            "type",
            "adapter");

        assertEquals("LIVE_INGEST", config.kind());
    }

    @Test
    void targetConfigSupportsBrokerKind() {
        ConnectorTargetConfig config = new ConnectorTargetConfig(
            "BROKER",
            "pipeline",
            "type",
            "adapter");

        assertEquals("BROKER", config.kind());
    }

    @Test
    void targetConfigWithPipelineNameContainingHyphens() {
        ConnectorTargetConfig config = new ConnectorTargetConfig(
            "LIVE_INGEST",
            "deliver-order-pipeline",
            "type",
            "adapter");

        assertEquals("deliver-order-pipeline", config.pipeline());
    }

    @Test
    void targetConfigWithFullyQualifiedJavaType() {
        ConnectorTargetConfig config = new ConnectorTargetConfig(
            "LIVE_INGEST",
            "pipeline",
            "com.example.domain.order.DispatchOrderCommand",
            "adapter");

        assertEquals("com.example.domain.order.DispatchOrderCommand", config.type());
    }

    @Test
    void targetConfigWithFullyQualifiedAdapterClass() {
        ConnectorTargetConfig config = new ConnectorTargetConfig(
            "LIVE_INGEST",
            "pipeline",
            "type",
            "com.example.connector.DeliverOrderConnectorTarget");

        assertEquals("com.example.connector.DeliverOrderConnectorTarget", config.adapter());
    }

    @Test
    void targetConfigRecordEquality() {
        ConnectorTargetConfig config1 = new ConnectorTargetConfig("LIVE_INGEST", "pipeline", "type", "adapter");
        ConnectorTargetConfig config2 = new ConnectorTargetConfig("LIVE_INGEST", "pipeline", "type", "adapter");

        assertEquals(config1, config2);
    }

    @Test
    void targetConfigRecordToString() {
        ConnectorTargetConfig config = new ConnectorTargetConfig("LIVE_INGEST", "pipeline", "type", "adapter");

        String str = config.toString();
        assertNotNull(str);
        assertEquals(true, str.contains("LIVE_INGEST"));
        assertEquals(true, str.contains("pipeline"));
        assertEquals(true, str.contains("type"));
        assertEquals(true, str.contains("adapter"));
    }
}