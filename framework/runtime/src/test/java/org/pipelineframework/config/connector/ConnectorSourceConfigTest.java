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

class ConnectorSourceConfigTest {

    @Test
    void constructorCreatesValidSourceConfig() {
        ConnectorSourceConfig config = new ConnectorSourceConfig(
            "OUTPUT_BUS",
            "Order Ready",
            "com.example.ReadyOrder");

        assertNotNull(config);
        assertEquals("OUTPUT_BUS", config.kind());
        assertEquals("Order Ready", config.step());
        assertEquals("com.example.ReadyOrder", config.type());
    }

    @Test
    void constructorAllowsNullKind() {
        ConnectorSourceConfig config = new ConnectorSourceConfig(
            null,
            "step",
            "type");

        assertNull(config.kind());
        assertEquals("step", config.step());
    }

    @Test
    void constructorAllowsBlankKind() {
        ConnectorSourceConfig config = new ConnectorSourceConfig(
            "",
            "step",
            "type");

        assertEquals("", config.kind());
    }

    @Test
    void constructorAllowsNullStep() {
        ConnectorSourceConfig config = new ConnectorSourceConfig(
            "OUTPUT_BUS",
            null,
            "type");

        assertNull(config.step());
    }

    @Test
    void constructorAllowsNullType() {
        ConnectorSourceConfig config = new ConnectorSourceConfig(
            "OUTPUT_BUS",
            "step",
            null);

        assertNull(config.type());
    }

    @Test
    void sourceConfigSupportsOutputBusKind() {
        ConnectorSourceConfig config = new ConnectorSourceConfig(
            "OUTPUT_BUS",
            "step",
            "type");

        assertEquals("OUTPUT_BUS", config.kind());
    }

    @Test
    void sourceConfigSupportsBrokerKind() {
        ConnectorSourceConfig config = new ConnectorSourceConfig(
            "BROKER",
            "step",
            "type");

        assertEquals("BROKER", config.kind());
    }

    @Test
    void sourceConfigWithStepNameContainingSpaces() {
        ConnectorSourceConfig config = new ConnectorSourceConfig(
            "OUTPUT_BUS",
            "Order Ready Step",
            "com.example.Type");

        assertEquals("Order Ready Step", config.step());
    }

    @Test
    void sourceConfigWithFullyQualifiedJavaType() {
        ConnectorSourceConfig config = new ConnectorSourceConfig(
            "OUTPUT_BUS",
            "step",
            "com.example.domain.order.ReadyOrderEvent");

        assertEquals("com.example.domain.order.ReadyOrderEvent", config.type());
    }

    @Test
    void sourceConfigRecordEquality() {
        ConnectorSourceConfig config1 = new ConnectorSourceConfig("OUTPUT_BUS", "step", "type");
        ConnectorSourceConfig config2 = new ConnectorSourceConfig("OUTPUT_BUS", "step", "type");

        assertEquals(config1, config2);
    }

    @Test
    void sourceConfigRecordToString() {
        ConnectorSourceConfig config = new ConnectorSourceConfig("OUTPUT_BUS", "step", "type");

        String str = config.toString();
        assertNotNull(str);
        assertEquals(true, str.contains("OUTPUT_BUS"));
        assertEquals(true, str.contains("step"));
        assertEquals(true, str.contains("type"));
    }
}