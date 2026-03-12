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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class ConnectorConfigTest {

    @Test
    void constructorCreatesValidConnectorConfig() {
        ConnectorSourceConfig source = new ConnectorSourceConfig(
            "OUTPUT_BUS",
            "Order Ready",
            "com.example.ReadyOrder");
        ConnectorTargetConfig target = new ConnectorTargetConfig(
            "LIVE_INGEST",
            "deliver-order",
            "com.example.DispatchOrder",
            "com.example.DispatchTarget");

        ConnectorConfig config = new ConnectorConfig(
            "orders-to-delivery",
            true,
            source,
            target,
            "com.example.OrderMapper",
            "GRPC",
            "PRE_FORWARD",
            "BUFFER",
            "PROPAGATE",
            256,
            10000,
            List.of("orderId", "customerId"),
            null);

        assertNotNull(config);
        assertEquals("orders-to-delivery", config.name());
        assertTrue(config.enabled());
        assertEquals(source, config.source());
        assertEquals(target, config.target());
        assertEquals("com.example.OrderMapper", config.mapper());
        assertEquals("GRPC", config.transport());
        assertEquals("PRE_FORWARD", config.idempotency());
        assertEquals("BUFFER", config.backpressure());
        assertEquals("PROPAGATE", config.failureMode());
        assertEquals(256, config.backpressureBufferCapacity());
        assertEquals(10000, config.idempotencyMaxKeys());
        assertEquals(2, config.idempotencyKeyFields().size());
        assertEquals("orderId", config.idempotencyKeyFields().get(0));
        assertEquals("customerId", config.idempotencyKeyFields().get(1));
    }

    @Test
    void constructorRejectsNullName() {
        ConnectorSourceConfig source = new ConnectorSourceConfig("OUTPUT_BUS", "step", "Type");
        ConnectorTargetConfig target = new ConnectorTargetConfig("LIVE_INGEST", "pipeline", "Type", "Adapter");

        assertThrows(NullPointerException.class, () ->
            new ConnectorConfig(
                null,
                true,
                source,
                target,
                null,
                "GRPC",
                "PRE_FORWARD",
                "BUFFER",
                "PROPAGATE",
                256,
                10000,
                List.of(),
                null));
    }

    @Test
    void constructorRejectsBlankName() {
        ConnectorSourceConfig source = new ConnectorSourceConfig("OUTPUT_BUS", "step", "Type");
        ConnectorTargetConfig target = new ConnectorTargetConfig("LIVE_INGEST", "pipeline", "Type", "Adapter");

        assertThrows(IllegalArgumentException.class, () ->
            new ConnectorConfig(
                "",
                true,
                source,
                target,
                null,
                "GRPC",
                "PRE_FORWARD",
                "BUFFER",
                "PROPAGATE",
                256,
                10000,
                List.of(),
                null));
    }

    @Test
    void constructorRejectsNullSource() {
        ConnectorTargetConfig target = new ConnectorTargetConfig("LIVE_INGEST", "pipeline", "Type", "Adapter");

        assertThrows(NullPointerException.class, () ->
            new ConnectorConfig(
                "connector-1",
                true,
                null,
                target,
                null,
                "GRPC",
                "PRE_FORWARD",
                "BUFFER",
                "PROPAGATE",
                256,
                10000,
                List.of(),
                null));
    }

    @Test
    void constructorRejectsNullTarget() {
        ConnectorSourceConfig source = new ConnectorSourceConfig("OUTPUT_BUS", "step", "Type");

        assertThrows(NullPointerException.class, () ->
            new ConnectorConfig(
                "connector-1",
                true,
                source,
                null,
                null,
                "GRPC",
                "PRE_FORWARD",
                "BUFFER",
                "PROPAGATE",
                256,
                10000,
                List.of(),
                null));
    }

    @Test
    void constructorRejectsNegativeBackpressureBufferCapacity() {
        ConnectorSourceConfig source = new ConnectorSourceConfig("OUTPUT_BUS", "step", "Type");
        ConnectorTargetConfig target = new ConnectorTargetConfig("LIVE_INGEST", "pipeline", "Type", "Adapter");

        assertThrows(IllegalArgumentException.class, () ->
            new ConnectorConfig(
                "connector-1",
                true,
                source,
                target,
                null,
                "GRPC",
                "PRE_FORWARD",
                "BUFFER",
                "PROPAGATE",
                -1,
                10000,
                List.of(),
                null));
    }

    @Test
    void constructorRejectsNegativeIdempotencyMaxKeys() {
        ConnectorSourceConfig source = new ConnectorSourceConfig("OUTPUT_BUS", "step", "Type");
        ConnectorTargetConfig target = new ConnectorTargetConfig("LIVE_INGEST", "pipeline", "Type", "Adapter");

        assertThrows(IllegalArgumentException.class, () ->
            new ConnectorConfig(
                "connector-1",
                true,
                source,
                target,
                null,
                "GRPC",
                "PRE_FORWARD",
                "BUFFER",
                "PROPAGATE",
                256,
                -1,
                List.of(),
                null));
    }

    @Test
    void constructorAcceptsNullIdempotencyKeyFields() {
        ConnectorSourceConfig source = new ConnectorSourceConfig("OUTPUT_BUS", "step", "Type");
        ConnectorTargetConfig target = new ConnectorTargetConfig("LIVE_INGEST", "pipeline", "Type", "Adapter");

        ConnectorConfig config = new ConnectorConfig(
            "connector-1",
            true,
            source,
            target,
            null,
            "GRPC",
            "PRE_FORWARD",
            "BUFFER",
            "PROPAGATE",
            256,
            10000,
            null,
            null);

        assertNotNull(config.idempotencyKeyFields());
        assertTrue(config.idempotencyKeyFields().isEmpty());
    }

    @Test
    void constructorMakesIdempotencyKeyFieldsUnmodifiable() {
        ConnectorSourceConfig source = new ConnectorSourceConfig("OUTPUT_BUS", "step", "Type");
        ConnectorTargetConfig target = new ConnectorTargetConfig("LIVE_INGEST", "pipeline", "Type", "Adapter");

        List<String> fields = List.of("field1", "field2");
        ConnectorConfig config = new ConnectorConfig(
            "connector-1",
            true,
            source,
            target,
            null,
            "GRPC",
            "PRE_FORWARD",
            "BUFFER",
            "PROPAGATE",
            256,
            10000,
            fields,
            null);

        assertThrows(UnsupportedOperationException.class, () ->
            config.idempotencyKeyFields().add("field3"));
    }

    @Test
    void constructorAcceptsNullMapper() {
        ConnectorSourceConfig source = new ConnectorSourceConfig("OUTPUT_BUS", "step", "Type");
        ConnectorTargetConfig target = new ConnectorTargetConfig("LIVE_INGEST", "pipeline", "Type", "Adapter");

        ConnectorConfig config = new ConnectorConfig(
            "connector-1",
            true,
            source,
            target,
            null,
            "GRPC",
            "PRE_FORWARD",
            "BUFFER",
            "PROPAGATE",
            256,
            10000,
            List.of(),
            null);

        assertEquals(null, config.mapper());
    }

    @Test
    void constructorAcceptsNullBroker() {
        ConnectorSourceConfig source = new ConnectorSourceConfig("OUTPUT_BUS", "step", "Type");
        ConnectorTargetConfig target = new ConnectorTargetConfig("LIVE_INGEST", "pipeline", "Type", "Adapter");

        ConnectorConfig config = new ConnectorConfig(
            "connector-1",
            true,
            source,
            target,
            null,
            "GRPC",
            "PRE_FORWARD",
            "BUFFER",
            "PROPAGATE",
            256,
            10000,
            List.of(),
            null);

        assertEquals(null, config.broker());
    }

    @Test
    void constructorAcceptsValidBrokerConfig() {
        ConnectorSourceConfig source = new ConnectorSourceConfig("BROKER", "step", "Type");
        ConnectorTargetConfig target = new ConnectorTargetConfig("LIVE_INGEST", "pipeline", "Type", "Adapter");
        ConnectorBrokerConfig broker = new ConnectorBrokerConfig("SQS", "queue-url", "BrokerAdapter");

        ConnectorConfig config = new ConnectorConfig(
            "connector-1",
            true,
            source,
            target,
            null,
            "GRPC",
            "PRE_FORWARD",
            "BUFFER",
            "PROPAGATE",
            256,
            10000,
            List.of(),
            broker);

        assertEquals(broker, config.broker());
    }
}