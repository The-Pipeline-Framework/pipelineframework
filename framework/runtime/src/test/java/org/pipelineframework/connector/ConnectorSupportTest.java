package org.pipelineframework.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.pipelineframework.context.TransportDispatchMetadata;

class ConnectorSupportTest {

    @Test
    void derivesDispatchMetadataFromRecordProperties() {
        TransportDispatchMetadata metadata = ConnectorSupport.ensureDispatchMetadata(
            null,
            "orders-to-delivery",
            new TestPayload("order-1", "customer-1", "ready-1"),
            List.of("orderId", "customerId", "readyAt"));

        assertNotNull(metadata);
        assertNotNull(metadata.idempotencyKey());
        assertEquals(
            ConnectorSupport.deterministicHandoffKey(
                "orders-to-delivery",
                "order-1",
                "customer-1",
                "ready-1"),
            metadata.idempotencyKey());
    }

    @Test
    void preservesExistingIdempotencyKey() {
        TransportDispatchMetadata existing = new TransportDispatchMetadata(
            "corr-1",
            "exec-1",
            "idem-1",
            0,
            10L,
            20L,
            "parent-1");

        TransportDispatchMetadata metadata = ConnectorSupport.ensureDispatchMetadata(
            existing,
            "orders-to-delivery",
            new TestPayload("order-1", "customer-1", "ready-1"),
            List.of("orderId"));

        assertEquals("idem-1", metadata.idempotencyKey());
        assertEquals("corr-1", metadata.correlationId());
    }

    @Test
    void readsMapAndBeanProperties() {
        assertEquals("value-1", ConnectorSupport.readProperty(Map.of("key", "value-1"), "key"));
        assertEquals("order-1", ConnectorSupport.readProperty(new TestPayload("order-1", "customer-1", "ready-1"), "orderId"));
    }

    @Test
    void returnsNullMetadataWhenKeyFieldsListIsEmpty() {
        TransportDispatchMetadata metadata = ConnectorSupport.ensureDispatchMetadata(
            null,
            "orders-to-delivery",
            new TestPayload("order-1", "customer-1", "ready-1"),
            List.of());

        assertNull(metadata);
    }

    private record TestPayload(String orderId, String customerId, String readyAt) {
    }
}
