package org.pipelineframework.checkout.create_order.connector;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.Cancellable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.PipelineOutputBus;
import org.pipelineframework.checkout.createorder.grpc.OrderReadySvc;
import org.pipelineframework.checkout.deliverorder.grpc.OrderDispatchSvc;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.pipelineframework.checkout.common.connector.TestAwaitUtils.awaitUntil;

class CreateToDeliverIngestBridgeTest {

    private PipelineOutputBus outputBus;
    private DeliverOrderIngestClient deliverOrderIngestClient;
    private Cancellable mockCancellable;

    @BeforeEach
    void setUp() {
        outputBus = new PipelineOutputBus();
        deliverOrderIngestClient = mock(DeliverOrderIngestClient.class);
        mockCancellable = mock(Cancellable.class);
        when(deliverOrderIngestClient.forward(any())).thenReturn(mockCancellable);
    }

    @Test
    void constructorRejectsNullOutputBus() {
        assertThrows(NullPointerException.class, () ->
            new CreateToDeliverIngestBridge(null, deliverOrderIngestClient, true, 1000, "BUFFER", 256));
    }

    @Test
    void constructorRejectsNullDeliverOrderIngestClient() {
        assertThrows(NullPointerException.class, () ->
            new CreateToDeliverIngestBridge(outputBus, null, true, 1000, "BUFFER", 256));
    }

    @Test
    void constructorNormalizesInvalidIdempotencyMaxKeys() {
        CreateToDeliverIngestBridge bridge = new CreateToDeliverIngestBridge(
            outputBus, deliverOrderIngestClient, true, -1, "BUFFER", 256);
        assertNotNull(bridge);
    }

    @Test
    void constructorNormalizesInvalidBackpressureStrategy() {
        CreateToDeliverIngestBridge bridge = new CreateToDeliverIngestBridge(
            outputBus, deliverOrderIngestClient, true, 1000, "INVALID", 256);
        assertNotNull(bridge);
    }

    @Test
    void constructorNormalizesInvalidBackpressureBufferCapacity() {
        CreateToDeliverIngestBridge bridge = new CreateToDeliverIngestBridge(
            outputBus, deliverOrderIngestClient, true, 1000, "BUFFER", -1);
        assertNotNull(bridge);
    }

    @Test
    void onStartupStartsForwardingToDeliverOrderIngestClient() {
        CreateToDeliverIngestBridge bridge = new CreateToDeliverIngestBridge(
            outputBus, deliverOrderIngestClient, false, 1000, "BUFFER", 256);

        bridge.onStartup(mock(StartupEvent.class));

        verify(deliverOrderIngestClient, times(1)).forward(any());
    }

    @Test
    void onShutdownCancelsForwardingSubscription() {
        CreateToDeliverIngestBridge bridge = new CreateToDeliverIngestBridge(
            outputBus, deliverOrderIngestClient, false, 1000, "BUFFER", 256);

        bridge.onStartup(mock(StartupEvent.class));
        bridge.onShutdown();

        verify(mockCancellable, times(1)).cancel();
    }

    @Test
    void onShutdownHandlesMissingSubscription() {
        CreateToDeliverIngestBridge bridge = new CreateToDeliverIngestBridge(
            outputBus, deliverOrderIngestClient, false, 1000, "BUFFER", 256);

        bridge.onShutdown();

        verify(mockCancellable, never()).cancel();
    }

    @Test
    void bridgeForwardsReadyOrderToDeliverIngest() {
        List<OrderDispatchSvc.ReadyOrder> forwardedOrders = new ArrayList<>();
        DeliverOrderIngestClient capturingClient = stream -> {
            stream.subscribe().with(forwardedOrders::add);
            return mock(Cancellable.class);
        };

        CreateToDeliverIngestBridge bridge = new CreateToDeliverIngestBridge(
            outputBus, capturingClient, false, 1000, "BUFFER", 256);
        bridge.onStartup(mock(StartupEvent.class));

        OrderReadySvc.ReadyOrder readyOrder = OrderReadySvc.ReadyOrder.newBuilder()
            .setOrderId("order-1")
            .setCustomerId("customer-1")
            .setReadyAt("2026-03-07T10:00:00Z")
            .build();

        outputBus.publish(readyOrder);

        awaitUntil(() -> forwardedOrders.size() == 1, "Expected one forwarded ready order");
        assertEquals(1, forwardedOrders.size());
        assertEquals("order-1", forwardedOrders.get(0).getOrderId());
        assertEquals("customer-1", forwardedOrders.get(0).getCustomerId());
        assertEquals("2026-03-07T10:00:00Z", forwardedOrders.get(0).getReadyAt());
    }

    @Test
    void bridgeForwardsMultipleReadyOrders() {
        List<OrderDispatchSvc.ReadyOrder> forwardedOrders = new ArrayList<>();
        DeliverOrderIngestClient capturingClient = stream -> {
            stream.subscribe().with(forwardedOrders::add);
            return mock(Cancellable.class);
        };

        CreateToDeliverIngestBridge bridge = new CreateToDeliverIngestBridge(
            outputBus, capturingClient, false, 1000, "BUFFER", 256);
        bridge.onStartup(mock(StartupEvent.class));

        for (int i = 1; i <= 5; i++) {
            OrderReadySvc.ReadyOrder readyOrder = OrderReadySvc.ReadyOrder.newBuilder()
                .setOrderId("order-" + i)
                .setCustomerId("customer-" + i)
                .setReadyAt("2026-03-07T10:00:00Z")
                .build();
            outputBus.publish(readyOrder);
        }

        awaitUntil(() -> forwardedOrders.size() == 5, "Expected five forwarded ready orders");
        assertEquals(5, forwardedOrders.size());
    }

    @Test
    void bridgeDropsReadyOrderWithMissingOrderId() {
        List<OrderDispatchSvc.ReadyOrder> forwardedOrders = new ArrayList<>();
        DeliverOrderIngestClient capturingClient = stream -> {
            stream.subscribe().with(forwardedOrders::add);
            return mock(Cancellable.class);
        };

        CreateToDeliverIngestBridge bridge = new CreateToDeliverIngestBridge(
            outputBus, capturingClient, false, 1000, "BUFFER", 256);
        bridge.onStartup(mock(StartupEvent.class));

        OrderReadySvc.ReadyOrder readyOrder = OrderReadySvc.ReadyOrder.newBuilder()
            .setOrderId("")
            .setCustomerId("customer-1")
            .setReadyAt("2026-03-07T10:00:00Z")
            .build();
        outputBus.publish(readyOrder);

        assertTrue(forwardedOrders.isEmpty());
    }

    @Test
    void bridgeDropsReadyOrderWithMissingCustomerId() {
        List<OrderDispatchSvc.ReadyOrder> forwardedOrders = new ArrayList<>();
        DeliverOrderIngestClient capturingClient = stream -> {
            stream.subscribe().with(forwardedOrders::add);
            return mock(Cancellable.class);
        };

        CreateToDeliverIngestBridge bridge = new CreateToDeliverIngestBridge(
            outputBus, capturingClient, false, 1000, "BUFFER", 256);
        bridge.onStartup(mock(StartupEvent.class));

        OrderReadySvc.ReadyOrder readyOrder = OrderReadySvc.ReadyOrder.newBuilder()
            .setOrderId("order-1")
            .setCustomerId("")
            .setReadyAt("2026-03-07T10:00:00Z")
            .build();
        outputBus.publish(readyOrder);

        assertTrue(forwardedOrders.isEmpty());
    }

    @Test
    void bridgeDropsReadyOrderWithMissingReadyAt() {
        List<OrderDispatchSvc.ReadyOrder> forwardedOrders = new ArrayList<>();
        DeliverOrderIngestClient capturingClient = stream -> {
            stream.subscribe().with(forwardedOrders::add);
            return mock(Cancellable.class);
        };

        CreateToDeliverIngestBridge bridge = new CreateToDeliverIngestBridge(
            outputBus, capturingClient, false, 1000, "BUFFER", 256);
        bridge.onStartup(mock(StartupEvent.class));

        OrderReadySvc.ReadyOrder readyOrder = OrderReadySvc.ReadyOrder.newBuilder()
            .setOrderId("order-1")
            .setCustomerId("customer-1")
            .setReadyAt("")
            .build();
        outputBus.publish(readyOrder);

        assertTrue(forwardedOrders.isEmpty());
    }

    @Test
    void bridgeMapsGenericProtobufMessageWithCorrectFields() {
        List<OrderDispatchSvc.ReadyOrder> forwardedOrders = new ArrayList<>();
        DeliverOrderIngestClient capturingClient = stream -> {
            stream.subscribe().with(forwardedOrders::add);
            return mock(Cancellable.class);
        };

        CreateToDeliverIngestBridge bridge = new CreateToDeliverIngestBridge(
            outputBus, capturingClient, false, 1000, "BUFFER", 256);
        bridge.onStartup(mock(StartupEvent.class));

        Message mockMessage = mock(Message.class);
        Descriptors.Descriptor descriptor = mock(Descriptors.Descriptor.class);
        Descriptors.FieldDescriptor orderIdField = createMockStringField();
        Descriptors.FieldDescriptor customerIdField = createMockStringField();
        Descriptors.FieldDescriptor readyAtField = createMockStringField();

        when(mockMessage.getDescriptorForType()).thenReturn(descriptor);
        when(descriptor.findFieldByName("order_id")).thenReturn(orderIdField);
        when(descriptor.findFieldByName("customer_id")).thenReturn(customerIdField);
        when(descriptor.findFieldByName("ready_at")).thenReturn(readyAtField);
        when(mockMessage.getField(orderIdField)).thenReturn("order-1");
        when(mockMessage.getField(customerIdField)).thenReturn("customer-1");
        when(mockMessage.getField(readyAtField)).thenReturn("2026-03-07T10:00:00Z");

        outputBus.publish(mockMessage);

        awaitUntil(() -> forwardedOrders.size() == 1, "Expected mapped protobuf message to be forwarded");
        assertEquals(1, forwardedOrders.size());
        assertEquals("order-1", forwardedOrders.get(0).getOrderId());
    }

    @Test
    void bridgeDropsGenericProtobufMessageWithMissingFields() {
        List<OrderDispatchSvc.ReadyOrder> forwardedOrders = new ArrayList<>();
        DeliverOrderIngestClient capturingClient = stream -> {
            stream.subscribe().with(forwardedOrders::add);
            return mock(Cancellable.class);
        };

        CreateToDeliverIngestBridge bridge = new CreateToDeliverIngestBridge(
            outputBus, capturingClient, false, 1000, "BUFFER", 256);
        bridge.onStartup(mock(StartupEvent.class));

        Message mockMessage = mock(Message.class);
        Descriptors.Descriptor descriptor = mock(Descriptors.Descriptor.class);

        when(mockMessage.getDescriptorForType()).thenReturn(descriptor);
        when(descriptor.findFieldByName(any())).thenReturn(null);

        outputBus.publish(mockMessage);

        assertTrue(forwardedOrders.isEmpty());
    }

    @Test
    void bridgeDropsUnsupportedItemTypes() {
        List<OrderDispatchSvc.ReadyOrder> forwardedOrders = new ArrayList<>();
        DeliverOrderIngestClient capturingClient = stream -> {
            stream.subscribe().with(forwardedOrders::add);
            return mock(Cancellable.class);
        };

        CreateToDeliverIngestBridge bridge = new CreateToDeliverIngestBridge(
            outputBus, capturingClient, false, 1000, "BUFFER", 256);
        bridge.onStartup(mock(StartupEvent.class));

        outputBus.publish("unsupported-string");
        outputBus.publish(123);
        outputBus.publish(new Object());

        assertTrue(forwardedOrders.isEmpty());
    }

    @Test
    void bridgeFiltersDuplicatesWhenIdempotencyEnabled() {
        List<OrderDispatchSvc.ReadyOrder> forwardedOrders = new ArrayList<>();
        DeliverOrderIngestClient capturingClient = stream -> {
            stream.subscribe().with(forwardedOrders::add);
            return mock(Cancellable.class);
        };

        CreateToDeliverIngestBridge bridge = new CreateToDeliverIngestBridge(
            outputBus, capturingClient, true, 1000, "BUFFER", 256);
        bridge.onStartup(mock(StartupEvent.class));

        OrderReadySvc.ReadyOrder readyOrder = OrderReadySvc.ReadyOrder.newBuilder()
            .setOrderId("order-1")
            .setCustomerId("customer-1")
            .setReadyAt("2026-03-07T10:00:00Z")
            .build();

        outputBus.publish(readyOrder);
        outputBus.publish(readyOrder);
        outputBus.publish(readyOrder);

        awaitUntil(() -> forwardedOrders.size() == 1, "Expected duplicate filtering to forward exactly one item");
        assertEquals(1, forwardedOrders.size(), "Duplicate orders should be filtered");
    }

    @Test
    void bridgeForwardsDuplicatesWhenIdempotencyDisabled() {
        List<OrderDispatchSvc.ReadyOrder> forwardedOrders = new ArrayList<>();
        DeliverOrderIngestClient capturingClient = stream -> {
            stream.subscribe().with(forwardedOrders::add);
            return mock(Cancellable.class);
        };

        CreateToDeliverIngestBridge bridge = new CreateToDeliverIngestBridge(
            outputBus, capturingClient, false, 1000, "BUFFER", 256);
        bridge.onStartup(mock(StartupEvent.class));

        OrderReadySvc.ReadyOrder readyOrder = OrderReadySvc.ReadyOrder.newBuilder()
            .setOrderId("order-1")
            .setCustomerId("customer-1")
            .setReadyAt("2026-03-07T10:00:00Z")
            .build();

        outputBus.publish(readyOrder);
        outputBus.publish(readyOrder);
        outputBus.publish(readyOrder);

        awaitUntil(() -> forwardedOrders.size() == 3, "Expected all duplicates to be forwarded when idempotency is disabled");
        assertEquals(3, forwardedOrders.size(), "All items should be forwarded when idempotency is disabled");
    }

    @Test
    void bridgeForwardsDifferentOrdersWhenIdempotencyEnabled() {
        List<OrderDispatchSvc.ReadyOrder> forwardedOrders = new ArrayList<>();
        DeliverOrderIngestClient capturingClient = stream -> {
            stream.subscribe().with(forwardedOrders::add);
            return mock(Cancellable.class);
        };

        CreateToDeliverIngestBridge bridge = new CreateToDeliverIngestBridge(
            outputBus, capturingClient, true, 1000, "BUFFER", 256);
        bridge.onStartup(mock(StartupEvent.class));

        outputBus.publish(OrderReadySvc.ReadyOrder.newBuilder()
            .setOrderId("order-1")
            .setCustomerId("customer-1")
            .setReadyAt("2026-03-07T10:00:00Z")
            .build());

        outputBus.publish(OrderReadySvc.ReadyOrder.newBuilder()
            .setOrderId("order-2")
            .setCustomerId("customer-2")
            .setReadyAt("2026-03-07T10:01:00Z")
            .build());

        awaitUntil(() -> forwardedOrders.size() == 2, "Expected two distinct ready orders");
        assertEquals(2, forwardedOrders.size());
    }

    @Test
    void bridgeTreatsSameOrderIdWithDifferentCustomerAsDistinctHandoffs() {
        List<OrderDispatchSvc.ReadyOrder> forwardedOrders = new ArrayList<>();
        DeliverOrderIngestClient capturingClient = stream -> {
            stream.subscribe().with(forwardedOrders::add);
            return mock(Cancellable.class);
        };

        CreateToDeliverIngestBridge bridge = new CreateToDeliverIngestBridge(
            outputBus, capturingClient, true, 1000, "BUFFER", 256);
        bridge.onStartup(mock(StartupEvent.class));

        outputBus.publish(OrderReadySvc.ReadyOrder.newBuilder()
            .setOrderId("order-1")
            .setCustomerId("customer-1")
            .setReadyAt("2026-03-07T10:00:00Z")
            .build());

        outputBus.publish(OrderReadySvc.ReadyOrder.newBuilder()
            .setOrderId("order-1")
            .setCustomerId("customer-2")
            .setReadyAt("2026-03-07T10:00:00Z")
            .build());

        awaitUntil(() -> forwardedOrders.size() == 2, "Expected distinct handoffs for same orderId with different customerId");
        assertEquals(2, forwardedOrders.size());
    }

    @Test
    void bridgeTreatsSameOrderAndCustomerWithDifferentReadyAtAsDistinctHandoffs() {
        List<OrderDispatchSvc.ReadyOrder> forwardedOrders = new ArrayList<>();
        DeliverOrderIngestClient capturingClient = stream -> {
            stream.subscribe().with(forwardedOrders::add);
            return mock(Cancellable.class);
        };

        CreateToDeliverIngestBridge bridge = new CreateToDeliverIngestBridge(
            outputBus, capturingClient, true, 1000, "BUFFER", 256);
        bridge.onStartup(mock(StartupEvent.class));

        outputBus.publish(OrderReadySvc.ReadyOrder.newBuilder()
            .setOrderId("order-1")
            .setCustomerId("customer-1")
            .setReadyAt("2026-03-07T10:00:00Z")
            .build());

        outputBus.publish(OrderReadySvc.ReadyOrder.newBuilder()
            .setOrderId("order-1")
            .setCustomerId("customer-1")
            .setReadyAt("2026-03-07T10:01:00Z")
            .build());

        awaitUntil(() -> forwardedOrders.size() == 2, "Expected distinct handoffs for same order/customer with different readyAt");
        assertEquals(2, forwardedOrders.size());
    }

    @Test
    void bridgeAppliesBackpressureBufferStrategy() {
        AtomicInteger forwardedCount = new AtomicInteger(0);
        DeliverOrderIngestClient countingClient = stream -> {
            stream.subscribe().with(item -> forwardedCount.incrementAndGet());
            return mock(Cancellable.class);
        };

        CreateToDeliverIngestBridge bridge = new CreateToDeliverIngestBridge(
            outputBus, countingClient, false, 1000, "BUFFER", 10);
        bridge.onStartup(mock(StartupEvent.class));

        for (int i = 1; i <= 25; i++) {
            outputBus.publish(OrderReadySvc.ReadyOrder.newBuilder()
                .setOrderId("order-" + i)
                .setCustomerId("customer-" + i)
                .setReadyAt("2026-03-07T10:00:00Z")
                .build());
        }

        awaitUntil(() -> forwardedCount.get() == 25, "Expected forwarded count to reach published size");
        assertEquals(25, forwardedCount.get());
    }

    private Descriptors.FieldDescriptor createMockStringField() {
        Descriptors.FieldDescriptor field = mock(Descriptors.FieldDescriptor.class);
        when(field.isRepeated()).thenReturn(false);
        when(field.isMapField()).thenReturn(false);
        when(field.getJavaType()).thenReturn(Descriptors.FieldDescriptor.JavaType.STRING);
        return field;
    }

}
