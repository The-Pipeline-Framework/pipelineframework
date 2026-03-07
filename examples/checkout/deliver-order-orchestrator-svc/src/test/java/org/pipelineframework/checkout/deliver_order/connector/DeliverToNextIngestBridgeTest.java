package org.pipelineframework.checkout.deliver_order.connector;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.Cancellable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.PipelineOutputBus;
import org.pipelineframework.checkout.deliverorder.grpc.OrderDeliveredSvc;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DeliverToNextIngestBridgeTest {

    private PipelineOutputBus outputBus;
    private DeliveredOrderForwardClient forwardClient;
    private Cancellable mockCancellable;

    @BeforeEach
    void setUp() {
        outputBus = new PipelineOutputBus();
        forwardClient = mock(DeliveredOrderForwardClient.class);
        mockCancellable = mock(Cancellable.class);
        when(forwardClient.forward(any(), any(), any())).thenReturn(mockCancellable);
    }

    @Test
    void constructorRejectsNullOutputBus() {
        assertThrows(NullPointerException.class, () ->
            new DeliverToNextIngestBridge(null, forwardClient, true, true, 1000, "BUFFER", 256));
    }

    @Test
    void constructorRejectsNullForwardClient() {
        assertThrows(NullPointerException.class, () ->
            new DeliverToNextIngestBridge(outputBus, null, true, true, 1000, "BUFFER", 256));
    }

    @Test
    void constructorNormalizesInvalidIdempotencyMaxKeys() {
        DeliverToNextIngestBridge bridge = new DeliverToNextIngestBridge(
            outputBus, forwardClient, true, true, -1, "BUFFER", 256);
        assertEquals(10000, bridge.getIdempotencyMaxKeys());
    }

    @Test
    void constructorNormalizesInvalidBackpressureStrategy() {
        DeliverToNextIngestBridge bridge = new DeliverToNextIngestBridge(
            outputBus, forwardClient, true, true, 1000, "INVALID", 256);
        assertEquals("BUFFER", bridge.getBackpressureStrategy());
    }

    @Test
    void constructorNormalizesInvalidBackpressureBufferCapacity() {
        DeliverToNextIngestBridge bridge = new DeliverToNextIngestBridge(
            outputBus, forwardClient, true, true, 1000, "BUFFER", -1);
        assertEquals(256, bridge.getBackpressureBufferCapacity());
    }

    @Test
    void onStartupDoesNotStartForwardingWhenDisabled() {
        DeliverToNextIngestBridge bridge = new DeliverToNextIngestBridge(
            outputBus, forwardClient, false, true, 1000, "BUFFER", 256);

        bridge.onStartup(mock(StartupEvent.class));

        verify(forwardClient, never()).forward(any(), any(), any());
    }

    @Test
    void onStartupStartsForwardingWhenEnabled() {
        DeliverToNextIngestBridge bridge = new DeliverToNextIngestBridge(
            outputBus, forwardClient, true, true, 1000, "BUFFER", 256);

        bridge.onStartup(mock(StartupEvent.class));

        verify(forwardClient, times(1)).forward(any(), any(), any());
    }

    @Test
    void onShutdownCancelsForwardingSubscriptionWhenActive() {
        DeliverToNextIngestBridge bridge = new DeliverToNextIngestBridge(
            outputBus, forwardClient, true, true, 1000, "BUFFER", 256);

        bridge.onStartup(mock(StartupEvent.class));
        bridge.onShutdown();

        verify(mockCancellable, times(1)).cancel();
    }

    @Test
    void onShutdownHandlesMissingSubscription() {
        DeliverToNextIngestBridge bridge = new DeliverToNextIngestBridge(
            outputBus, forwardClient, true, true, 1000, "BUFFER", 256);

        bridge.onShutdown();

        verify(mockCancellable, never()).cancel();
    }

    @Test
    void bridgeForwardsDeliveredOrderToForwardClient() {
        List<OrderDeliveredSvc.DeliveredOrder> forwardedOrders = new ArrayList<>();
        DeliveredOrderForwardClient capturingClient = stream -> {
            stream.subscribe().with(forwardedOrders::add);
            return mock(Cancellable.class);
        };

        DeliverToNextIngestBridge bridge = new DeliverToNextIngestBridge(
            outputBus, capturingClient, true, false, 1000, "BUFFER", 256);
        bridge.onStartup(mock(StartupEvent.class));

        OrderDeliveredSvc.DeliveredOrder deliveredOrder = OrderDeliveredSvc.DeliveredOrder.newBuilder()
            .setOrderId("order-1")
            .setCustomerId("customer-1")
            .setReadyAt("2026-03-07T10:00:00Z")
            .setDispatchId("dispatch-1")
            .setDispatchedAt("2026-03-07T10:30:00Z")
            .setDeliveredAt("2026-03-07T11:00:00Z")
            .build();

        outputBus.publish(deliveredOrder);

        awaitUntil(() -> forwardedOrders.size() == 1, "Expected one forwarded delivered order");
        assertEquals(1, forwardedOrders.size());
        assertEquals("order-1", forwardedOrders.get(0).getOrderId());
        assertEquals("dispatch-1", forwardedOrders.get(0).getDispatchId());
    }

    @Test
    void bridgeForwardsMultipleDeliveredOrders() {
        List<OrderDeliveredSvc.DeliveredOrder> forwardedOrders = new ArrayList<>();
        DeliveredOrderForwardClient capturingClient = stream -> {
            stream.subscribe().with(forwardedOrders::add);
            return mock(Cancellable.class);
        };

        DeliverToNextIngestBridge bridge = new DeliverToNextIngestBridge(
            outputBus, capturingClient, true, false, 1000, "BUFFER", 256);
        bridge.onStartup(mock(StartupEvent.class));

        for (int i = 1; i <= 5; i++) {
            OrderDeliveredSvc.DeliveredOrder deliveredOrder = OrderDeliveredSvc.DeliveredOrder.newBuilder()
                .setOrderId("order-" + i)
                .setCustomerId("customer-" + i)
                .setReadyAt("2026-03-07T10:00:00Z")
                .setDispatchId("dispatch-" + i)
                .setDispatchedAt("2026-03-07T10:30:00Z")
                .setDeliveredAt("2026-03-07T11:00:00Z")
                .build();
            outputBus.publish(deliveredOrder);
        }

        awaitUntil(() -> forwardedOrders.size() == 5, "Expected five forwarded delivered orders");
        assertEquals(5, forwardedOrders.size());
    }

    @Test
    void bridgeDropsDeliveredOrderWithMissingOrderId() {
        List<OrderDeliveredSvc.DeliveredOrder> forwardedOrders = new ArrayList<>();
        DeliveredOrderForwardClient capturingClient = stream -> {
            stream.subscribe().with(forwardedOrders::add);
            return mock(Cancellable.class);
        };

        DeliverToNextIngestBridge bridge = new DeliverToNextIngestBridge(
            outputBus, capturingClient, true, false, 1000, "BUFFER", 256);
        bridge.onStartup(mock(StartupEvent.class));

        OrderDeliveredSvc.DeliveredOrder deliveredOrder = OrderDeliveredSvc.DeliveredOrder.newBuilder()
            .setOrderId("")
            .setCustomerId("customer-1")
            .setReadyAt("2026-03-07T10:00:00Z")
            .setDispatchId("dispatch-1")
            .setDispatchedAt("2026-03-07T10:30:00Z")
            .setDeliveredAt("2026-03-07T11:00:00Z")
            .build();
        outputBus.publish(deliveredOrder);

        assertTrue(forwardedOrders.isEmpty());
    }

    @Test
    void bridgeDropsDeliveredOrderWithMissingCustomerId() {
        List<OrderDeliveredSvc.DeliveredOrder> forwardedOrders = new ArrayList<>();
        DeliveredOrderForwardClient capturingClient = stream -> {
            stream.subscribe().with(forwardedOrders::add);
            return mock(Cancellable.class);
        };

        DeliverToNextIngestBridge bridge = new DeliverToNextIngestBridge(
            outputBus, capturingClient, true, false, 1000, "BUFFER", 256);
        bridge.onStartup(mock(StartupEvent.class));

        OrderDeliveredSvc.DeliveredOrder deliveredOrder = OrderDeliveredSvc.DeliveredOrder.newBuilder()
            .setOrderId("order-1")
            .setCustomerId("")
            .setReadyAt("2026-03-07T10:00:00Z")
            .setDispatchId("dispatch-1")
            .setDispatchedAt("2026-03-07T10:30:00Z")
            .setDeliveredAt("2026-03-07T11:00:00Z")
            .build();
        outputBus.publish(deliveredOrder);

        assertTrue(forwardedOrders.isEmpty());
    }

    @Test
    void bridgeDropsDeliveredOrderWithMissingDispatchId() {
        List<OrderDeliveredSvc.DeliveredOrder> forwardedOrders = new ArrayList<>();
        DeliveredOrderForwardClient capturingClient = stream -> {
            stream.subscribe().with(forwardedOrders::add);
            return mock(Cancellable.class);
        };

        DeliverToNextIngestBridge bridge = new DeliverToNextIngestBridge(
            outputBus, capturingClient, true, false, 1000, "BUFFER", 256);
        bridge.onStartup(mock(StartupEvent.class));

        OrderDeliveredSvc.DeliveredOrder deliveredOrder = OrderDeliveredSvc.DeliveredOrder.newBuilder()
            .setOrderId("order-1")
            .setCustomerId("customer-1")
            .setReadyAt("2026-03-07T10:00:00Z")
            .setDispatchId("")
            .setDispatchedAt("2026-03-07T10:30:00Z")
            .setDeliveredAt("2026-03-07T11:00:00Z")
            .build();
        outputBus.publish(deliveredOrder);

        assertTrue(forwardedOrders.isEmpty());
    }

    @Test
    void bridgeMapsGenericProtobufMessageWithCorrectFields() {
        List<OrderDeliveredSvc.DeliveredOrder> forwardedOrders = new ArrayList<>();
        DeliveredOrderForwardClient capturingClient = stream -> {
            stream.subscribe().with(forwardedOrders::add);
            return mock(Cancellable.class);
        };

        DeliverToNextIngestBridge bridge = new DeliverToNextIngestBridge(
            outputBus, capturingClient, true, false, 1000, "BUFFER", 256);
        bridge.onStartup(mock(StartupEvent.class));

        Message mockMessage = createMockDeliveredMessage(
            "order-1", "customer-1", "2026-03-07T10:00:00Z",
            "dispatch-1", "2026-03-07T10:30:00Z", "2026-03-07T11:00:00Z");

        outputBus.publish(mockMessage);

        awaitUntil(() -> forwardedOrders.size() == 1, "Expected mapped protobuf message to be forwarded");
        assertEquals(1, forwardedOrders.size());
        assertEquals("order-1", forwardedOrders.get(0).getOrderId());
        assertEquals("dispatch-1", forwardedOrders.get(0).getDispatchId());
    }

    @Test
    void bridgeDropsGenericProtobufMessageWithMissingFields() {
        List<OrderDeliveredSvc.DeliveredOrder> forwardedOrders = new ArrayList<>();
        DeliveredOrderForwardClient capturingClient = stream -> {
            stream.subscribe().with(forwardedOrders::add);
            return mock(Cancellable.class);
        };

        DeliverToNextIngestBridge bridge = new DeliverToNextIngestBridge(
            outputBus, capturingClient, true, false, 1000, "BUFFER", 256);
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
        List<OrderDeliveredSvc.DeliveredOrder> forwardedOrders = new ArrayList<>();
        DeliveredOrderForwardClient capturingClient = stream -> {
            stream.subscribe().with(forwardedOrders::add);
            return mock(Cancellable.class);
        };

        DeliverToNextIngestBridge bridge = new DeliverToNextIngestBridge(
            outputBus, capturingClient, true, false, 1000, "BUFFER", 256);
        bridge.onStartup(mock(StartupEvent.class));

        outputBus.publish("unsupported-string");
        outputBus.publish(123);
        outputBus.publish(new Object());

        assertTrue(forwardedOrders.isEmpty());
    }

    @Test
    void bridgeFiltersDuplicatesWhenIdempotencyEnabled() {
        List<OrderDeliveredSvc.DeliveredOrder> forwardedOrders = new ArrayList<>();
        DeliveredOrderForwardClient capturingClient = stream -> {
            stream.subscribe().with(forwardedOrders::add);
            return mock(Cancellable.class);
        };

        DeliverToNextIngestBridge bridge = new DeliverToNextIngestBridge(
            outputBus, capturingClient, true, true, 1000, "BUFFER", 256);
        bridge.onStartup(mock(StartupEvent.class));

        OrderDeliveredSvc.DeliveredOrder deliveredOrder = OrderDeliveredSvc.DeliveredOrder.newBuilder()
            .setOrderId("order-1")
            .setCustomerId("customer-1")
            .setReadyAt("2026-03-07T10:00:00Z")
            .setDispatchId("dispatch-1")
            .setDispatchedAt("2026-03-07T10:30:00Z")
            .setDeliveredAt("2026-03-07T11:00:00Z")
            .build();

        outputBus.publish(deliveredOrder);
        outputBus.publish(deliveredOrder);
        outputBus.publish(deliveredOrder);

        awaitUntil(() -> forwardedOrders.size() == 1, "Expected duplicate filtering to forward exactly one item");
        assertEquals(1, forwardedOrders.size(), "Duplicate orders should be filtered");
    }

    @Test
    void bridgeForwardsDuplicatesWhenIdempotencyDisabled() {
        List<OrderDeliveredSvc.DeliveredOrder> forwardedOrders = new ArrayList<>();
        DeliveredOrderForwardClient capturingClient = stream -> {
            stream.subscribe().with(forwardedOrders::add);
            return mock(Cancellable.class);
        };

        DeliverToNextIngestBridge bridge = new DeliverToNextIngestBridge(
            outputBus, capturingClient, true, false, 1000, "BUFFER", 256);
        bridge.onStartup(mock(StartupEvent.class));

        OrderDeliveredSvc.DeliveredOrder deliveredOrder = OrderDeliveredSvc.DeliveredOrder.newBuilder()
            .setOrderId("order-1")
            .setCustomerId("customer-1")
            .setReadyAt("2026-03-07T10:00:00Z")
            .setDispatchId("dispatch-1")
            .setDispatchedAt("2026-03-07T10:30:00Z")
            .setDeliveredAt("2026-03-07T11:00:00Z")
            .build();

        outputBus.publish(deliveredOrder);
        outputBus.publish(deliveredOrder);
        outputBus.publish(deliveredOrder);

        awaitUntil(() -> forwardedOrders.size() == 3, "Expected all duplicates to be forwarded when idempotency is disabled");
        assertEquals(3, forwardedOrders.size(), "All items should be forwarded when idempotency is disabled");
    }

    @Test
    void bridgeForwardsDifferentOrdersWhenIdempotencyEnabled() {
        List<OrderDeliveredSvc.DeliveredOrder> forwardedOrders = new ArrayList<>();
        DeliveredOrderForwardClient capturingClient = stream -> {
            stream.subscribe().with(forwardedOrders::add);
            return mock(Cancellable.class);
        };

        DeliverToNextIngestBridge bridge = new DeliverToNextIngestBridge(
            outputBus, capturingClient, true, true, 1000, "BUFFER", 256);
        bridge.onStartup(mock(StartupEvent.class));

        outputBus.publish(OrderDeliveredSvc.DeliveredOrder.newBuilder()
            .setOrderId("order-1")
            .setCustomerId("customer-1")
            .setReadyAt("2026-03-07T10:00:00Z")
            .setDispatchId("dispatch-1")
            .setDispatchedAt("2026-03-07T10:30:00Z")
            .setDeliveredAt("2026-03-07T11:00:00Z")
            .build());

        outputBus.publish(OrderDeliveredSvc.DeliveredOrder.newBuilder()
            .setOrderId("order-2")
            .setCustomerId("customer-2")
            .setReadyAt("2026-03-07T10:00:00Z")
            .setDispatchId("dispatch-2")
            .setDispatchedAt("2026-03-07T10:35:00Z")
            .setDeliveredAt("2026-03-07T11:05:00Z")
            .build());

        awaitUntil(() -> forwardedOrders.size() == 2, "Expected two distinct delivered orders");
        assertEquals(2, forwardedOrders.size());
    }

    @Test
    void bridgeAppliesBackpressureBufferStrategy() {
        AtomicInteger forwardedCount = new AtomicInteger(0);
        DeliveredOrderForwardClient countingClient = stream -> {
            stream.subscribe().with(item -> forwardedCount.incrementAndGet());
            return mock(Cancellable.class);
        };

        DeliverToNextIngestBridge bridge = new DeliverToNextIngestBridge(
            outputBus, countingClient, true, false, 1000, "BUFFER", 10);
        bridge.onStartup(mock(StartupEvent.class));

        for (int i = 1; i <= 5; i++) {
            outputBus.publish(OrderDeliveredSvc.DeliveredOrder.newBuilder()
                .setOrderId("order-" + i)
                .setCustomerId("customer-" + i)
                .setReadyAt("2026-03-07T10:00:00Z")
                .setDispatchId("dispatch-" + i)
                .setDispatchedAt("2026-03-07T10:30:00Z")
                .setDeliveredAt("2026-03-07T11:00:00Z")
                .build());
        }

        awaitUntil(() -> forwardedCount.get() == 5, "Expected forwarded count to reach five");
        assertEquals(5, forwardedCount.get());
    }

    @Test
    void bridgeClearsInFlightReservationsOnFailure() {
        AtomicInteger attempts = new AtomicInteger(0);
        AtomicInteger acked = new AtomicInteger(0);
        DeliveredOrderForwardClient failingClient = new DeliveredOrderForwardClient() {
            @Override
            public Cancellable forward(Multi<OrderDeliveredSvc.DeliveredOrder> deliveredOrderStream) {
                return forward(deliveredOrderStream, item -> {
                }, failure -> {
                });
            }

            @Override
            public Cancellable forward(
                Multi<OrderDeliveredSvc.DeliveredOrder> stream,
                java.util.function.Consumer<OrderDeliveredSvc.DeliveredOrder> onForwarded,
                java.util.function.Consumer<Throwable> onForwardFailure
            ) {
                return stream.subscribe().with(item -> {
                    if (attempts.getAndIncrement() == 0) {
                        onForwardFailure.accept(new IllegalStateException("forced-forward-failure"));
                        return;
                    }
                    acked.incrementAndGet();
                    onForwarded.accept(item);
                }, onForwardFailure::accept);
            }
        };

        DeliverToNextIngestBridge bridge = new DeliverToNextIngestBridge(
            outputBus, failingClient, true, true, 1000, "BUFFER", 256);
        bridge.onStartup(mock(StartupEvent.class));

        OrderDeliveredSvc.DeliveredOrder deliveredOrder = OrderDeliveredSvc.DeliveredOrder.newBuilder()
            .setOrderId("order-1")
            .setCustomerId("customer-1")
            .setReadyAt("2026-03-07T10:00:00Z")
            .setDispatchId("dispatch-1")
            .setDispatchedAt("2026-03-07T10:30:00Z")
            .setDeliveredAt("2026-03-07T11:00:00Z")
            .build();

        outputBus.publish(deliveredOrder);
        outputBus.publish(deliveredOrder);

        awaitUntil(() -> attempts.get() == 2, "Expected two processing attempts");
        awaitUntil(() -> acked.get() == 1, "Expected second publish to be acknowledged");
        assertEquals(2, attempts.get());
        assertEquals(1, acked.get(), "Second publish should be accepted after in-flight reservations are cleared");
    }

    private Message createMockDeliveredMessage(String orderId, String customerId, String readyAt,
                                                String dispatchId, String dispatchedAt, String deliveredAt) {
        Message mockMessage = mock(Message.class);
        Descriptors.Descriptor descriptor = mock(Descriptors.Descriptor.class);

        Descriptors.FieldDescriptor orderIdField = createMockStringField();
        Descriptors.FieldDescriptor customerIdField = createMockStringField();
        Descriptors.FieldDescriptor readyAtField = createMockStringField();
        Descriptors.FieldDescriptor dispatchIdField = createMockStringField();
        Descriptors.FieldDescriptor dispatchedAtField = createMockStringField();
        Descriptors.FieldDescriptor deliveredAtField = createMockStringField();

        when(mockMessage.getDescriptorForType()).thenReturn(descriptor);
        when(descriptor.findFieldByName("order_id")).thenReturn(orderIdField);
        when(descriptor.findFieldByName("customer_id")).thenReturn(customerIdField);
        when(descriptor.findFieldByName("ready_at")).thenReturn(readyAtField);
        when(descriptor.findFieldByName("dispatch_id")).thenReturn(dispatchIdField);
        when(descriptor.findFieldByName("dispatched_at")).thenReturn(dispatchedAtField);
        when(descriptor.findFieldByName("delivered_at")).thenReturn(deliveredAtField);

        when(mockMessage.getField(orderIdField)).thenReturn(orderId);
        when(mockMessage.getField(customerIdField)).thenReturn(customerId);
        when(mockMessage.getField(readyAtField)).thenReturn(readyAt);
        when(mockMessage.getField(dispatchIdField)).thenReturn(dispatchId);
        when(mockMessage.getField(dispatchedAtField)).thenReturn(dispatchedAt);
        when(mockMessage.getField(deliveredAtField)).thenReturn(deliveredAt);

        return mockMessage;
    }

    private Descriptors.FieldDescriptor createMockStringField() {
        Descriptors.FieldDescriptor field = mock(Descriptors.FieldDescriptor.class);
        when(field.isRepeated()).thenReturn(false);
        when(field.isMapField()).thenReturn(false);
        when(field.getJavaType()).thenReturn(Descriptors.FieldDescriptor.JavaType.STRING);
        return field;
    }

    private static void awaitUntil(BooleanSupplier condition, String message) {
        for (int i = 0; i < 50; i++) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(10L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        assertTrue(condition.getAsBoolean(), message);
    }
}
