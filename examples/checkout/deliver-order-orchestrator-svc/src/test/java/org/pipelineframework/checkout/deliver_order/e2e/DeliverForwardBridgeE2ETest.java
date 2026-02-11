package org.pipelineframework.checkout.deliver_order.e2e;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.pipelineframework.PipelineOutputBus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.checkout.deliverorder.grpc.OrderDeliveredSvc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@QuarkusTest
class DeliverForwardBridgeE2ETest {

    @Inject
    PipelineOutputBus pipelineOutputBus;

    @BeforeEach
    void resetSinkCapture() {
        LocalDeliveredOrderForwardClient.reset();
    }

    @Test
    void bridgeForwardsDeliveredCheckpointFromOutputBus() {
        OrderDeliveredSvc.DeliveredOrder checkpoint = OrderDeliveredSvc.DeliveredOrder.newBuilder()
            .setOrderId("11111111-1111-1111-1111-111111111111")
            .setCustomerId("22222222-2222-2222-2222-222222222222")
            .setReadyAt("2026-02-09T20:00:00Z")
            .setDispatchId("33333333-3333-3333-3333-333333333333")
            .setDispatchedAt("2026-02-09T20:01:00Z")
            .setDeliveredAt("2026-02-09T20:02:00Z")
            .build();

        pipelineOutputBus.publish(checkpoint);
        OrderDeliveredSvc.DeliveredOrder forwarded = waitForForwarded(Duration.ofSeconds(3));

        assertNotNull(forwarded, "Expected delivered checkpoint forwarded to downstream sink");
        assertFalse(forwarded.getOrderId().isBlank(), "forwarded orderId must not be blank");
        assertEquals(checkpoint.getOrderId(), forwarded.getOrderId());
        assertEquals(checkpoint.getDeliveredAt(), forwarded.getDeliveredAt());
    }

    @Test
    void bridgeDropsDeliveredCheckpointWhenForwardingFails() {
        LocalDeliveredOrderForwardClient.setFailForward(true);

        OrderDeliveredSvc.DeliveredOrder checkpoint = OrderDeliveredSvc.DeliveredOrder.newBuilder()
            .setOrderId("aaaaaaaa-1111-1111-1111-111111111111")
            .setCustomerId("bbbbbbbb-2222-2222-2222-222222222222")
            .setReadyAt("2026-02-09T20:01:00Z")
            .setDispatchId("cccccccc-3333-3333-3333-333333333333")
            .setDispatchedAt("2026-02-09T20:02:00Z")
            .setDeliveredAt("2026-02-09T20:03:00Z")
            .build();

        int initialFailures = LocalDeliveredOrderForwardClient.forwardFailures();
        pipelineOutputBus.publish(checkpoint);

        assertNull(waitForForwarded(Duration.ofSeconds(2)));
        assertEquals(initialFailures + 1, LocalDeliveredOrderForwardClient.forwardFailures());
    }

    private OrderDeliveredSvc.DeliveredOrder waitForForwarded(Duration timeout) {
        try {
            return LocalDeliveredOrderForwardClient.forwardedQueue()
                .poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }
}
