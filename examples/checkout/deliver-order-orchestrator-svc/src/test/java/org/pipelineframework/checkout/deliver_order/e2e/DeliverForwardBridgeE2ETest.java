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
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    void bridgeDropsDuplicateDeliveredCheckpointByOrderId() {
        OrderDeliveredSvc.DeliveredOrder checkpoint = OrderDeliveredSvc.DeliveredOrder.newBuilder()
            .setOrderId("10101010-aaaa-bbbb-cccc-121212121212")
            .setCustomerId("34343434-dddd-eeee-ffff-565656565656")
            .setReadyAt("2026-02-09T20:20:00Z")
            .setDispatchId("78787878-1111-2222-3333-909090909090")
            .setDispatchedAt("2026-02-09T20:21:00Z")
            .setDeliveredAt("2026-02-09T20:22:00Z")
            .build();

        pipelineOutputBus.publish(checkpoint);
        pipelineOutputBus.publish(checkpoint);

        OrderDeliveredSvc.DeliveredOrder firstForwarded = waitForForwarded(Duration.ofSeconds(3));
        OrderDeliveredSvc.DeliveredOrder duplicateForwarded = waitForForwarded(Duration.ofMillis(250));

        assertNotNull(firstForwarded, "Expected first delivered checkpoint to be forwarded");
        assertEquals(checkpoint.getOrderId(), firstForwarded.getOrderId());
        assertNull(duplicateForwarded, "Expected duplicate delivered checkpoint to be dropped by idempotency guard");
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
        assertTrue(waitForFailureCount(initialFailures + 1, Duration.ofSeconds(2)));
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

    private boolean waitForFailureCount(int expected, Duration timeout) {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadlineNanos) {
            if (LocalDeliveredOrderForwardClient.forwardFailures() >= expected) {
                return true;
            }
            try {
                TimeUnit.MILLISECONDS.sleep(25);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return LocalDeliveredOrderForwardClient.forwardFailures() >= expected;
    }
}
