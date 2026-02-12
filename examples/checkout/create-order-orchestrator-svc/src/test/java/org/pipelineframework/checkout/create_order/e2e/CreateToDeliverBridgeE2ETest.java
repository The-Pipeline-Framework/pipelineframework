package org.pipelineframework.checkout.create_order.e2e;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.pipelineframework.PipelineOutputBus;
import org.pipelineframework.checkout.createorder.grpc.OrderReadySvc;
import org.pipelineframework.checkout.deliverorder.grpc.OrderDispatchSvc;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
@Execution(ExecutionMode.SAME_THREAD)
class CreateToDeliverBridgeE2ETest {

    @Inject
    PipelineOutputBus pipelineOutputBus;

    @BeforeEach
    void resetStubCapture() {
        LocalDeliverCaptureIngestClient.reset();
    }

    @Test
    void bridgeForwardsCheckpointToDeliverIngest() {
        OrderReadySvc.ReadyOrder checkpoint = OrderReadySvc.ReadyOrder.newBuilder()
            .setOrderId("11111111-1111-1111-1111-111111111111")
            .setCustomerId("22222222-2222-2222-2222-222222222222")
            .setReadyAt("2026-02-09T20:00:00Z")
            .build();

        pipelineOutputBus.publish(checkpoint);
        OrderDispatchSvc.ReadyOrder forwarded = waitForForwarded(Duration.ofSeconds(3));

        assertNotNull(forwarded, "Expected at least one forwarded item in downstream ingest");

        assertEquals(checkpoint.getOrderId(), forwarded.getOrderId());
        assertEquals(checkpoint.getCustomerId(), forwarded.getCustomerId());
        assertEquals(checkpoint.getReadyAt(), forwarded.getReadyAt());
    }

    @Test
    void bridgeDropsDuplicateCheckpointByOrderId() {
        OrderReadySvc.ReadyOrder checkpoint = OrderReadySvc.ReadyOrder.newBuilder()
            .setOrderId("21212121-1111-2222-3333-444444444444")
            .setCustomerId("56565656-7777-8888-9999-000000000000")
            .setReadyAt("2026-02-09T20:10:00Z")
            .build();

        pipelineOutputBus.publish(checkpoint);
        pipelineOutputBus.publish(checkpoint);

        OrderDispatchSvc.ReadyOrder firstForwarded = waitForForwarded(Duration.ofSeconds(3));
        OrderDispatchSvc.ReadyOrder duplicateForwarded = waitForForwarded(Duration.ofMillis(250));

        assertNotNull(firstForwarded, "Expected first checkpoint to be forwarded");
        assertEquals(checkpoint.getOrderId(), firstForwarded.getOrderId());
        assertEquals(checkpoint.getCustomerId(), firstForwarded.getCustomerId());
        assertEquals(checkpoint.getReadyAt(), firstForwarded.getReadyAt());
        assertNull(duplicateForwarded, "Expected duplicate checkpoint to be dropped by idempotency guard");
    }

    private OrderDispatchSvc.ReadyOrder waitForForwarded(Duration timeout) {
        try {
            return LocalDeliverCaptureIngestClient.forwardedQueue()
                .poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    private boolean waitForIngestFailures(Duration timeout) {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadlineNanos) {
            if (LocalDeliverCaptureIngestClient.ingestFailures() > 0) {
                return true;
            }
            try {
                TimeUnit.MILLISECONDS.sleep(25);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return LocalDeliverCaptureIngestClient.ingestFailures() > 0;
    }

    @Test
    void bridgeDropsCheckpointWhenDeliverIngestFails() {
        LocalDeliverCaptureIngestClient.setFailIngest(true);

        OrderReadySvc.ReadyOrder checkpoint = OrderReadySvc.ReadyOrder.newBuilder()
            .setOrderId("aaaaaaaa-1111-1111-1111-111111111111")
            .setCustomerId("bbbbbbbb-2222-2222-2222-222222222222")
            .setReadyAt("2026-02-09T20:01:00Z")
            .build();

        pipelineOutputBus.publish(checkpoint);

        assertNull(waitForForwarded(Duration.ofSeconds(2)),
            "No forwarded item expected when downstream ingest is forced to fail");
        assertTrue(waitForIngestFailures(Duration.ofSeconds(2)),
            "Expected at least one downstream ingest failure");
    }

    @Test
    void bridgeContinuesAfterDownstreamRecovery() {
        LocalDeliverCaptureIngestClient.setFailIngest(true);

        OrderReadySvc.ReadyOrder firstCheckpoint = OrderReadySvc.ReadyOrder.newBuilder()
            .setOrderId("cccccccc-3333-3333-3333-333333333333")
            .setCustomerId("dddddddd-4444-4444-4444-444444444444")
            .setReadyAt("2026-02-09T20:02:00Z")
            .build();

        pipelineOutputBus.publish(firstCheckpoint);

        assertTrue(waitForIngestFailures(Duration.ofSeconds(2)));

        LocalDeliverCaptureIngestClient.setFailIngest(false);

        OrderReadySvc.ReadyOrder secondCheckpoint = OrderReadySvc.ReadyOrder.newBuilder()
            .setOrderId("eeeeeeee-5555-5555-5555-555555555555")
            .setCustomerId("ffffffff-6666-6666-6666-666666666666")
            .setReadyAt("2026-02-09T20:03:00Z")
            .build();

        pipelineOutputBus.publish(secondCheckpoint);
        OrderDispatchSvc.ReadyOrder forwarded = waitForForwarded(Duration.ofSeconds(3));

        assertNotNull(forwarded, "Expected forwarding to resume after downstream recovery");
        assertEquals(secondCheckpoint.getOrderId(), forwarded.getOrderId());
    }

    @Test
    void bridgeKeepsForwardingRawCheckpointWhenUnknownEnvelopeLikeItemIsPublished() {
        pipelineOutputBus.publish(new EnvelopeLike("trace-1", "not-a-checkpoint"));

        OrderReadySvc.ReadyOrder checkpoint = OrderReadySvc.ReadyOrder.newBuilder()
            .setOrderId("12121212-3434-5656-7878-909090909090")
            .setCustomerId("abababab-cdcd-efef-1212-343434343434")
            .setReadyAt("2026-02-09T20:04:00Z")
            .build();

        pipelineOutputBus.publish(checkpoint);
        OrderDispatchSvc.ReadyOrder forwarded = waitForForwarded(Duration.ofSeconds(3));

        assertNotNull(forwarded, "Raw checkpoint should still be forwarded even with unknown wrapper items");
        assertEquals(checkpoint.getOrderId(), forwarded.getOrderId());
        assertEquals(checkpoint.getCustomerId(), forwarded.getCustomerId());
        assertEquals(checkpoint.getReadyAt(), forwarded.getReadyAt());
    }

    private record EnvelopeLike(String traceId, Object payload) {
    }
}
