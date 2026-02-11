package org.pipelineframework.checkout.create_order.e2e;

import java.time.Duration;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.PipelineOutputBus;
import org.pipelineframework.checkout.createorder.grpc.OrderReadySvc;
import org.pipelineframework.checkout.deliverorder.grpc.OrderDispatchSvc;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@QuarkusTest
@TestProfile(RealGrpcBridgeTestProfile.class)
class CreateToDeliverGrpcBridgeE2ETest {

    @Inject
    PipelineOutputBus pipelineOutputBus;

    @BeforeEach
    void resetCapture() {
        EmbeddedDeliverOrchestratorGrpcService.reset();
    }

    @Test
    void bridgeForwardsCheckpointOverGrpcIngest() {
        OrderReadySvc.ReadyOrder checkpoint = OrderReadySvc.ReadyOrder.newBuilder()
            .setOrderId("99999999-1111-2222-3333-444444444444")
            .setCustomerId("88888888-1111-2222-3333-444444444444")
            .setReadyAt("2026-02-10T12:00:00Z")
            .build();

        pipelineOutputBus.publish(checkpoint);
        OrderDispatchSvc.ReadyOrder captured = waitForCaptured(Duration.ofSeconds(15));

        assertAll(
            () -> assertNotNull(captured, "Expected deliver ingest endpoint to capture forwarded checkpoint"),
            () -> assertEquals(checkpoint.getOrderId(), captured == null ? null : captured.getOrderId()),
            () -> assertEquals(checkpoint.getCustomerId(), captured == null ? null : captured.getCustomerId()),
            () -> assertEquals(checkpoint.getReadyAt(), captured == null ? null : captured.getReadyAt())
        );
    }

    private OrderDispatchSvc.ReadyOrder waitForCaptured(Duration timeout) {
        return EmbeddedDeliverOrchestratorGrpcService.pollCapturedIngest(timeout.toMillis());
    }
}
