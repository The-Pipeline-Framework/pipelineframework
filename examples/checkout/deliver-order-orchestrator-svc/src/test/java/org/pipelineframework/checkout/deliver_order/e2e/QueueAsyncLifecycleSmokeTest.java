package org.pipelineframework.checkout.deliver_order.e2e;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.UUID;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import jakarta.ws.rs.BadRequestException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.pipelineframework.checkout.deliverorder.grpc.Orchestrator;
import org.pipelineframework.checkout.deliverorder.grpc.OrderDeliveredSvc;
import org.pipelineframework.checkout.deliverorder.grpc.OrderDispatchSvc;
import org.pipelineframework.checkout.deliverorder.orchestrator.service.OrchestratorGrpcService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@QuarkusTest
@TestProfile(QueueAsyncLifecycleTestProfile.class)
@Execution(ExecutionMode.SAME_THREAD)
class QueueAsyncLifecycleSmokeTest {

    private static final String TENANT_ID = "checkout-queue-async";
    private static final Duration ASYNC_TIMEOUT = Duration.ofSeconds(10);

    @Inject
    @io.quarkus.grpc.GrpcService
    OrchestratorGrpcService orchestratorGrpcService;

    @Test
    void submitStatusResultLifecycleCompletesSuccessfully() {
        String policy = idempotencyPolicy();
        assumeTrue(!"SERVER_KEY_ONLY".equals(policy));
        OrderDispatchSvc.ReadyOrder input = readyOrder("lifecycle");
        String idempotencyKey = "CLIENT_KEY_REQUIRED".equals(policy) ? "idem-lifecycle" : null;

        Orchestrator.RunAsyncResponse accepted = submit(input, idempotencyKey);
        assertNotNull(accepted.getExecutionId());
        assertFalse(accepted.getDuplicate());

        Orchestrator.GetExecutionStatusResponse terminal = awaitTerminal(accepted.getExecutionId());
        assertEquals("SUCCEEDED", terminal.getStatus());

        Orchestrator.GetExecutionResultResponse resultResponse = orchestratorGrpcService
            .getExecutionResult(Orchestrator.GetExecutionResultRequest.newBuilder()
                .setTenantId(TENANT_ID)
                .setExecutionId(accepted.getExecutionId())
                .build())
            .await().atMost(ASYNC_TIMEOUT);
        assertEquals(1, resultResponse.getItemsCount());
        OrderDeliveredSvc.DeliveredOrder result = resultResponse.getItems(0);

        assertNotNull(result);
        assertEquals(input.getOrderId(), result.getOrderId());
        assertEquals(input.getCustomerId(), result.getCustomerId());
        assertEquals(input.getReadyAt(), result.getReadyAt());
        assertNotNull(result.getDispatchId());
        assertNotNull(result.getDispatchedAt());
        assertNotNull(result.getDeliveredAt());
    }

    @Test
    void duplicateSubmissionsResolveDeterministically() {
        String policy = idempotencyPolicy();
        assumeTrue(!"SERVER_KEY_ONLY".equals(policy));
        OrderDispatchSvc.ReadyOrder input = readyOrder("duplicate");
        String idempotencyKey = "CLIENT_KEY_REQUIRED".equals(policy) ? "idem-duplicate" : null;

        Orchestrator.RunAsyncResponse first = submit(input, idempotencyKey);
        Orchestrator.RunAsyncResponse second = submit(input, idempotencyKey);

        assertFalse(first.getDuplicate());
        assertTrue(second.getDuplicate());
        assertEquals(first.getExecutionId(), second.getExecutionId());

        Orchestrator.GetExecutionStatusResponse terminal = awaitTerminal(first.getExecutionId());
        assertEquals("SUCCEEDED", terminal.getStatus());
    }

    @Test
    void clientKeyRequiredPolicyRejectsMissingIdempotencyKey() {
        String policy = idempotencyPolicy();
        assumeTrue("CLIENT_KEY_REQUIRED".equals(policy));
        OrderDispatchSvc.ReadyOrder input = readyOrder("missing-key");

        BadRequestException error = assertThrows(BadRequestException.class, () -> submit(input, null));
        assertTrue(error.getMessage().contains("Idempotency-Key header is required"));
    }

    @Test
    void serverKeyOnlyPolicyFailsFastForUnsupportedGrpcPayloadShape() {
        String policy = idempotencyPolicy();
        assumeTrue("SERVER_KEY_ONLY".equals(policy));
        OrderDispatchSvc.ReadyOrder input = readyOrder("server-key");

        IllegalStateException error = assertThrows(IllegalStateException.class, () -> submit(input, null));
        assertTrue(error.getMessage().contains("Failed to derive deterministic execution key"));
    }

    private Orchestrator.RunAsyncResponse submit(OrderDispatchSvc.ReadyOrder input, String idempotencyKey) {
        Orchestrator.RunAsyncRequest.Builder request = Orchestrator.RunAsyncRequest.newBuilder()
            .setInput(input)
            .setTenantId(TENANT_ID);
        if (idempotencyKey != null && !idempotencyKey.isBlank()) {
            request.setIdempotencyKey(idempotencyKey);
        }
        return orchestratorGrpcService
            .runAsync(request.build())
            .await().atMost(ASYNC_TIMEOUT);
    }

    private Orchestrator.GetExecutionStatusResponse awaitTerminal(String executionId) {
        long deadline = System.nanoTime() + ASYNC_TIMEOUT.toNanos();
        Orchestrator.GetExecutionStatusResponse status = null;
        while (System.nanoTime() < deadline) {
            status = orchestratorGrpcService
                .getExecutionStatus(Orchestrator.GetExecutionStatusRequest.newBuilder()
                    .setTenantId(TENANT_ID)
                    .setExecutionId(executionId)
                    .build())
                .await().atMost(Duration.ofSeconds(2));
            if (terminalStatus(status.getStatus())) {
                return status;
            }
            sleep(25);
        }
        throw new AssertionError("Execution did not reach terminal status in time: "
            + executionId + " lastStatus=" + (status == null ? "null" : status.getStatus()));
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while waiting for async execution", e);
        }
    }

    private static String idempotencyPolicy() {
        return System.getProperty("pipeline.orchestrator.idempotency-policy", "CLIENT_KEY_REQUIRED")
            .trim()
            .toUpperCase(Locale.ROOT);
    }

    private static OrderDispatchSvc.ReadyOrder readyOrder(String seed) {
        UUID orderId = deterministicUuid("order", seed);
        UUID customerId = deterministicUuid("customer", seed);
        long offsetSeconds = Integer.toUnsignedLong(seed.hashCode()) % 120L;
        Instant readyAt = Instant.parse("2026-03-10T12:00:00Z").plusSeconds(offsetSeconds);
        return OrderDispatchSvc.ReadyOrder.newBuilder()
            .setOrderId(orderId.toString())
            .setCustomerId(customerId.toString())
            .setReadyAt(readyAt.toString())
            .build();
    }

    private static UUID deterministicUuid(String namespace, String value) {
        String seed = namespace + ":" + value;
        return UUID.nameUUIDFromBytes(seed.getBytes(StandardCharsets.UTF_8));
    }

    private static boolean terminalStatus(String status) {
        return "SUCCEEDED".equals(status) || "FAILED".equals(status);
    }
}
