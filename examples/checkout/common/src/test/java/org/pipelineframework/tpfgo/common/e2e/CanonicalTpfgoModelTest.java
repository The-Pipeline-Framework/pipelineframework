package org.pipelineframework.tpfgo.common.e2e;

import java.math.BigDecimal;
import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.pipelineframework.tpfgo.common.domain.PaymentCaptureResult;
import org.pipelineframework.tpfgo.common.domain.PlaceOrderRequest;
import org.pipelineframework.tpfgo.common.domain.TerminalOrderState;
import org.pipelineframework.tpfgo.common.util.DeterministicIds;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class CanonicalTpfgoModelTest {

    @Test
    void deterministicIdsRemainStableForCanonicalInputs() {
        String orderSeed = "checkout|request-1|customer-1|restaurant-1";
        String differentSeed = "checkout|request-2|customer-1|restaurant-1";
        assertEquals(
            DeterministicIds.uuid("order", orderSeed),
            DeterministicIds.uuid("order", orderSeed));
        assertNotEquals(
            DeterministicIds.uuid("order", orderSeed),
            DeterministicIds.uuid("order", differentSeed));
    }

    @Test
    void terminalStateCanRepresentHappyAndFailureOutcomes() {
        TerminalOrderState completed = new TerminalOrderState(
            DeterministicIds.uuid("order", "happy"),
            "COMPLETED",
            Instant.parse("2026-03-27T10:15:30Z"),
            "none",
            "CAPTURED",
            DeterministicIds.uuid("payment", "happy"),
            null);
        TerminalOrderState failed = new TerminalOrderState(
            DeterministicIds.uuid("order", "fail"),
            "FAILED_COMPENSATED",
            Instant.parse("2026-03-27T10:15:30Z"),
            "manual-review",
            "FAILED",
            null,
            "PAYMENT_CAPTURE_REJECTED");

        assertEquals("COMPLETED", completed.outcome());
        assertEquals("FAILED_COMPENSATED", failed.outcome());
        assertEquals("manual-review", failed.resolutionAction());
        assertEquals("PAYMENT_CAPTURE_REJECTED", failed.failureCode());
        assertNull(completed.failureCode());
    }

    @Test
    void paymentResultKeepsFailureFieldsOptional() {
        PlaceOrderRequest request = new PlaceOrderRequest(
            DeterministicIds.uuid("request", "r1"),
            DeterministicIds.uuid("customer", "c1"),
            DeterministicIds.uuid("restaurant", "rest1"),
            "burger x1",
            new BigDecimal("42.50"),
            "EUR");
        PaymentCaptureResult captured = new PaymentCaptureResult(
            DeterministicIds.uuid("order", request.requestId().toString()),
            DeterministicIds.uuid("payment", request.requestId().toString()),
            Instant.parse("2026-03-27T11:00:00Z"),
            request.totalAmount(),
            request.currency(),
            "CAPTURED",
            null,
            null);

        assertEquals("CAPTURED", captured.status());
        assertNull(captured.failureCode());
    }
}
