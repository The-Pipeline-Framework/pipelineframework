package org.pipelineframework.tpfgo.common.e2e;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.tpfgo.common.domain.PaymentCaptured;
import org.pipelineframework.tpfgo.common.domain.PaymentOutcome;
import org.pipelineframework.tpfgo.common.domain.PlaceOrderRequest;
import org.pipelineframework.tpfgo.common.domain.OrderItem;
import org.pipelineframework.tpfgo.common.domain.TerminalOrderState;
import org.pipelineframework.tpfgo.common.util.DeterministicIds;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    void paymentOutcomeModelsCapturedVariant() {
        PlaceOrderRequest request = new PlaceOrderRequest(
            DeterministicIds.uuid("request", "r1"),
            DeterministicIds.uuid("customer", "c1"),
            DeterministicIds.uuid("restaurant", "rest1"),
            List.of(new OrderItem("burger", 1)),
            new BigDecimal("42.50"),
            "EUR");
        PaymentCaptured captured = new PaymentCaptured(
            DeterministicIds.uuid("order", request.requestId().toString()),
            DeterministicIds.uuid("payment", request.requestId().toString()),
            Instant.parse("2026-03-27T11:00:00Z"),
            request.totalAmount(),
            request.currency());

        assertEquals("CAPTURED", captured.status());
    }

    @Test
    void paymentOutcomeUsesDiscriminatedJsonObject() throws Exception {
        PaymentOutcome captured = new PaymentCaptured(
            DeterministicIds.uuid("order", "json"),
            DeterministicIds.uuid("payment", "json"),
            Instant.parse("2026-03-27T11:00:00Z"),
            new BigDecimal("42.50"),
            "EUR");

        String json = PipelineJson.mapper().writerFor(PaymentOutcome.class).writeValueAsString(captured);
        PaymentOutcome roundTripped = PipelineJson.mapper().readValue(json, PaymentOutcome.class);

        assertTrue(json.contains("\"type\":\"captured\""));
        assertTrue(json.contains("\"paymentId\""));
        assertEquals(captured, roundTripped);
    }

    @Test
    void paymentOutcomeReadsProtobufOneofJsonShape() throws Exception {
        PaymentOutcome expected = new PaymentCaptured(
            DeterministicIds.uuid("order", "proto-json"),
            DeterministicIds.uuid("payment", "proto-json"),
            Instant.parse("2026-03-27T11:00:00Z"),
            new BigDecimal("42.50"),
            "EUR");
        String json = """
            {
              "captured": {
                "orderId": "%s",
                "paymentId": "%s",
                "processedAt": "2026-03-27T11:00:00Z",
                "amount": "42.50",
                "currency": "EUR"
              }
            }
            """.formatted(
            DeterministicIds.uuid("order", "proto-json"),
            DeterministicIds.uuid("payment", "proto-json"));

        PaymentOutcome roundTripped = PipelineJson.mapper().readValue(json, PaymentOutcome.class);

        assertEquals(expected, roundTripped);
    }
}
