package org.pipelineframework.tpfgo.common.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CommonDomainValidationTest {

    @Test
    void placeOrderRequestRejectsBlankItems() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
            () -> new PlaceOrderRequest(
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                "   ",
                BigDecimal.ONE,
                "EUR"));
        assertEquals("items must not be blank", exception.getMessage());
    }

    @Test
    void validatedOrderRequestNormalizesCurrencyCode() {
        ValidatedOrderRequest request = new ValidatedOrderRequest(
            UUID.randomUUID(),
            UUID.randomUUID(),
            UUID.randomUUID(),
            "burger x1",
            BigDecimal.TEN,
            "usd",
            Instant.parse("2026-03-27T10:15:30Z"));
        assertEquals("USD", request.currency());
    }

    @Test
    void paymentCaptureResultRejectsNegativeAmount() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
            () -> new PaymentCaptureResult(
                UUID.randomUUID(),
                UUID.randomUUID(),
                Instant.parse("2026-03-27T10:15:30Z"),
                new BigDecimal("-0.01"),
                "EUR",
                "FAILED",
                null,
                null));
        assertEquals("amount must be non-negative", exception.getMessage());
    }

    @Test
    void deliveryAssignedRejectsNegativeEta() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
            () -> new DeliveryAssigned(
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                BigDecimal.ONE,
                "EUR",
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                -1,
                Instant.parse("2026-03-27T10:15:30Z"),
                "lineage"));
        assertEquals("etaMinutes must be non-negative", exception.getMessage());
    }
}
