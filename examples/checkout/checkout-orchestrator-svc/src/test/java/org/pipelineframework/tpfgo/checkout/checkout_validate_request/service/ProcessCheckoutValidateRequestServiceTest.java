package org.pipelineframework.tpfgo.checkout.checkout_validate_request.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.pipelineframework.tpfgo.common.domain.PlaceOrderRequest;
import org.pipelineframework.tpfgo.common.domain.ValidatedOrderRequest;

class ProcessCheckoutValidateRequestServiceTest {

    private static final Instant FIXED_NOW = Instant.parse("2026-01-15T10:00:00Z");
    private static final Clock FIXED_CLOCK = Clock.fixed(FIXED_NOW, ZoneOffset.UTC);

    private final ProcessCheckoutValidateRequestService service =
        new ProcessCheckoutValidateRequestService(FIXED_CLOCK);

    @Test
    void processNullInputReturnsFailure() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
            () -> service.process(null).await().indefinitely());
        assertEquals("input must not be null", exception.getMessage());
    }

    @Test
    void processNegativeTotalAmountReturnsFailure() {
        PlaceOrderRequest request = validRequest(new BigDecimal("-0.01"));
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
            () -> service.process(request).await().indefinitely());
        assertEquals("totalAmount must be >= 0", exception.getMessage());
    }

    @Test
    void processZeroTotalAmountSucceeds() {
        PlaceOrderRequest request = validRequest(BigDecimal.ZERO);
        ValidatedOrderRequest result = service.process(request).await().indefinitely();
        assertNotNull(result);
        assertEquals(BigDecimal.ZERO, result.totalAmount());
    }

    @Test
    void processValidRequestProducesValidatedOrderWithAllFields() {
        UUID requestId = UUID.randomUUID();
        UUID customerId = UUID.randomUUID();
        UUID restaurantId = UUID.randomUUID();
        PlaceOrderRequest request = new PlaceOrderRequest(
            requestId, customerId, restaurantId, "burger x1", new BigDecimal("12.50"), "USD");

        ValidatedOrderRequest validated = service.process(request).await().indefinitely();

        assertNotNull(validated);
        assertEquals(requestId, validated.requestId());
        assertEquals(customerId, validated.customerId());
        assertEquals(restaurantId, validated.restaurantId());
        assertEquals("burger x1", validated.items());
        assertEquals(new BigDecimal("12.50"), validated.totalAmount());
        assertEquals("USD", validated.currency());
        assertEquals(FIXED_NOW, validated.validatedAt());
    }

    @Test
    void processUsesClockForValidatedAt() {
        Instant specificInstant = Instant.parse("2026-03-01T08:30:00Z");
        Clock specificClock = Clock.fixed(specificInstant, ZoneOffset.UTC);
        ProcessCheckoutValidateRequestService svc = new ProcessCheckoutValidateRequestService(specificClock);

        PlaceOrderRequest request = validRequest(new BigDecimal("5.00"));
        ValidatedOrderRequest result = svc.process(request).await().indefinitely();

        assertEquals(specificInstant, result.validatedAt());
    }

    @Test
    void processLargePositiveTotalAmountSucceeds() {
        PlaceOrderRequest request = validRequest(new BigDecimal("999999999.99"));
        ValidatedOrderRequest result = service.process(request).await().indefinitely();
        assertNotNull(result);
        assertEquals(new BigDecimal("999999999.99"), result.totalAmount());
    }

    @Test
    void processPreservesItemsStringExactly() {
        PlaceOrderRequest request = new PlaceOrderRequest(
            UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
            "  item with spaces  ", new BigDecimal("1.00"), "EUR");
        ValidatedOrderRequest result = service.process(request).await().indefinitely();
        assertEquals("  item with spaces  ", result.items());
    }

    @Test
    void processPreservesCurrencyCode() {
        PlaceOrderRequest request = new PlaceOrderRequest(
            UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
            "item", new BigDecimal("10.00"), "GBP");
        ValidatedOrderRequest result = service.process(request).await().indefinitely();
        assertEquals("GBP", result.currency());
    }

    // Regression: exactly -1 totalAmount (not just -0.01) is rejected
    @Test
    void processNegativeOneTotalAmountReturnsFailure() {
        PlaceOrderRequest request = validRequest(new BigDecimal("-1"));
        assertThrows(IllegalArgumentException.class,
            () -> service.process(request).await().indefinitely());
    }

    private static PlaceOrderRequest validRequest(BigDecimal totalAmount) {
        return new PlaceOrderRequest(
            UUID.randomUUID(),
            UUID.randomUUID(),
            UUID.randomUUID(),
            "test item",
            totalAmount,
            "USD");
    }
}