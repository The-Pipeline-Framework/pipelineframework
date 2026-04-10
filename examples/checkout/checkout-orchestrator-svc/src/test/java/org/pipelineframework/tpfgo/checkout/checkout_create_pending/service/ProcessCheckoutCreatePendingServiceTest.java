package org.pipelineframework.tpfgo.checkout.checkout_create_pending.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.pipelineframework.tpfgo.common.domain.OrderPending;
import org.pipelineframework.tpfgo.common.domain.ValidatedOrderRequest;
import org.pipelineframework.tpfgo.common.util.DeterministicIds;

class ProcessCheckoutCreatePendingServiceTest {

    private static final Instant FIXED_NOW = Instant.parse("2026-02-20T14:00:00Z");
    private static final Clock FIXED_CLOCK = Clock.fixed(FIXED_NOW, ZoneOffset.UTC);

    private final ProcessCheckoutCreatePendingService service =
        new ProcessCheckoutCreatePendingService(FIXED_CLOCK);

    @Test
    void processNullInputReturnsFailure() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
            () -> service.process(null).await().indefinitely());
        assertEquals("input must not be null", exception.getMessage());
    }

    @Test
    void processValidInputProducesOrderPendingWithAllFields() {
        UUID requestId = UUID.randomUUID();
        UUID customerId = UUID.randomUUID();
        UUID restaurantId = UUID.randomUUID();
        ValidatedOrderRequest input = validatedRequest(requestId, customerId, restaurantId, new BigDecimal("25.00"), "USD");

        OrderPending pending = service.process(input).await().indefinitely();

        assertNotNull(pending);
        assertEquals(requestId, pending.requestId());
        assertEquals(customerId, pending.customerId());
        assertEquals(restaurantId, pending.restaurantId());
        assertEquals(new BigDecimal("25.00"), pending.totalAmount());
        assertEquals("USD", pending.currency());
        assertEquals(FIXED_NOW, pending.createdAt());
    }

    @Test
    void processProducesDeterministicOrderId() {
        UUID requestId = UUID.randomUUID();
        UUID customerId = UUID.randomUUID();
        UUID restaurantId = UUID.randomUUID();
        ValidatedOrderRequest input = validatedRequest(requestId, customerId, restaurantId, new BigDecimal("10.00"), "EUR");

        UUID expectedOrderId = DeterministicIds.uuid(
            "order",
            requestId.toString(),
            customerId.toString(),
            restaurantId.toString());

        OrderPending result = service.process(input).await().indefinitely();

        assertEquals(expectedOrderId, result.orderId());
    }

    @Test
    void processSameInputAlwaysProducesSameOrderId() {
        UUID requestId = UUID.fromString("11111111-1111-1111-1111-111111111111");
        UUID customerId = UUID.fromString("22222222-2222-2222-2222-222222222222");
        UUID restaurantId = UUID.fromString("33333333-3333-3333-3333-333333333333");
        ValidatedOrderRequest input = validatedRequest(requestId, customerId, restaurantId, new BigDecimal("5.00"), "USD");

        OrderPending result1 = service.process(input).await().indefinitely();
        OrderPending result2 = service.process(input).await().indefinitely();

        assertEquals(result1.orderId(), result2.orderId());
    }

    @Test
    void processDifferentInputsProduceDifferentOrderIds() {
        UUID requestId1 = UUID.randomUUID();
        UUID requestId2 = UUID.randomUUID();
        UUID customerId = UUID.randomUUID();
        UUID restaurantId = UUID.randomUUID();

        ValidatedOrderRequest input1 = validatedRequest(requestId1, customerId, restaurantId, new BigDecimal("10.00"), "USD");
        ValidatedOrderRequest input2 = validatedRequest(requestId2, customerId, restaurantId, new BigDecimal("10.00"), "USD");

        OrderPending result1 = service.process(input1).await().indefinitely();
        OrderPending result2 = service.process(input2).await().indefinitely();

        assertNotEquals(result1.orderId(), result2.orderId());
    }

    @Test
    void processUsesClockForCreatedAt() {
        Instant specificInstant = Instant.parse("2026-03-15T09:00:00Z");
        Clock specificClock = Clock.fixed(specificInstant, ZoneOffset.UTC);
        ProcessCheckoutCreatePendingService svc = new ProcessCheckoutCreatePendingService(specificClock);

        ValidatedOrderRequest input = validatedRequest(
            UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), new BigDecimal("7.50"), "GBP");

        OrderPending result = svc.process(input).await().indefinitely();

        assertEquals(specificInstant, result.createdAt());
    }

    @Test
    void processPreservesCurrencyCode() {
        ValidatedOrderRequest input = validatedRequest(
            UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), new BigDecimal("3.00"), "JPY");

        OrderPending result = service.process(input).await().indefinitely();

        assertEquals("JPY", result.currency());
    }

    @Test
    void processOrderIdIsNotNull() {
        ValidatedOrderRequest input = validatedRequest(
            UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), new BigDecimal("1.00"), "USD");

        OrderPending result = service.process(input).await().indefinitely();

        assertNotNull(result.orderId());
    }

    // Regression: zero totalAmount should be accepted
    @Test
    void processZeroTotalAmountSucceeds() {
        ValidatedOrderRequest input = validatedRequest(
            UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), BigDecimal.ZERO, "USD");

        OrderPending result = service.process(input).await().indefinitely();

        assertNotNull(result);
        assertEquals(BigDecimal.ZERO, result.totalAmount());
    }

    private static ValidatedOrderRequest validatedRequest(
        UUID requestId, UUID customerId, UUID restaurantId,
        BigDecimal totalAmount, String currency) {
        return new ValidatedOrderRequest(
            requestId, customerId, restaurantId,
            "test items", totalAmount, currency,
            Instant.parse("2026-01-01T00:00:00Z"));
    }
}