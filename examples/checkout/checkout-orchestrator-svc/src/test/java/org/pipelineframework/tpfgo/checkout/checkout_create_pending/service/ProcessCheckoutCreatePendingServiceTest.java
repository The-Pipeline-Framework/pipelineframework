package org.pipelineframework.tpfgo.checkout.checkout_create_pending.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

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
        AtomicReference<Throwable> captured = new AtomicReference<>();
        service.process(null)
            .subscribe().with(
                ignored -> {},
                captured::set);
        assertNotNull(captured.get());
        assertTrue(captured.get() instanceof IllegalArgumentException);
        assertEquals("input must not be null", captured.get().getMessage());
    }

    @Test
    void processValidInputProducesOrderPendingWithAllFields() {
        UUID requestId = UUID.randomUUID();
        UUID customerId = UUID.randomUUID();
        UUID restaurantId = UUID.randomUUID();
        ValidatedOrderRequest input = validatedRequest(requestId, customerId, restaurantId, new BigDecimal("25.00"), "USD");

        AtomicReference<OrderPending> result = new AtomicReference<>();
        service.process(input)
            .subscribe().with(result::set, err -> { throw new RuntimeException(err); });

        OrderPending pending = result.get();
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

        AtomicReference<OrderPending> result = new AtomicReference<>();
        service.process(input)
            .subscribe().with(result::set, err -> { throw new RuntimeException(err); });

        assertEquals(expectedOrderId, result.get().orderId());
    }

    @Test
    void processSameInputAlwaysProducesSameOrderId() {
        UUID requestId = UUID.fromString("11111111-1111-1111-1111-111111111111");
        UUID customerId = UUID.fromString("22222222-2222-2222-2222-222222222222");
        UUID restaurantId = UUID.fromString("33333333-3333-3333-3333-333333333333");
        ValidatedOrderRequest input = validatedRequest(requestId, customerId, restaurantId, new BigDecimal("5.00"), "USD");

        AtomicReference<OrderPending> result1 = new AtomicReference<>();
        AtomicReference<OrderPending> result2 = new AtomicReference<>();
        service.process(input).subscribe().with(result1::set, err -> { throw new RuntimeException(err); });
        service.process(input).subscribe().with(result2::set, err -> { throw new RuntimeException(err); });

        assertEquals(result1.get().orderId(), result2.get().orderId());
    }

    @Test
    void processDifferentInputsProduceDifferentOrderIds() {
        UUID requestId1 = UUID.randomUUID();
        UUID requestId2 = UUID.randomUUID();
        UUID customerId = UUID.randomUUID();
        UUID restaurantId = UUID.randomUUID();

        ValidatedOrderRequest input1 = validatedRequest(requestId1, customerId, restaurantId, new BigDecimal("10.00"), "USD");
        ValidatedOrderRequest input2 = validatedRequest(requestId2, customerId, restaurantId, new BigDecimal("10.00"), "USD");

        AtomicReference<OrderPending> result1 = new AtomicReference<>();
        AtomicReference<OrderPending> result2 = new AtomicReference<>();
        service.process(input1).subscribe().with(result1::set, err -> { throw new RuntimeException(err); });
        service.process(input2).subscribe().with(result2::set, err -> { throw new RuntimeException(err); });

        assertNotEquals(result1.get().orderId(), result2.get().orderId());
    }

    @Test
    void processUsesClockForCreatedAt() {
        Instant specificInstant = Instant.parse("2026-03-15T09:00:00Z");
        Clock specificClock = Clock.fixed(specificInstant, ZoneOffset.UTC);
        ProcessCheckoutCreatePendingService svc = new ProcessCheckoutCreatePendingService(specificClock);

        ValidatedOrderRequest input = validatedRequest(
            UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), new BigDecimal("7.50"), "GBP");

        AtomicReference<OrderPending> result = new AtomicReference<>();
        svc.process(input).subscribe().with(result::set, err -> { throw new RuntimeException(err); });

        assertEquals(specificInstant, result.get().createdAt());
    }

    @Test
    void processPreservesCurrencyCode() {
        ValidatedOrderRequest input = validatedRequest(
            UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), new BigDecimal("3.00"), "JPY");

        AtomicReference<OrderPending> result = new AtomicReference<>();
        service.process(input).subscribe().with(result::set, err -> { throw new RuntimeException(err); });

        assertEquals("JPY", result.get().currency());
    }

    @Test
    void processOrderIdIsNotNull() {
        ValidatedOrderRequest input = validatedRequest(
            UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), new BigDecimal("1.00"), "USD");

        AtomicReference<OrderPending> result = new AtomicReference<>();
        service.process(input).subscribe().with(result::set, err -> { throw new RuntimeException(err); });

        assertNotNull(result.get().orderId());
    }

    // Regression: zero totalAmount should be accepted
    @Test
    void processZeroTotalAmountSucceeds() {
        ValidatedOrderRequest input = validatedRequest(
            UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), BigDecimal.ZERO, "USD");

        AtomicReference<OrderPending> result = new AtomicReference<>();
        service.process(input).subscribe().with(result::set, err -> { throw new RuntimeException(err); });

        assertNotNull(result.get());
        assertEquals(BigDecimal.ZERO, result.get().totalAmount());
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