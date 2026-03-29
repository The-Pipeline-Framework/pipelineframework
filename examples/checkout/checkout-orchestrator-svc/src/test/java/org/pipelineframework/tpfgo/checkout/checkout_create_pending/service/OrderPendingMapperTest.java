package org.pipelineframework.tpfgo.checkout.checkout_create_pending.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.pipelineframework.tpfgo.checkout.grpc.CheckoutCreatePendingSvc;
import org.pipelineframework.tpfgo.common.domain.OrderPending;

class OrderPendingMapperTest {

    private final OrderPendingMapper mapper = new OrderPendingMapper();

    // --- fromExternal ---

    @Test
    void fromExternalMapsAllFieldsFromGrpcMessage() {
        UUID orderId = UUID.randomUUID();
        UUID requestId = UUID.randomUUID();
        UUID customerId = UUID.randomUUID();
        UUID restaurantId = UUID.randomUUID();
        BigDecimal totalAmount = new BigDecimal("42.50");
        Instant createdAt = Instant.parse("2026-01-15T10:00:00Z");

        CheckoutCreatePendingSvc.OrderPending grpc = CheckoutCreatePendingSvc.OrderPending.newBuilder()
            .setOrderId(orderId.toString())
            .setRequestId(requestId.toString())
            .setCustomerId(customerId.toString())
            .setRestaurantId(restaurantId.toString())
            .setTotalAmount(totalAmount.toPlainString())
            .setCurrency("USD")
            .setCreatedAt(createdAt.toString())
            .build();

        OrderPending domain = mapper.fromExternal(grpc);

        assertNotNull(domain);
        assertEquals(orderId, domain.orderId());
        assertEquals(requestId, domain.requestId());
        assertEquals(customerId, domain.customerId());
        assertEquals(restaurantId, domain.restaurantId());
        assertEquals(totalAmount, domain.totalAmount());
        assertEquals("USD", domain.currency());
        assertEquals(createdAt, domain.createdAt());
    }

    @Test
    void fromExternalEmptyUuidStringThrowsDueToNonNullRecord() {
        CheckoutCreatePendingSvc.OrderPending grpc = CheckoutCreatePendingSvc.OrderPending.newBuilder()
            .setOrderId("")
            .setRequestId(UUID.randomUUID().toString())
            .setCustomerId(UUID.randomUUID().toString())
            .setRestaurantId(UUID.randomUUID().toString())
            .setTotalAmount("10.00")
            .setCurrency("USD")
            .setCreatedAt(Instant.now().toString())
            .build();

        // GrpcMappingSupport.uuid("") returns null; OrderPending record rejects null orderId
        assertThrows(NullPointerException.class, () -> mapper.fromExternal(grpc));
    }

    @Test
    void fromExternalThrowsOnInvalidUuidString() {
        CheckoutCreatePendingSvc.OrderPending grpc = CheckoutCreatePendingSvc.OrderPending.newBuilder()
            .setOrderId("not-a-valid-uuid")
            .build();

        assertThrows(IllegalArgumentException.class, () -> mapper.fromExternal(grpc));
    }

    @Test
    void fromExternalThrowsOnInvalidDecimalString() {
        CheckoutCreatePendingSvc.OrderPending grpc = CheckoutCreatePendingSvc.OrderPending.newBuilder()
            .setOrderId(UUID.randomUUID().toString())
            .setRequestId(UUID.randomUUID().toString())
            .setCustomerId(UUID.randomUUID().toString())
            .setRestaurantId(UUID.randomUUID().toString())
            .setTotalAmount("not-a-number")
            .setCurrency("USD")
            .setCreatedAt(Instant.now().toString())
            .build();

        assertThrows(IllegalArgumentException.class, () -> mapper.fromExternal(grpc));
    }

    @Test
    void fromExternalThrowsOnInvalidInstantString() {
        CheckoutCreatePendingSvc.OrderPending grpc = CheckoutCreatePendingSvc.OrderPending.newBuilder()
            .setOrderId(UUID.randomUUID().toString())
            .setRequestId(UUID.randomUUID().toString())
            .setCustomerId(UUID.randomUUID().toString())
            .setRestaurantId(UUID.randomUUID().toString())
            .setTotalAmount("10.00")
            .setCurrency("USD")
            .setCreatedAt("not-a-timestamp")
            .build();

        assertThrows(IllegalArgumentException.class, () -> mapper.fromExternal(grpc));
    }

    @Test
    void fromExternalPreservesCurrencyString() {
        CheckoutCreatePendingSvc.OrderPending grpc = validGrpcMessage(
            UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
            "99.00", "JPY", Instant.parse("2026-03-01T00:00:00Z"));

        OrderPending domain = mapper.fromExternal(grpc);

        assertEquals("JPY", domain.currency());
    }

    // --- toExternal ---

    @Test
    void toExternalMapsAllFieldsToGrpcMessage() {
        UUID orderId = UUID.randomUUID();
        UUID requestId = UUID.randomUUID();
        UUID customerId = UUID.randomUUID();
        UUID restaurantId = UUID.randomUUID();
        BigDecimal totalAmount = new BigDecimal("99.99");
        Instant createdAt = Instant.parse("2026-03-01T08:30:00Z");

        OrderPending domain = new OrderPending(
            orderId, requestId, customerId, restaurantId,
            totalAmount, "GBP", createdAt);

        CheckoutCreatePendingSvc.OrderPending grpc = mapper.toExternal(domain);

        assertNotNull(grpc);
        assertEquals(orderId.toString(), grpc.getOrderId());
        assertEquals(requestId.toString(), grpc.getRequestId());
        assertEquals(customerId.toString(), grpc.getCustomerId());
        assertEquals(restaurantId.toString(), grpc.getRestaurantId());
        assertEquals(totalAmount.toPlainString(), grpc.getTotalAmount());
        assertEquals("GBP", grpc.getCurrency());
        assertEquals(createdAt.toString(), grpc.getCreatedAt());
    }

    @Test
    void toExternalUsesPlainStringForDecimal() {
        // Verifies toPlainString is used — no scientific notation for large values
        BigDecimal largeAmount = new BigDecimal("9999999.99");
        OrderPending domain = new OrderPending(
            UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
            largeAmount, "USD", Instant.parse("2026-05-01T00:00:00Z"));

        CheckoutCreatePendingSvc.OrderPending grpc = mapper.toExternal(domain);

        assertEquals("9999999.99", grpc.getTotalAmount());
    }

    // --- round-trip ---

    @Test
    void roundTripDomainToExternalAndBack() {
        UUID orderId = UUID.randomUUID();
        UUID requestId = UUID.randomUUID();
        UUID customerId = UUID.randomUUID();
        UUID restaurantId = UUID.randomUUID();
        BigDecimal totalAmount = new BigDecimal("15.75");
        Instant createdAt = Instant.parse("2026-02-10T12:00:00Z");

        OrderPending original = new OrderPending(
            orderId, requestId, customerId, restaurantId,
            totalAmount, "JPY", createdAt);

        OrderPending roundTripped = mapper.fromExternal(mapper.toExternal(original));

        assertEquals(original.orderId(), roundTripped.orderId());
        assertEquals(original.requestId(), roundTripped.requestId());
        assertEquals(original.customerId(), roundTripped.customerId());
        assertEquals(original.restaurantId(), roundTripped.restaurantId());
        assertEquals(original.totalAmount(), roundTripped.totalAmount());
        assertEquals(original.currency(), roundTripped.currency());
        assertEquals(original.createdAt(), roundTripped.createdAt());
    }

    @Test
    void roundTripExternalToDomainAndBack() {
        UUID orderId = UUID.randomUUID();
        UUID requestId = UUID.randomUUID();
        UUID customerId = UUID.randomUUID();
        UUID restaurantId = UUID.randomUUID();
        Instant createdAt = Instant.parse("2026-04-05T14:30:00Z");

        CheckoutCreatePendingSvc.OrderPending original = validGrpcMessage(
            orderId, requestId, customerId, restaurantId,
            "25.00", "EUR", createdAt);

        CheckoutCreatePendingSvc.OrderPending roundTripped = mapper.toExternal(mapper.fromExternal(original));

        assertEquals(original.getOrderId(), roundTripped.getOrderId());
        assertEquals(original.getRequestId(), roundTripped.getRequestId());
        assertEquals(original.getCustomerId(), roundTripped.getCustomerId());
        assertEquals(original.getRestaurantId(), roundTripped.getRestaurantId());
        assertEquals(original.getTotalAmount(), roundTripped.getTotalAmount());
        assertEquals(original.getCurrency(), roundTripped.getCurrency());
        assertEquals(original.getCreatedAt(), roundTripped.getCreatedAt());
    }

    // Regression: large decimal value survives round-trip without scientific notation
    @Test
    void roundTripPreservesLargeDecimalWithoutScientificNotation() {
        BigDecimal largeAmount = new BigDecimal("9999999.99");
        OrderPending domain = new OrderPending(
            UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
            largeAmount, "USD", Instant.parse("2026-05-01T00:00:00Z"));

        OrderPending roundTripped = mapper.fromExternal(mapper.toExternal(domain));

        assertEquals(largeAmount, roundTripped.totalAmount());
    }

    private static CheckoutCreatePendingSvc.OrderPending validGrpcMessage(
        UUID orderId, UUID requestId, UUID customerId, UUID restaurantId,
        String totalAmount, String currency, Instant createdAt) {
        return CheckoutCreatePendingSvc.OrderPending.newBuilder()
            .setOrderId(orderId.toString())
            .setRequestId(requestId.toString())
            .setCustomerId(customerId.toString())
            .setRestaurantId(restaurantId.toString())
            .setTotalAmount(totalAmount)
            .setCurrency(currency)
            .setCreatedAt(createdAt.toString())
            .build();
    }
}