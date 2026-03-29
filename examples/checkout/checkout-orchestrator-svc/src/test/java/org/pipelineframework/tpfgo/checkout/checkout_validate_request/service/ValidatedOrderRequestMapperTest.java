package org.pipelineframework.tpfgo.checkout.checkout_validate_request.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.pipelineframework.tpfgo.checkout.grpc.CheckoutValidateRequestSvc;
import org.pipelineframework.tpfgo.common.domain.ValidatedOrderRequest;

class ValidatedOrderRequestMapperTest {

    private final ValidatedOrderRequestMapper mapper = new ValidatedOrderRequestMapper();

    // --- fromExternal ---

    @Test
    void fromExternalMapsAllFieldsFromGrpcMessage() {
        UUID requestId = UUID.randomUUID();
        UUID customerId = UUID.randomUUID();
        UUID restaurantId = UUID.randomUUID();
        BigDecimal totalAmount = new BigDecimal("55.00");
        Instant validatedAt = Instant.parse("2026-02-28T16:45:00Z");

        CheckoutValidateRequestSvc.ValidatedOrderRequest grpc =
            CheckoutValidateRequestSvc.ValidatedOrderRequest.newBuilder()
                .setRequestId(requestId.toString())
                .setCustomerId(customerId.toString())
                .setRestaurantId(restaurantId.toString())
                .setItems("pasta x2")
                .setTotalAmount(totalAmount.toPlainString())
                .setCurrency("USD")
                .setValidatedAt(validatedAt.toString())
                .build();

        ValidatedOrderRequest domain = mapper.fromExternal(grpc);

        assertNotNull(domain);
        assertEquals(requestId, domain.requestId());
        assertEquals(customerId, domain.customerId());
        assertEquals(restaurantId, domain.restaurantId());
        assertEquals("pasta x2", domain.items());
        assertEquals(totalAmount, domain.totalAmount());
        assertEquals("USD", domain.currency());
        assertEquals(validatedAt, domain.validatedAt());
    }

    @Test
    void fromExternalEmptyStringFieldsYieldNullTypedFields() {
        CheckoutValidateRequestSvc.ValidatedOrderRequest grpc =
            CheckoutValidateRequestSvc.ValidatedOrderRequest.newBuilder()
                .setRequestId("")
                .setCustomerId("")
                .setRestaurantId("")
                .setItems("item")
                .setTotalAmount("")
                .setCurrency("EUR")
                .setValidatedAt("")
                .build();

        ValidatedOrderRequest domain = mapper.fromExternal(grpc);

        assertNotNull(domain);
        assertNull(domain.requestId());
        assertNull(domain.customerId());
        assertNull(domain.restaurantId());
        assertEquals("item", domain.items());
        assertNull(domain.totalAmount());
        assertEquals("EUR", domain.currency());
        assertNull(domain.validatedAt());
    }

    @Test
    void fromExternalThrowsOnInvalidUuidInRequestId() {
        CheckoutValidateRequestSvc.ValidatedOrderRequest grpc =
            CheckoutValidateRequestSvc.ValidatedOrderRequest.newBuilder()
                .setRequestId("not-a-uuid")
                .build();

        assertThrows(IllegalArgumentException.class, () -> mapper.fromExternal(grpc));
    }

    @Test
    void fromExternalThrowsOnInvalidInstantString() {
        CheckoutValidateRequestSvc.ValidatedOrderRequest grpc =
            CheckoutValidateRequestSvc.ValidatedOrderRequest.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setCustomerId(UUID.randomUUID().toString())
                .setRestaurantId(UUID.randomUUID().toString())
                .setItems("item")
                .setTotalAmount("10.00")
                .setCurrency("USD")
                .setValidatedAt("bad-timestamp")
                .build();

        assertThrows(IllegalArgumentException.class, () -> mapper.fromExternal(grpc));
    }

    @Test
    void fromExternalThrowsOnInvalidDecimalString() {
        CheckoutValidateRequestSvc.ValidatedOrderRequest grpc =
            CheckoutValidateRequestSvc.ValidatedOrderRequest.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setCustomerId(UUID.randomUUID().toString())
                .setRestaurantId(UUID.randomUUID().toString())
                .setItems("item")
                .setTotalAmount("not-a-number")
                .setCurrency("USD")
                .setValidatedAt(Instant.now().toString())
                .build();

        assertThrows(IllegalArgumentException.class, () -> mapper.fromExternal(grpc));
    }

    // --- toExternal ---

    @Test
    void toExternalMapsAllFieldsToGrpcMessage() {
        UUID requestId = UUID.randomUUID();
        UUID customerId = UUID.randomUUID();
        UUID restaurantId = UUID.randomUUID();
        BigDecimal totalAmount = new BigDecimal("88.88");
        Instant validatedAt = Instant.parse("2026-04-01T09:00:00Z");

        ValidatedOrderRequest domain = new ValidatedOrderRequest(
            requestId, customerId, restaurantId,
            "salad x1", totalAmount, "GBP", validatedAt);

        CheckoutValidateRequestSvc.ValidatedOrderRequest grpc = mapper.toExternal(domain);

        assertNotNull(grpc);
        assertEquals(requestId.toString(), grpc.getRequestId());
        assertEquals(customerId.toString(), grpc.getCustomerId());
        assertEquals(restaurantId.toString(), grpc.getRestaurantId());
        assertEquals("salad x1", grpc.getItems());
        assertEquals(totalAmount.toPlainString(), grpc.getTotalAmount());
        assertEquals("GBP", grpc.getCurrency());
        assertEquals(validatedAt.toString(), grpc.getValidatedAt());
    }

    @Test
    void toExternalNullUuidAndInstantFieldsYieldEmptyStrings() {
        ValidatedOrderRequest domain = new ValidatedOrderRequest(
            null, null, null, "item", null, "USD", null);

        CheckoutValidateRequestSvc.ValidatedOrderRequest grpc = mapper.toExternal(domain);

        assertNotNull(grpc);
        assertEquals("", grpc.getRequestId());
        assertEquals("", grpc.getCustomerId());
        assertEquals("", grpc.getRestaurantId());
        assertEquals("item", grpc.getItems());
        assertEquals("", grpc.getTotalAmount());
        assertEquals("USD", grpc.getCurrency());
        assertEquals("", grpc.getValidatedAt());
    }

    // --- round-trip ---

    @Test
    void roundTripDomainToExternalAndBack() {
        UUID requestId = UUID.randomUUID();
        UUID customerId = UUID.randomUUID();
        UUID restaurantId = UUID.randomUUID();
        BigDecimal totalAmount = new BigDecimal("24.95");
        Instant validatedAt = Instant.parse("2026-03-20T11:15:00Z");

        ValidatedOrderRequest original = new ValidatedOrderRequest(
            requestId, customerId, restaurantId,
            "ramen x2", totalAmount, "JPY", validatedAt);

        ValidatedOrderRequest roundTripped = mapper.fromExternal(mapper.toExternal(original));

        assertEquals(original.requestId(), roundTripped.requestId());
        assertEquals(original.customerId(), roundTripped.customerId());
        assertEquals(original.restaurantId(), roundTripped.restaurantId());
        assertEquals(original.items(), roundTripped.items());
        assertEquals(original.totalAmount(), roundTripped.totalAmount());
        assertEquals(original.currency(), roundTripped.currency());
        assertEquals(original.validatedAt(), roundTripped.validatedAt());
    }

    @Test
    void roundTripExternalToDomainAndBack() {
        UUID requestId = UUID.randomUUID();
        UUID customerId = UUID.randomUUID();
        UUID restaurantId = UUID.randomUUID();
        Instant validatedAt = Instant.parse("2026-06-10T13:00:00Z");

        CheckoutValidateRequestSvc.ValidatedOrderRequest original =
            CheckoutValidateRequestSvc.ValidatedOrderRequest.newBuilder()
                .setRequestId(requestId.toString())
                .setCustomerId(customerId.toString())
                .setRestaurantId(restaurantId.toString())
                .setItems("steak x1")
                .setTotalAmount("45.00")
                .setCurrency("EUR")
                .setValidatedAt(validatedAt.toString())
                .build();

        CheckoutValidateRequestSvc.ValidatedOrderRequest roundTripped =
            mapper.toExternal(mapper.fromExternal(original));

        assertEquals(original.getRequestId(), roundTripped.getRequestId());
        assertEquals(original.getCustomerId(), roundTripped.getCustomerId());
        assertEquals(original.getRestaurantId(), roundTripped.getRestaurantId());
        assertEquals(original.getItems(), roundTripped.getItems());
        assertEquals(original.getTotalAmount(), roundTripped.getTotalAmount());
        assertEquals(original.getCurrency(), roundTripped.getCurrency());
        assertEquals(original.getValidatedAt(), roundTripped.getValidatedAt());
    }

    // Regression: items string with Unicode characters survives round-trip
    @Test
    void roundTripPreservesUnicodeItemsString() {
        ValidatedOrderRequest domain = new ValidatedOrderRequest(
            UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
            "拉面 x2, 饺子 x4", new BigDecimal("30.00"), "CNY",
            Instant.parse("2026-07-01T00:00:00Z"));

        ValidatedOrderRequest roundTripped = mapper.fromExternal(mapper.toExternal(domain));

        assertEquals("拉面 x2, 饺子 x4", roundTripped.items());
    }
}