package org.pipelineframework.tpfgo.checkout.checkout_validate_request.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
        BigDecimal totalAmount = new BigDecimal("55.25");
        Instant validatedAt = Instant.parse("2026-01-20T09:15:00Z");

        CheckoutValidateRequestSvc.ValidatedOrderRequest grpc =
            CheckoutValidateRequestSvc.ValidatedOrderRequest.newBuilder()
                .setRequestId(requestId.toString())
                .setCustomerId(customerId.toString())
                .setRestaurantId(restaurantId.toString())
                .setItems("pasta x2")
                .setTotalAmount(totalAmount.toPlainString())
                .setCurrency("EUR")
                .setValidatedAt(validatedAt.toString())
                .build();

        ValidatedOrderRequest domain = mapper.fromExternal(grpc);

        assertNotNull(domain);
        assertEquals(requestId, domain.requestId());
        assertEquals(customerId, domain.customerId());
        assertEquals(restaurantId, domain.restaurantId());
        assertEquals("pasta x2", domain.items());
        assertEquals(totalAmount, domain.totalAmount());
        assertEquals("EUR", domain.currency());
        assertEquals(validatedAt, domain.validatedAt());
    }

    @Test
    void fromExternalEmptyUuidStringThrowsDueToNonNullRecord() {
        CheckoutValidateRequestSvc.ValidatedOrderRequest grpc =
            CheckoutValidateRequestSvc.ValidatedOrderRequest.newBuilder()
                .setRequestId("")
                .setCustomerId(UUID.randomUUID().toString())
                .setRestaurantId(UUID.randomUUID().toString())
                .setItems("item")
                .setTotalAmount("5.00")
                .setCurrency("USD")
                .setValidatedAt(Instant.now().toString())
                .build();

        // GrpcMappingSupport.uuid("") returns null; ValidatedOrderRequest record rejects null requestId
        assertThrows(NullPointerException.class, () -> mapper.fromExternal(grpc));
    }

    @Test
    void fromExternalThrowsOnInvalidUuidString() {
        CheckoutValidateRequestSvc.ValidatedOrderRequest grpc =
            CheckoutValidateRequestSvc.ValidatedOrderRequest.newBuilder()
                .setRequestId("not-a-uuid")
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
    void fromExternalPreservesItemsString() {
        CheckoutValidateRequestSvc.ValidatedOrderRequest grpc = validGrpcMessage(
            UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
            "special & items, more", "8.00", "GBP",
            Instant.parse("2026-02-01T00:00:00Z"));

        ValidatedOrderRequest domain = mapper.fromExternal(grpc);

        assertEquals("special & items, more", domain.items());
    }

    // --- toExternal ---

    @Test
    void toExternalMapsAllFieldsToGrpcMessage() {
        UUID requestId = UUID.randomUUID();
        UUID customerId = UUID.randomUUID();
        UUID restaurantId = UUID.randomUUID();
        BigDecimal totalAmount = new BigDecimal("12.34");
        Instant validatedAt = Instant.parse("2026-02-15T11:45:00Z");

        ValidatedOrderRequest domain = new ValidatedOrderRequest(
            requestId, customerId, restaurantId,
            "salad x1", totalAmount, "USD", validatedAt);

        CheckoutValidateRequestSvc.ValidatedOrderRequest grpc = mapper.toExternal(domain);

        assertNotNull(grpc);
        assertEquals(requestId.toString(), grpc.getRequestId());
        assertEquals(customerId.toString(), grpc.getCustomerId());
        assertEquals(restaurantId.toString(), grpc.getRestaurantId());
        assertEquals("salad x1", grpc.getItems());
        assertEquals(totalAmount.toPlainString(), grpc.getTotalAmount());
        assertEquals("USD", grpc.getCurrency());
        assertEquals(validatedAt.toString(), grpc.getValidatedAt());
    }

    @Test
    void toExternalUsesPlainStringForDecimal() {
        BigDecimal largeAmount = new BigDecimal("9999999.99");
        ValidatedOrderRequest domain = new ValidatedOrderRequest(
            UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
            "item", largeAmount, "USD",
            Instant.parse("2026-05-01T00:00:00Z"));

        CheckoutValidateRequestSvc.ValidatedOrderRequest grpc = mapper.toExternal(domain);

        assertEquals("9999999.99", grpc.getTotalAmount());
    }

    // --- round-trip ---

    @Test
    void roundTripDomainToExternalAndBack() {
        UUID requestId = UUID.randomUUID();
        UUID customerId = UUID.randomUUID();
        UUID restaurantId = UUID.randomUUID();
        BigDecimal totalAmount = new BigDecimal("44.44");
        Instant validatedAt = Instant.parse("2026-03-10T16:00:00Z");

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
        Instant validatedAt = Instant.parse("2026-04-12T13:00:00Z");

        CheckoutValidateRequestSvc.ValidatedOrderRequest original = validGrpcMessage(
            requestId, customerId, restaurantId,
            "dim sum x5", "30.00", "HKD", validatedAt);

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

    // Regression: large decimal value survives round-trip without scientific notation
    @Test
    void roundTripPreservesLargeDecimalWithoutScientificNotation() {
        BigDecimal largeAmount = new BigDecimal("9999999.99");
        ValidatedOrderRequest domain = new ValidatedOrderRequest(
            UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
            "item", largeAmount, "USD",
            Instant.parse("2026-06-01T00:00:00Z"));

        ValidatedOrderRequest roundTripped = mapper.fromExternal(mapper.toExternal(domain));

        assertEquals(largeAmount, roundTripped.totalAmount());
    }

    private static CheckoutValidateRequestSvc.ValidatedOrderRequest validGrpcMessage(
        UUID requestId, UUID customerId, UUID restaurantId,
        String items, String totalAmount, String currency, Instant validatedAt) {
        return CheckoutValidateRequestSvc.ValidatedOrderRequest.newBuilder()
            .setRequestId(requestId.toString())
            .setCustomerId(customerId.toString())
            .setRestaurantId(restaurantId.toString())
            .setItems(items)
            .setTotalAmount(totalAmount)
            .setCurrency(currency)
            .setValidatedAt(validatedAt.toString())
            .build();
    }
}