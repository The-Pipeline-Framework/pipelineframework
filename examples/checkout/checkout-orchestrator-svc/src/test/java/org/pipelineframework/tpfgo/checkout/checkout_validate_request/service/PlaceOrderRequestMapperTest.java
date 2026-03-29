package org.pipelineframework.tpfgo.checkout.checkout_validate_request.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.pipelineframework.tpfgo.checkout.grpc.CheckoutValidateRequestSvc;
import org.pipelineframework.tpfgo.common.domain.PlaceOrderRequest;

class PlaceOrderRequestMapperTest {

    private final PlaceOrderRequestMapper mapper = new PlaceOrderRequestMapper();

    // --- fromExternal ---

    @Test
    void fromExternalMapsAllFieldsFromGrpcMessage() {
        UUID requestId = UUID.randomUUID();
        UUID customerId = UUID.randomUUID();
        UUID restaurantId = UUID.randomUUID();
        BigDecimal totalAmount = new BigDecimal("18.50");

        CheckoutValidateRequestSvc.PlaceOrderRequest grpc =
            CheckoutValidateRequestSvc.PlaceOrderRequest.newBuilder()
                .setRequestId(requestId.toString())
                .setCustomerId(customerId.toString())
                .setRestaurantId(restaurantId.toString())
                .setItems("burger x1, fries x1")
                .setTotalAmount(totalAmount.toPlainString())
                .setCurrency("USD")
                .build();

        PlaceOrderRequest domain = mapper.fromExternal(grpc);

        assertNotNull(domain);
        assertEquals(requestId, domain.requestId());
        assertEquals(customerId, domain.customerId());
        assertEquals(restaurantId, domain.restaurantId());
        assertEquals("burger x1, fries x1", domain.items());
        assertEquals(totalAmount, domain.totalAmount());
        assertEquals("USD", domain.currency());
    }

    @Test
    void fromExternalEmptyUuidStringsYieldNullUuids() {
        CheckoutValidateRequestSvc.PlaceOrderRequest grpc =
            CheckoutValidateRequestSvc.PlaceOrderRequest.newBuilder()
                .setRequestId("")
                .setCustomerId("")
                .setRestaurantId("")
                .setItems("some items")
                .setTotalAmount("")
                .setCurrency("EUR")
                .build();

        PlaceOrderRequest domain = mapper.fromExternal(grpc);

        assertNotNull(domain);
        assertNull(domain.requestId());
        assertNull(domain.customerId());
        assertNull(domain.restaurantId());
        assertEquals("some items", domain.items());
        assertNull(domain.totalAmount());
        assertEquals("EUR", domain.currency());
    }

    @Test
    void fromExternalThrowsOnInvalidUuidString() {
        CheckoutValidateRequestSvc.PlaceOrderRequest grpc =
            CheckoutValidateRequestSvc.PlaceOrderRequest.newBuilder()
                .setRequestId("bad-uuid-value")
                .build();

        assertThrows(IllegalArgumentException.class, () -> mapper.fromExternal(grpc));
    }

    @Test
    void fromExternalThrowsOnInvalidDecimalString() {
        CheckoutValidateRequestSvc.PlaceOrderRequest grpc =
            CheckoutValidateRequestSvc.PlaceOrderRequest.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setCustomerId(UUID.randomUUID().toString())
                .setRestaurantId(UUID.randomUUID().toString())
                .setItems("item")
                .setTotalAmount("not-a-decimal")
                .setCurrency("USD")
                .build();

        assertThrows(IllegalArgumentException.class, () -> mapper.fromExternal(grpc));
    }

    @Test
    void fromExternalPreservesItemsStringWithSpecialCharacters() {
        CheckoutValidateRequestSvc.PlaceOrderRequest grpc =
            CheckoutValidateRequestSvc.PlaceOrderRequest.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setCustomerId(UUID.randomUUID().toString())
                .setRestaurantId(UUID.randomUUID().toString())
                .setItems("  item with spaces & commas,  more items  ")
                .setTotalAmount("5.00")
                .setCurrency("USD")
                .build();

        PlaceOrderRequest domain = mapper.fromExternal(grpc);

        assertEquals("  item with spaces & commas,  more items  ", domain.items());
    }

    // --- toExternal ---

    @Test
    void toExternalMapsAllFieldsToGrpcMessage() {
        UUID requestId = UUID.randomUUID();
        UUID customerId = UUID.randomUUID();
        UUID restaurantId = UUID.randomUUID();
        BigDecimal totalAmount = new BigDecimal("7.25");

        PlaceOrderRequest domain = new PlaceOrderRequest(
            requestId, customerId, restaurantId,
            "pizza x2", totalAmount, "GBP");

        CheckoutValidateRequestSvc.PlaceOrderRequest grpc = mapper.toExternal(domain);

        assertNotNull(grpc);
        assertEquals(requestId.toString(), grpc.getRequestId());
        assertEquals(customerId.toString(), grpc.getCustomerId());
        assertEquals(restaurantId.toString(), grpc.getRestaurantId());
        assertEquals("pizza x2", grpc.getItems());
        assertEquals(totalAmount.toPlainString(), grpc.getTotalAmount());
        assertEquals("GBP", grpc.getCurrency());
    }

    @Test
    void toExternalNullUuidFieldsYieldEmptyStrings() {
        PlaceOrderRequest domain = new PlaceOrderRequest(
            null, null, null, "items", null, "USD");

        CheckoutValidateRequestSvc.PlaceOrderRequest grpc = mapper.toExternal(domain);

        assertNotNull(grpc);
        assertEquals("", grpc.getRequestId());
        assertEquals("", grpc.getCustomerId());
        assertEquals("", grpc.getRestaurantId());
        assertEquals("items", grpc.getItems());
        assertEquals("", grpc.getTotalAmount());
        assertEquals("USD", grpc.getCurrency());
    }

    // --- round-trip ---

    @Test
    void roundTripDomainToExternalAndBack() {
        UUID requestId = UUID.randomUUID();
        UUID customerId = UUID.randomUUID();
        UUID restaurantId = UUID.randomUUID();
        BigDecimal totalAmount = new BigDecimal("33.33");

        PlaceOrderRequest original = new PlaceOrderRequest(
            requestId, customerId, restaurantId,
            "sushi x3", totalAmount, "JPY");

        PlaceOrderRequest roundTripped = mapper.fromExternal(mapper.toExternal(original));

        assertEquals(original.requestId(), roundTripped.requestId());
        assertEquals(original.customerId(), roundTripped.customerId());
        assertEquals(original.restaurantId(), roundTripped.restaurantId());
        assertEquals(original.items(), roundTripped.items());
        assertEquals(original.totalAmount(), roundTripped.totalAmount());
        assertEquals(original.currency(), roundTripped.currency());
    }

    @Test
    void roundTripExternalToDomainAndBack() {
        UUID requestId = UUID.randomUUID();
        UUID customerId = UUID.randomUUID();
        UUID restaurantId = UUID.randomUUID();

        CheckoutValidateRequestSvc.PlaceOrderRequest original =
            CheckoutValidateRequestSvc.PlaceOrderRequest.newBuilder()
                .setRequestId(requestId.toString())
                .setCustomerId(customerId.toString())
                .setRestaurantId(restaurantId.toString())
                .setItems("tacos x4")
                .setTotalAmount("12.00")
                .setCurrency("EUR")
                .build();

        CheckoutValidateRequestSvc.PlaceOrderRequest roundTripped =
            mapper.toExternal(mapper.fromExternal(original));

        assertEquals(original.getRequestId(), roundTripped.getRequestId());
        assertEquals(original.getCustomerId(), roundTripped.getCustomerId());
        assertEquals(original.getRestaurantId(), roundTripped.getRestaurantId());
        assertEquals(original.getItems(), roundTripped.getItems());
        assertEquals(original.getTotalAmount(), roundTripped.getTotalAmount());
        assertEquals(original.getCurrency(), roundTripped.getCurrency());
    }

    // Regression: zero amount round-trips correctly
    @Test
    void roundTripZeroAmountPreservesValue() {
        PlaceOrderRequest domain = new PlaceOrderRequest(
            UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
            "free item", BigDecimal.ZERO, "USD");

        PlaceOrderRequest roundTripped = mapper.fromExternal(mapper.toExternal(domain));

        assertEquals(0, BigDecimal.ZERO.compareTo(roundTripped.totalAmount()));
    }
}