package org.pipelineframework.tpfgo.checkout.checkout_validate_request.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.pipelineframework.tpfgo.checkout.grpc.PipelineTypes;
import org.pipelineframework.tpfgo.common.domain.OrderItem;
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

        PipelineTypes.PlaceOrderRequest grpc =
            PipelineTypes.PlaceOrderRequest.newBuilder()
                .setRequestId(requestId.toString())
                .setCustomerId(customerId.toString())
                .setRestaurantId(restaurantId.toString())
                .addItems(orderItem("burger", 1))
                .addItems(orderItem("fries", 2))
                .setTotalAmount(totalAmount.toPlainString())
                .setCurrency("USD")
                .build();

        PlaceOrderRequest domain = mapper.fromExternal(grpc);

        assertNotNull(domain);
        assertEquals(requestId, domain.requestId());
        assertEquals(customerId, domain.customerId());
        assertEquals(restaurantId, domain.restaurantId());
        assertEquals(List.of(
            new OrderItem("burger", 1),
            new OrderItem("fries", 2)
        ), domain.items());
        assertEquals(totalAmount, domain.totalAmount());
        assertEquals("USD", domain.currency());
    }

    @Test
    void fromExternalEmptyUuidStringsFailFast() {
        PipelineTypes.PlaceOrderRequest grpc =
            PipelineTypes.PlaceOrderRequest.newBuilder()
                .setRequestId("")
                .setCustomerId("")
                .setRestaurantId("")
                .addItems(orderItem("item", 1))
                .setTotalAmount("")
                .setCurrency("EUR")
                .build();

        assertThrows(NullPointerException.class, () -> mapper.fromExternal(grpc));
    }

    @Test
    void fromExternalThrowsOnInvalidUuidString() {
        PipelineTypes.PlaceOrderRequest grpc =
            PipelineTypes.PlaceOrderRequest.newBuilder()
                .setRequestId("bad-uuid-value")
                .build();

        assertThrows(IllegalArgumentException.class, () -> mapper.fromExternal(grpc));
    }

    @Test
    void fromExternalThrowsOnInvalidDecimalString() {
        PipelineTypes.PlaceOrderRequest grpc =
            PipelineTypes.PlaceOrderRequest.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setCustomerId(UUID.randomUUID().toString())
                .setRestaurantId(UUID.randomUUID().toString())
                .addItems(orderItem("item", 1))
                .setTotalAmount("not-a-decimal")
                .setCurrency("USD")
                .build();

        assertThrows(IllegalArgumentException.class, () -> mapper.fromExternal(grpc));
    }

    @Test
    void fromExternalPreservesSkuWhitespaceAsCanonicalValue() {
        PipelineTypes.PlaceOrderRequest grpc =
            PipelineTypes.PlaceOrderRequest.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setCustomerId(UUID.randomUUID().toString())
                .setRestaurantId(UUID.randomUUID().toString())
                .addItems(orderItem("  sku with spaces  ", 3))
                .setTotalAmount("5.00")
                .setCurrency("USD")
                .build();

        PlaceOrderRequest domain = mapper.fromExternal(grpc);

        assertEquals("sku with spaces", domain.items().get(0).sku());
    }

    // --- toExternal ---

    @Test
    void toExternalMapsAllFieldsToGrpcMessage() {
        UUID requestId = UUID.randomUUID();
        UUID customerId = UUID.randomUUID();
        UUID restaurantId = UUID.randomUUID();
        BigDecimal totalAmount = new BigDecimal("7.25");

        PlaceOrderRequest domain = new PlaceOrderRequest(
            requestId,
            customerId,
            restaurantId,
            List.of(new OrderItem("pizza", 2)),
            totalAmount,
            "GBP");

        PipelineTypes.PlaceOrderRequest grpc = mapper.toExternal(domain);

        assertNotNull(grpc);
        assertEquals(requestId.toString(), grpc.getRequestId());
        assertEquals(customerId.toString(), grpc.getCustomerId());
        assertEquals(restaurantId.toString(), grpc.getRestaurantId());
        assertEquals(List.of(orderItem("pizza", 2)), grpc.getItemsList());
        assertEquals(totalAmount.toPlainString(), grpc.getTotalAmount());
        assertEquals("GBP", grpc.getCurrency());
    }

    @Test
    void placeOrderRequestRejectsNullRequiredFields() {
        assertThrows(NullPointerException.class, () -> new PlaceOrderRequest(
            null, null, null, List.of(new OrderItem("item", 1)), null, "USD"));
    }

    // --- round-trip ---

    @Test
    void roundTripDomainToExternalAndBack() {
        UUID requestId = UUID.randomUUID();
        UUID customerId = UUID.randomUUID();
        UUID restaurantId = UUID.randomUUID();
        BigDecimal totalAmount = new BigDecimal("33.33");

        PlaceOrderRequest original = new PlaceOrderRequest(
            requestId,
            customerId,
            restaurantId,
            List.of(new OrderItem("sushi", 3)),
            totalAmount,
            "JPY");

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

        PipelineTypes.PlaceOrderRequest original =
            PipelineTypes.PlaceOrderRequest.newBuilder()
                .setRequestId(requestId.toString())
                .setCustomerId(customerId.toString())
                .setRestaurantId(restaurantId.toString())
                .addItems(orderItem("tacos", 4))
                .setTotalAmount("12.00")
                .setCurrency("EUR")
                .build();

        PipelineTypes.PlaceOrderRequest roundTripped =
            mapper.toExternal(mapper.fromExternal(original));

        assertEquals(original.getRequestId(), roundTripped.getRequestId());
        assertEquals(original.getCustomerId(), roundTripped.getCustomerId());
        assertEquals(original.getRestaurantId(), roundTripped.getRestaurantId());
        assertEquals(original.getItemsList(), roundTripped.getItemsList());
        assertEquals(original.getTotalAmount(), roundTripped.getTotalAmount());
        assertEquals(original.getCurrency(), roundTripped.getCurrency());
    }

    // Regression: zero amount round-trips correctly
    @Test
    void roundTripZeroAmountPreservesValue() {
        PlaceOrderRequest domain = new PlaceOrderRequest(
            UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
            List.of(new OrderItem("item", 1)), BigDecimal.ZERO, "USD");

        PlaceOrderRequest roundTripped = mapper.fromExternal(mapper.toExternal(domain));

        assertEquals(0, BigDecimal.ZERO.compareTo(roundTripped.totalAmount()));
    }

    private static PipelineTypes.OrderItem orderItem(String sku, int quantity) {
        return PipelineTypes.OrderItem.newBuilder()
            .setSku(sku)
            .setQuantity(quantity)
            .build();
    }
}

