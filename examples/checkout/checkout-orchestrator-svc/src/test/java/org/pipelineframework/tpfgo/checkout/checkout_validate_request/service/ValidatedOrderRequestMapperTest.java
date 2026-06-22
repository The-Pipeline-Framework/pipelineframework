package org.pipelineframework.tpfgo.checkout.checkout_validate_request.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.pipelineframework.tpfgo.checkout.grpc.PipelineTypes;
import org.pipelineframework.tpfgo.common.domain.OrderItem;
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

        PipelineTypes.ValidatedOrderRequest grpc =
            PipelineTypes.ValidatedOrderRequest.newBuilder()
                .setRequestId(requestId.toString())
                .setCustomerId(customerId.toString())
                .setRestaurantId(restaurantId.toString())
                .addItems(orderItem("pasta", 2))
                .setTotalAmount(totalAmount.toPlainString())
                .setCurrency("USD")
                .setValidatedAt(validatedAt.toString())
                .build();

        ValidatedOrderRequest domain = mapper.fromExternal(grpc);

        assertNotNull(domain);
        assertEquals(requestId, domain.requestId());
        assertEquals(customerId, domain.customerId());
        assertEquals(restaurantId, domain.restaurantId());
        assertEquals(List.of(new OrderItem("pasta", 2)), domain.items());
        assertEquals(totalAmount, domain.totalAmount());
        assertEquals("USD", domain.currency());
        assertEquals(validatedAt, domain.validatedAt());
    }

    @Test
    void fromExternalEmptyStringFieldsFailFast() {
        PipelineTypes.ValidatedOrderRequest grpc =
            PipelineTypes.ValidatedOrderRequest.newBuilder()
                .setRequestId("")
                .setCustomerId("")
                .setRestaurantId("")
                .addItems(orderItem("item", 1))
                .setTotalAmount("")
                .setCurrency("EUR")
                .setValidatedAt("")
                .build();

        assertThrows(NullPointerException.class, () -> mapper.fromExternal(grpc));
    }

    @Test
    void fromExternalThrowsOnInvalidUuidInRequestId() {
        PipelineTypes.ValidatedOrderRequest grpc =
            PipelineTypes.ValidatedOrderRequest.newBuilder()
                .setRequestId("not-a-uuid")
                .build();

        assertThrows(IllegalArgumentException.class, () -> mapper.fromExternal(grpc));
    }

    @Test
    void fromExternalThrowsOnInvalidInstantString() {
        PipelineTypes.ValidatedOrderRequest grpc =
            PipelineTypes.ValidatedOrderRequest.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setCustomerId(UUID.randomUUID().toString())
                .setRestaurantId(UUID.randomUUID().toString())
                .addItems(orderItem("item", 1))
                .setTotalAmount("10.00")
                .setCurrency("USD")
                .setValidatedAt("bad-timestamp")
                .build();

        assertThrows(IllegalArgumentException.class, () -> mapper.fromExternal(grpc));
    }

    @Test
    void fromExternalThrowsOnInvalidDecimalString() {
        PipelineTypes.ValidatedOrderRequest grpc =
            PipelineTypes.ValidatedOrderRequest.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setCustomerId(UUID.randomUUID().toString())
                .setRestaurantId(UUID.randomUUID().toString())
                .addItems(orderItem("item", 1))
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
            requestId,
            customerId,
            restaurantId,
            List.of(new OrderItem("salad", 1)),
            totalAmount,
            "GBP",
            validatedAt);

        PipelineTypes.ValidatedOrderRequest grpc = mapper.toExternal(domain);

        assertNotNull(grpc);
        assertEquals(requestId.toString(), grpc.getRequestId());
        assertEquals(customerId.toString(), grpc.getCustomerId());
        assertEquals(restaurantId.toString(), grpc.getRestaurantId());
        assertEquals(List.of(orderItem("salad", 1)), grpc.getItemsList());
        assertEquals(totalAmount.toPlainString(), grpc.getTotalAmount());
        assertEquals("GBP", grpc.getCurrency());
        assertEquals(validatedAt.toString(), grpc.getValidatedAt());
    }

    @Test
    void validatedOrderRequestRejectsNullRequiredFields() {
        assertThrows(NullPointerException.class, () -> new ValidatedOrderRequest(
            null, null, null, List.of(new OrderItem("item", 1)), null, "USD", null));
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
            requestId,
            customerId,
            restaurantId,
            List.of(new OrderItem("ramen", 2)),
            totalAmount,
            "JPY",
            validatedAt);

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

        PipelineTypes.ValidatedOrderRequest original =
            PipelineTypes.ValidatedOrderRequest.newBuilder()
                .setRequestId(requestId.toString())
                .setCustomerId(customerId.toString())
                .setRestaurantId(restaurantId.toString())
                .addItems(orderItem("steak", 1))
                .setTotalAmount("45.00")
                .setCurrency("EUR")
                .setValidatedAt(validatedAt.toString())
                .build();

        PipelineTypes.ValidatedOrderRequest roundTripped =
            mapper.toExternal(mapper.fromExternal(original));

        assertEquals(original.getRequestId(), roundTripped.getRequestId());
        assertEquals(original.getCustomerId(), roundTripped.getCustomerId());
        assertEquals(original.getRestaurantId(), roundTripped.getRestaurantId());
        assertEquals(original.getItemsList(), roundTripped.getItemsList());
        assertEquals(original.getTotalAmount(), roundTripped.getTotalAmount());
        assertEquals(original.getCurrency(), roundTripped.getCurrency());
        assertEquals(original.getValidatedAt(), roundTripped.getValidatedAt());
    }

    // Regression: unicode item text in a round-trip payload stays intact
    @Test
    void roundTripPreservesUnicodeItems() {
        ValidatedOrderRequest domain = new ValidatedOrderRequest(
            UUID.randomUUID(),
            UUID.randomUUID(),
            UUID.randomUUID(),
            List.of(new OrderItem("拉面", 2), new OrderItem("饺子", 4)),
            new BigDecimal("30.00"),
            "CNY",
            Instant.parse("2026-07-01T00:00:00Z"));

        ValidatedOrderRequest roundTripped = mapper.fromExternal(mapper.toExternal(domain));

        assertEquals(
            List.of(
                new OrderItem("拉面", 2),
                new OrderItem("饺子", 4)
            ),
            roundTripped.items());
    }

    private static PipelineTypes.OrderItem orderItem(String sku, int quantity) {
        return PipelineTypes.OrderItem.newBuilder()
            .setSku(sku)
            .setQuantity(quantity)
            .build();
    }
}

