package org.pipelineframework.tpfgo.common.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record OrderAcceptedByRestaurant(
    UUID orderId,
    UUID requestId,
    UUID customerId,
    UUID restaurantId,
    BigDecimal totalAmount,
    String currency,
    Instant acceptedAt,
    UUID kitchenTicketId
) {
    public OrderAcceptedByRestaurant {
        Objects.requireNonNull(orderId, "orderId must not be null");
        Objects.requireNonNull(requestId, "requestId must not be null");
        Objects.requireNonNull(customerId, "customerId must not be null");
        Objects.requireNonNull(restaurantId, "restaurantId must not be null");
        Objects.requireNonNull(totalAmount, "totalAmount must not be null");
        Objects.requireNonNull(currency, "currency must not be null");
        Objects.requireNonNull(acceptedAt, "acceptedAt must not be null");
        Objects.requireNonNull(kitchenTicketId, "kitchenTicketId must not be null");
    }
}
