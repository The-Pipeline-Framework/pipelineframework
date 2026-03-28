package org.pipelineframework.tpfgo.common.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record OrderDelivered(
    UUID orderId,
    UUID customerId,
    UUID dispatchId,
    UUID courierId,
    UUID restaurantId,
    UUID kitchenTicketId,
    Instant deliveredAt,
    BigDecimal amount,
    String currency,
    String lineageDigest
) {
    public OrderDelivered {
        Objects.requireNonNull(orderId, "orderId must not be null");
        Objects.requireNonNull(customerId, "customerId must not be null");
        Objects.requireNonNull(dispatchId, "dispatchId must not be null");
        Objects.requireNonNull(courierId, "courierId must not be null");
        Objects.requireNonNull(restaurantId, "restaurantId must not be null");
        Objects.requireNonNull(kitchenTicketId, "kitchenTicketId must not be null");
        Objects.requireNonNull(deliveredAt, "deliveredAt must not be null");
        Objects.requireNonNull(amount, "amount must not be null");
        Objects.requireNonNull(currency, "currency must not be null");
        Objects.requireNonNull(lineageDigest, "lineageDigest must not be null");
    }
}
