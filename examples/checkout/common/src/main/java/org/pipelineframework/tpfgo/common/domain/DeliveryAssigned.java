package org.pipelineframework.tpfgo.common.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record DeliveryAssigned(
    UUID orderId,
    UUID customerId,
    UUID restaurantId,
    BigDecimal totalAmount,
    String currency,
    UUID kitchenTicketId,
    UUID dispatchId,
    UUID courierId,
    Integer etaMinutes,
    Instant assignedAt,
    String lineageDigest
) {
    public DeliveryAssigned {
        Objects.requireNonNull(orderId, "orderId must not be null");
        Objects.requireNonNull(customerId, "customerId must not be null");
        Objects.requireNonNull(restaurantId, "restaurantId must not be null");
        Objects.requireNonNull(totalAmount, "totalAmount must not be null");
        Objects.requireNonNull(currency, "currency must not be null");
        Objects.requireNonNull(kitchenTicketId, "kitchenTicketId must not be null");
        Objects.requireNonNull(dispatchId, "dispatchId must not be null");
        Objects.requireNonNull(courierId, "courierId must not be null");
        Objects.requireNonNull(etaMinutes, "etaMinutes must not be null");
        Objects.requireNonNull(assignedAt, "assignedAt must not be null");
        Objects.requireNonNull(lineageDigest, "lineageDigest must not be null");
    }
}
