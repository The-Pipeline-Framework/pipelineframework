package org.pipelineframework.checkout.deliverorder.common.domain;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record DeliveredOrder(
    UUID orderId,
    UUID customerId,
    Instant readyAt,
    UUID dispatchId,
    Instant dispatchedAt,
    Instant deliveredAt
) {
    public DeliveredOrder {
        Objects.requireNonNull(orderId, "orderId must not be null");
        Objects.requireNonNull(customerId, "customerId must not be null");
        Objects.requireNonNull(readyAt, "readyAt must not be null");
        Objects.requireNonNull(dispatchId, "dispatchId must not be null");
        Objects.requireNonNull(dispatchedAt, "dispatchedAt must not be null");
        Objects.requireNonNull(deliveredAt, "deliveredAt must not be null");
        if (readyAt.isAfter(dispatchedAt)) {
            throw new IllegalArgumentException("readyAt must be <= dispatchedAt");
        }
        if (dispatchedAt.isAfter(deliveredAt)) {
            throw new IllegalArgumentException("dispatchedAt must be <= deliveredAt");
        }
    }
}
