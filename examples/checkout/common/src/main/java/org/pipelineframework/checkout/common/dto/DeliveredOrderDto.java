package org.pipelineframework.checkout.common.dto;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record DeliveredOrderDto(
    UUID orderId,
    UUID customerId,
    Instant readyAt,
    UUID dispatchId,
    Instant dispatchedAt,
    Instant deliveredAt
) {
    public DeliveredOrderDto {
        Objects.requireNonNull(orderId, "orderId must not be null");
        Objects.requireNonNull(customerId, "customerId must not be null");
        Objects.requireNonNull(dispatchId, "dispatchId must not be null");
        Objects.requireNonNull(dispatchedAt, "dispatchedAt must not be null");
        Objects.requireNonNull(deliveredAt, "deliveredAt must not be null");
    }
}
