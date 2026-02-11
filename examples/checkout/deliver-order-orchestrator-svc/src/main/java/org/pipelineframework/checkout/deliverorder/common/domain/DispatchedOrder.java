package org.pipelineframework.checkout.deliverorder.common.domain;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record DispatchedOrder(
    UUID orderId,
    UUID customerId,
    Instant readyAt,
    UUID dispatchId,
    Instant dispatchedAt
) {
    public DispatchedOrder {
        Objects.requireNonNull(orderId, "orderId must not be null");
        Objects.requireNonNull(customerId, "customerId must not be null");
        Objects.requireNonNull(readyAt, "readyAt must not be null");
        Objects.requireNonNull(dispatchId, "dispatchId must not be null");
        Objects.requireNonNull(dispatchedAt, "dispatchedAt must not be null");
    }
}
