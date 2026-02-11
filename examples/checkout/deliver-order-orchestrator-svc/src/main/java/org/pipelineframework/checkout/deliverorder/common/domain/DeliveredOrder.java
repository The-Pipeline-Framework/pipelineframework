package org.pipelineframework.checkout.deliverorder.common.domain;

import java.time.Instant;
import java.util.UUID;

public record DeliveredOrder(
    UUID orderId,
    UUID customerId,
    Instant readyAt,
    UUID dispatchId,
    Instant dispatchedAt,
    Instant deliveredAt
) {
}
