package org.pipelineframework.checkout.common.dto;

import java.time.Instant;
import java.util.UUID;

public record DeliveredOrderDto(
    UUID orderId,
    UUID customerId,
    Instant readyAt,
    UUID dispatchId,
    Instant dispatchedAt,
    Instant deliveredAt
) {
}
