package org.pipelineframework.checkout.common.dto;

import java.time.Instant;
import java.util.UUID;

public record ReadyOrderDto(
    UUID orderId,
    UUID customerId,
    Instant readyAt
) {
}
