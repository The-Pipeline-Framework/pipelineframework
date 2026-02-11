package org.pipelineframework.checkout.deliverorder.common.domain;

import java.time.Instant;
import java.util.UUID;

public record ReadyOrder(
    UUID orderId,
    UUID customerId,
    Instant readyAt
) {
}
