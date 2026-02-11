package org.pipelineframework.checkout.createorder.common.domain;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record ReadyOrder(
    UUID orderId,
    UUID customerId,
    Instant readyAt
) {
    public ReadyOrder {
        Objects.requireNonNull(orderId, "orderId must not be null");
        Objects.requireNonNull(customerId, "customerId must not be null");
        Objects.requireNonNull(readyAt, "readyAt must not be null");
    }
}
