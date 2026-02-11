package org.pipelineframework.checkout.common.dto;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record ReadyOrderDto(
    UUID orderId,
    UUID customerId,
    Instant readyAt
) {
    public ReadyOrderDto {
        Objects.requireNonNull(orderId, "orderId must not be null");
        Objects.requireNonNull(customerId, "customerId must not be null");
        Objects.requireNonNull(readyAt, "readyAt must not be null");
    }
}
