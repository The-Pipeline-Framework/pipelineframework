package org.pipelineframework.checkout.createorder.common.domain;

import java.util.Objects;
import java.util.UUID;

public record InitialOrder(
    UUID orderId,
    UUID customerId,
    int itemCount
) {
    public InitialOrder {
        Objects.requireNonNull(orderId, "orderId must not be null");
        Objects.requireNonNull(customerId, "customerId must not be null");
        if (itemCount < 0) {
            throw new IllegalArgumentException("itemCount must be >= 0");
        }
    }
}
