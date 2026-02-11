package org.pipelineframework.checkout.createorder.common.domain;

import java.util.UUID;

public record InitialOrder(
    UUID orderId,
    UUID customerId,
    int itemCount
) {
    public InitialOrder {
        if (itemCount < 0) {
            throw new IllegalArgumentException("itemCount must be >= 0");
        }
    }
}
