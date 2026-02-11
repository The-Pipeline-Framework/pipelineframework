package org.pipelineframework.checkout.createorder.common.domain;

import java.util.UUID;

public record OrderLineItem(
    UUID requestId,
    UUID customerId,
    String sku,
    int quantity
) {
    public OrderLineItem {
        if (quantity <= 0) {
            throw new IllegalArgumentException("quantity must be > 0");
        }
    }
}
