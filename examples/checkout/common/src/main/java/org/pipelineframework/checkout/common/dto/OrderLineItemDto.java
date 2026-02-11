package org.pipelineframework.checkout.common.dto;

import java.util.UUID;

public record OrderLineItemDto(
    UUID requestId,
    UUID customerId,
    String sku,
    int quantity
) {
    public OrderLineItemDto {
        if (quantity <= 0) {
            throw new IllegalArgumentException("quantity must be > 0");
        }
    }
}
