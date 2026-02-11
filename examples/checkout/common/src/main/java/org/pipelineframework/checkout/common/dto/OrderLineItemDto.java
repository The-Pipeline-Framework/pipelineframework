package org.pipelineframework.checkout.common.dto;

import java.util.Objects;
import java.util.UUID;

public record OrderLineItemDto(
    UUID requestId,
    UUID customerId,
    String sku,
    int quantity
) {
    public OrderLineItemDto {
        Objects.requireNonNull(requestId, "requestId must not be null");
        Objects.requireNonNull(customerId, "customerId must not be null");
        Objects.requireNonNull(sku, "sku must not be null");
        if (sku.isBlank()) {
            throw new IllegalArgumentException("sku must not be blank");
        }
        if (quantity <= 0) {
            throw new IllegalArgumentException("quantity must be > 0");
        }
    }
}
