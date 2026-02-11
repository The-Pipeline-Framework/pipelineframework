package org.pipelineframework.checkout.common.dto;

import java.util.Objects;
import java.util.UUID;

public record OrderRequestDto(
    UUID requestId,
    UUID customerId,
    String items
) {
    public OrderRequestDto {
        Objects.requireNonNull(requestId, "requestId must not be null");
        Objects.requireNonNull(customerId, "customerId must not be null");
        Objects.requireNonNull(items, "items must not be null");
        if (items.isBlank()) {
            throw new IllegalArgumentException("items must not be blank");
        }
    }
}
