package org.pipelineframework.checkout.common.dto;

import java.util.UUID;

public record InitialOrderDto(
    UUID orderId,
    UUID customerId,
    int itemCount
) {
    public InitialOrderDto {
        if (itemCount <= 0) {
            throw new IllegalArgumentException("itemCount must be > 0");
        }
    }
}
