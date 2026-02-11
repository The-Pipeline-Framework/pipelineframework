package org.pipelineframework.checkout.common.dto;

import java.util.UUID;

public record InitialOrderDto(
    UUID orderId,
    UUID customerId,
    int itemCount
) {
}
