package org.pipelineframework.checkout.common.dto;

import java.util.UUID;

public record OrderRequestDto(
    UUID requestId,
    UUID customerId,
    String items
) {
}
