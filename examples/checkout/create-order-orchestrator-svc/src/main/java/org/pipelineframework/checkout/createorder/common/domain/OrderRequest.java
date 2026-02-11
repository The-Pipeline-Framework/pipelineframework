package org.pipelineframework.checkout.createorder.common.domain;

import java.util.Objects;
import java.util.UUID;

public record OrderRequest(
    UUID requestId,
    UUID customerId,
    String items
) {
    public OrderRequest {
        Objects.requireNonNull(requestId, "requestId must not be null");
        Objects.requireNonNull(customerId, "customerId must not be null");
        Objects.requireNonNull(items, "items must not be null");
    }
}
