package org.pipelineframework.checkout.createorder.common.domain;

import java.util.UUID;

public record OrderRequest(
    UUID requestId,
    UUID customerId,
    String items
) {
}
