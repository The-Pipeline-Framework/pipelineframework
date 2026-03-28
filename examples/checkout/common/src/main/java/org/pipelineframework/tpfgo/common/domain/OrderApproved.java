package org.pipelineframework.tpfgo.common.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record OrderApproved(
    UUID orderId,
    UUID requestId,
    UUID customerId,
    UUID restaurantId,
    BigDecimal totalAmount,
    String currency,
    Instant approvedAt,
    String riskBand
) {
    public OrderApproved {
        Objects.requireNonNull(orderId, "orderId must not be null");
        Objects.requireNonNull(requestId, "requestId must not be null");
        Objects.requireNonNull(customerId, "customerId must not be null");
        Objects.requireNonNull(restaurantId, "restaurantId must not be null");
        Objects.requireNonNull(totalAmount, "totalAmount must not be null");
        Objects.requireNonNull(currency, "currency must not be null");
        Objects.requireNonNull(approvedAt, "approvedAt must not be null");
        Objects.requireNonNull(riskBand, "riskBand must not be null");
    }
}
