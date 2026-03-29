package org.pipelineframework.tpfgo.common.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record OrderPending(
    UUID orderId,
    UUID requestId,
    UUID customerId,
    UUID restaurantId,
    BigDecimal totalAmount,
    String currency,
    Instant createdAt
) {
    public OrderPending {
        Objects.requireNonNull(orderId, "orderId must not be null");
        Objects.requireNonNull(requestId, "requestId must not be null");
        Objects.requireNonNull(customerId, "customerId must not be null");
        Objects.requireNonNull(restaurantId, "restaurantId must not be null");
        totalAmount = CommonDomainValidation.requireNonNegative(totalAmount, "totalAmount");
        currency = CommonDomainValidation.requireCurrencyCode(currency, "currency");
        Objects.requireNonNull(createdAt, "createdAt must not be null");
    }
}
