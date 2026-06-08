package org.pipelineframework.tpfgo.common.domain;

import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public record PlaceOrderRequest(
    UUID requestId,
    UUID customerId,
    UUID restaurantId,
    List<OrderItem> items,
    BigDecimal totalAmount,
    String currency
) {
    public PlaceOrderRequest {
        Objects.requireNonNull(requestId, "requestId must not be null");
        Objects.requireNonNull(customerId, "customerId must not be null");
        Objects.requireNonNull(restaurantId, "restaurantId must not be null");
        items = CommonDomainValidation.requireNonEmptyList(items, "items");
        totalAmount = CommonDomainValidation.requireNonNegative(totalAmount, "totalAmount");
        currency = CommonDomainValidation.requireCurrencyCode(currency, "currency");
    }
}
