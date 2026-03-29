package org.pipelineframework.tpfgo.common.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record OrderReadyForDispatch(
    UUID orderId,
    UUID customerId,
    UUID restaurantId,
    BigDecimal totalAmount,
    String currency,
    Instant readyAt,
    UUID kitchenTicketId,
    String lineageDigest
) {
    public OrderReadyForDispatch {
        Objects.requireNonNull(orderId, "orderId must not be null");
        Objects.requireNonNull(customerId, "customerId must not be null");
        Objects.requireNonNull(restaurantId, "restaurantId must not be null");
        totalAmount = CommonDomainValidation.requireNonNegative(totalAmount, "totalAmount");
        currency = CommonDomainValidation.requireCurrencyCode(currency, "currency");
        Objects.requireNonNull(readyAt, "readyAt must not be null");
        Objects.requireNonNull(kitchenTicketId, "kitchenTicketId must not be null");
        lineageDigest = CommonDomainValidation.requireNonBlank(lineageDigest, "lineageDigest");
    }
}
