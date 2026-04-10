package org.pipelineframework.tpfgo.common.domain;

import java.math.BigDecimal;
import java.util.Objects;
import java.util.UUID;

public record KitchenTask(
    UUID orderId,
    UUID customerId,
    UUID restaurantId,
    BigDecimal totalAmount,
    String currency,
    UUID kitchenTicketId,
    UUID taskId,
    String taskName,
    String taskStatus
) {
    public KitchenTask {
        Objects.requireNonNull(orderId, "orderId must not be null");
        Objects.requireNonNull(customerId, "customerId must not be null");
        Objects.requireNonNull(restaurantId, "restaurantId must not be null");
        totalAmount = CommonDomainValidation.requireNonNegative(totalAmount, "totalAmount");
        currency = CommonDomainValidation.requireCurrencyCode(currency, "currency");
        Objects.requireNonNull(kitchenTicketId, "kitchenTicketId must not be null");
        Objects.requireNonNull(taskId, "taskId must not be null");
        taskName = CommonDomainValidation.requireNonBlank(taskName, "taskName");
        taskStatus = CommonDomainValidation.requireNonBlank(taskStatus, "taskStatus");
    }
}
