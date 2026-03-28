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
        Objects.requireNonNull(totalAmount, "totalAmount must not be null");
        Objects.requireNonNull(currency, "currency must not be null");
        Objects.requireNonNull(kitchenTicketId, "kitchenTicketId must not be null");
        Objects.requireNonNull(taskId, "taskId must not be null");
        Objects.requireNonNull(taskName, "taskName must not be null");
        Objects.requireNonNull(taskStatus, "taskStatus must not be null");
    }
}
